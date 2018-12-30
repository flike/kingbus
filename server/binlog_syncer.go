// Copyright 2018 The kingbus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"sync"

	"go.uber.org/atomic"

	"github.com/flike/kingbus/storage/storagepb"

	"fmt"

	"github.com/flike/kingbus/config"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/mysql"
	"github.com/flike/kingbus/storage"
	"github.com/siddontang/go-mysql/client"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

//Syncer is a mock mysql slave, and receive binlog from master
//and propose event into raft cluster
type Syncer struct {
	started *atomic.Bool
	cfg     *config.SyncerConfig

	connLock sync.Mutex
	conn     *client.Conn

	io           *replication.BinlogSyncer
	ctx          context.Context
	cancel       context.CancelFunc
	binlogEventC chan *storagepb.BinlogEvent //binlog event propose into this channel
	dataC        chan []byte                 //data args propose into this channel

	store storage.Storage
}

//NewSyncer create syncer in lead node, and init its gtid set
func NewSyncer(cfg *config.SyncerConfig, store storage.Storage) (*Syncer, error) {
	s := new(Syncer)
	s.started = atomic.NewBool(false)
	s.cfg = cfg
	s.io = replication.NewBinlogSyncer(cfg.BinlogSyncerConfig)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.binlogEventC = make(chan *storagepb.BinlogEvent, 1000)
	s.dataC = make(chan []byte, 10)
	s.store = store

	return s, nil
}

//getMasterGtidPurged get maseter gtid_purged
func (s *Syncer) getMasterGtidPurged() (string, error) {
	res, err := s.Execute("SELECT @@gtid_purged")
	if err != nil {
		log.Log.Errorf("Execute SQL error,err: %s, sql: SELECT @@gtid_purged", err)
		return "", err

	}
	masterGtidPurgedStr, err := res.GetString(0, 0)
	if err != nil {
		log.Log.Errorf("GetString error,err: %s, sql: SELECT @@gtid_purged", err)
		return "", err
	}
	return masterGtidPurgedStr, nil
}

func (s *Syncer) proposeMasterGtidPurged(gtidPurged string) error {
	var masterGtidPurged config.MasterGtidPurged

	masterGtidPurged.GtidPurged = gtidPurged
	data, err := masterGtidPurged.EncodeWithType()
	if err != nil {
		return err
	}
	select {
	case <-s.ctx.Done():
		log.Log.Infof("syncer quit")
	case s.dataC <- data:
		log.Log.Infof("propose masterGtidPurged to cluster,masterGtidPurged:%v", masterGtidPurged)
	}
	return nil
}

//check whether master meets the relevant conditions
func (s *Syncer) preCheck() error {
	//check the gtid mode in master
	res, err := s.Execute("SELECT @@gtid_mode")
	if err != nil {
		log.Log.Errorf("Execute SQL error,err: %s", err)
		return err
	} else if mode, _ := res.GetString(0, 0); mode != "ON" {
		log.Log.Errorf("git_mode want ON, but is %s now", mode)
		return ErrUnsupport
	}

	//check the row mode in master
	res, err = s.Execute(`SELECT @@binlog_format;`)
	if err != nil {
		log.Log.Errorf("Execute SQL error,err: %s", err)
		return err
	} else if f, _ := res.GetString(0, 0); f != "ROW" {
		log.Log.Errorf("binlog must ROW format, but %s now", f)
		return ErrUnsupport
	}

	return nil
}

//Start syncer
func (s *Syncer) Start(gset gomysql.GTIDSet) error {
	err := s.preCheck()
	if err != nil {
		return err
	}

	//get the master info, using in binlog server,and propose to raft cluster
	err = s.GetMasterInfo()
	if err != nil {
		log.Log.Errorf("GetMasterInfo error,err:%s", err)
		return err
	}

	//start syncer
	binlogStreamer, err := s.io.StartSyncGTID(gset)
	if err != nil {
		log.Log.Errorf("Syncer.StartSyncGTID error,err:%s,executedGtidSet:%s", err, gset.String())
		return err
	}
	log.Log.Infof("syncer:start syncer,syncer.executedGtidSet:%s", gset.String())

	s.started.Store(true)
	go func() {
		defer func() {
			s.Stop()
		}()

		for s.started.Load() {
			event, err := binlogStreamer.GetEvent(s.ctx)
			if err != nil {
				log.Log.Errorf("Syncer.Start:GetEvent error,err:%s", err)
				return
			}
			syncerEps.Mark(1)
			proposeChannelSize.Update(int64(len(s.BinlogEventC())))
			//ignore the heartbeat event from master
			if event.Header.EventType == replication.HEARTBEAT_EVENT {
				continue
			}
			err = s.proposeBinlogEvent(event)
			if err != nil {
				log.Log.Fatalf("Syncer.Start:proposeBinlogEvent error,err:%s", err)
			}
		}
	}()

	return nil
}

func (s *Syncer) proposeBinlogEvent(event *replication.BinlogEvent) error {
	//packet maxsize is 0xffffff
	if event.Header.EventSize+mysql.OKHeaderByte < uint32(mysql.MaxPayloadLen) {
		newEvent := &storagepb.BinlogEvent{
			Type:          uint32(event.Header.EventType),
			DividedCount:  0,
			DividedSeqNum: 0,
			Data:          event.RawData,
		}
		if s.cfg.Flavor == gomysql.MySQLFlavor {
			newEvent.Flavor = storagepb.FlavorType_MySQL
		} else {
			newEvent.Flavor = storagepb.FlavorType_Maria
		}
		select {
		case <-s.ctx.Done():
			log.Log.Infof("syncer quit")
		case s.binlogEventC <- newEvent:
		}
	} else {
		divideEvents, err := s.divideBinlogEvent(event)
		if err != nil {
			log.Log.Errorf("divideBinlogEvent error,err :%s", err)
			return err
		}
		for _, dividedEvent := range divideEvents {
			select {
			case <-s.ctx.Done():
				log.Log.Infof("syncer quit")
			case s.binlogEventC <- dividedEvent:
			}
		}
	}
	return nil
}

//BinlogEventC get binlogEvent channel
func (s *Syncer) BinlogEventC() chan *storagepb.BinlogEvent {
	return s.binlogEventC
}

//DataC get data channel
func (s *Syncer) DataC() chan []byte {
	return s.dataC
}

//GetMasterInfo get master info
func (s *Syncer) GetMasterInfo() error {
	//get server id
	res, err := s.Execute(`SELECT @@GLOBAL.SERVER_ID;`)
	if err != nil {
		log.Log.Errorf("GetMasterServerId:Execute error,err:%s,sql:%s", err, "SELECT @@server_id")
		return err
	}
	serverID, err := res.GetInt(0, 0)
	if err != nil {
		log.Log.Errorf("GetMasterServerId:GetInt error,err:%s", err)
		return err
	}

	//get server uuid
	res, err = s.Execute(`SELECT @@GLOBAL.SERVER_UUID;`)
	if err != nil {
		log.Log.Errorf("GetMasterServerId:Execute error,err:%s,sql:%s", err, "SELECT @@server_id")
		return err
	}
	serverUUID, err := res.GetString(0, 0)
	if err != nil {
		log.Log.Errorf("GetMasterServerId:GetInt error,err:%s", err)
		return err
	}

	//get version
	res, err = s.Execute(`SELECT VERSION();`)
	if err != nil {
		log.Log.Errorf("GetMasterVersion:Execute error,err:%s,sql:%s", err, "SELECT VERSION()")
		return err
	}
	version, err := res.GetString(0, 0)
	if err != nil {
		log.Log.Errorf("GetMasterVersion:GetInt error,err:%s", err)
		return err
	}

	masterInfo := &mysql.MasterInfo{
		ServerID:   int32(serverID),
		ServerUUID: serverUUID,
		Version:    version,
	}

	//sync the sycer gtid to other nodes
	data, err := masterInfo.EncodeMasterInfo()
	if err != nil {
		return err
	}
	select {
	case <-s.ctx.Done():
		log.Log.Infof("syncer quit")
	case s.dataC <- data:
		log.Log.Infof("GetMasterInfo:propose master info to cluster,masterInfo:%v", masterInfo)
	}

	return nil
}

// Execute a SQL in Master
func (s *Syncer) Execute(cmd string, args ...interface{}) (rr *gomysql.Result, err error) {
	s.connLock.Lock()
	defer s.connLock.Unlock()

	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	retryNum := 3
	for i := 0; i < retryNum; i++ {
		if s.conn == nil {
			s.conn, err = client.Connect(addr, s.cfg.User, s.cfg.Password, "")
			if err != nil {
				log.Log.Errorf("Syncer Connect %s error,err:%s,user:%s,pwd:%s",
					s.cfg.Host, err, s.cfg.User, s.cfg.Password)
				return nil, err
			}
		}

		rr, err = s.conn.Execute(cmd, args...)
		if err != nil && !gomysql.ErrorEqual(err, mysql.ErrBadConn) {
			return
		} else if gomysql.ErrorEqual(err, mysql.ErrBadConn) {
			s.conn.Close()
			s.conn = nil
			continue
		} else {
			return
		}
	}
	return
}

//Stop syncer
func (s *Syncer) Stop() {
	log.Log.Infof("stop syncer")
	if s.started.Load() == false {
		return
	}
	s.started.Store(false)
	s.cancel()

	s.connLock.Lock()
	s.conn.Close()
	s.conn = nil
	s.connLock.Unlock()

	s.io.Close()
}

//todo refactor
//todo srcEventLength + mysql.OKHeaderByte = 0xffffff时
//Finally, you need to add an empty packet
//packet 1
//4byte packet header
//1byte valueis [00], the binlog event flag
//16MB-2Btye is the first packet
//
//packet 2
//4Byte packet header
//20MB-16MB+2Byte is the second packet
//
//It is important to note that packet after does not have [00] characteristic bit.
//And packet size calculation range is to remove all the bytes of the first 4 bytes
func (s *Syncer) divideBinlogEvent(srcEvent *replication.BinlogEvent) ([]*storagepb.BinlogEvent, error) {
	var (
		pos              uint64
		i                uint32
		flavor           storagepb.FlavorType
		maxEventDataSize uint64
	)
	newEvents := make([]*storagepb.BinlogEvent, 0, 8)

	srcEventLength := len(srcEvent.RawData)
	//size + ok_header
	dividedCount := uint32((srcEventLength + mysql.OKHeaderByte) / mysql.MaxPayloadLen)
	if srcEventLength%mysql.MaxPayloadLen != 0 {
		dividedCount++
	}

	if s.cfg.Flavor == gomysql.MySQLFlavor {
		flavor = storagepb.FlavorType_MySQL
	} else {
		flavor = storagepb.FlavorType_MySQL
	}

	pos = 0
	for i = 0; i < dividedCount; i++ {
		if i == 0 {
			//0xffffff-1
			//ok_header
			maxEventDataSize = uint64(mysql.MaxPayloadLen) - 1
		} else {
			//0xffffff
			maxEventDataSize = uint64(mysql.MaxPayloadLen)
		}

		newEvent := &storagepb.BinlogEvent{
			Type:          uint32(srcEvent.Header.EventType),
			Flavor:        flavor,
			DividedCount:  dividedCount,
			DividedSeqNum: i,
		}
		if pos+maxEventDataSize < uint64(srcEventLength) {
			newEvent.Data = srcEvent.RawData[pos : pos+maxEventDataSize]
		} else {
			newEvent.Data = srcEvent.RawData[pos:]
		}
		pos += maxEventDataSize
		//newEvent.DataLen = uint32(len(newEvent.Data))
		newEvents = append(newEvents, newEvent)
	}
	return newEvents, nil
}
