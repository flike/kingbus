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
	"net"

	"github.com/flike/kingbus/storage/storagepb"

	"encoding/json"
	"sync"

	"fmt"

	"github.com/flike/kingbus/config"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/mysql"
	"github.com/flike/kingbus/storage"
	"github.com/flike/kingbus/utils"
	"github.com/satori/go.uuid"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/atomic"
)

//KingbusInfo get the kingbus server information
type KingbusInfo interface {
	AppliedIndex() uint64
	LastBinlogFile() string
	LastFilePosition() uint32
	ExecutedGtidSetStr() string
}

//BinlogServer is a binlog server,send binlog event to slave.
//The generic process:
//1.authentication
//SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'
//SET @master_binlog_checksum='NONE'
//SET @master_heartbeat_period=%d
//2.COM_REGISTER_SLAVE
//3.semi-sync:
//SHOW VARIABLES LIKE 'rpl_semi_sync_master_enabled';
//SET @rpl_semi_sync_slave = 1
//4.COM_BINLOG_DUMP_GTID
type BinlogServer struct {
	started *atomic.Bool
	cfg     *config.BinlogServerConfig

	listener net.Listener
	errch    chan error

	l      sync.RWMutex
	slaves map[string]*mysql.Slave //key is uuid

	broadcast   *utils.Broadcast
	kingbusInfo KingbusInfo
	store       storage.Storage
}

//NewBinlogServer create a binlog server
func NewBinlogServer(cfg *config.BinlogServerConfig, ki KingbusInfo, store storage.Storage, broadcast *utils.Broadcast) (*BinlogServer, error) {
	var err error
	s := new(BinlogServer)

	s.started = atomic.NewBool(false)
	s.cfg = cfg
	s.listener, err = net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		log.Log.Errorf("Listen error,err:%s,addr:%s", err, s.cfg.Addr)
		return nil, err
	}
	s.store = store
	s.broadcast = broadcast
	s.kingbusInfo = ki
	s.slaves = make(map[string]*mysql.Slave)
	s.errch = make(chan error, 1)

	return s, nil
}

//Start implements binlog server start
func (s *BinlogServer) Start() {
	s.started.Store(true)
	go func() {
		for s.started.Load() {
			select {
			case err := <-s.errch:
				log.Log.Errorf("binlog server Run error,err:%s", err)
				s.Stop()
				return
			default:
				conn, err := s.listener.Accept()
				if err != nil {
					log.Log.Infof("BinlogServer.Start:Accept error,err:%s", err)
					continue
				}
				go s.onConn(conn)
			}
		}
	}()
}

//Stop implements binlog server stop
func (s *BinlogServer) Stop() {
	log.Log.Infof("binlog master server stop")
	if s.started.Load() == false {
		return
	}
	s.started.Store(false)
	//s.cancel()
	err := s.listener.Close()
	if err != nil {
		log.Log.Errorf("BinlogServer close listener error,err:%s, addr:%s", err, s.listener.Addr().String())
	}

	s.l.Lock()
	for _, s := range s.slaves {
		s.Close()
	}
	s.l.Unlock()
}

func (s *BinlogServer) onConn(c net.Conn) {
	mysqlConn, err := mysql.NewConn(c, s, s.cfg.User, s.cfg.Password)
	if err != nil {
		log.Log.Errorf("onConn error,err:%s", err)
		return
	}
	mysqlConn.Run()
}

//RegisterSlave implements register slave into binlog server
func (s *BinlogServer) RegisterSlave(slave *mysql.Slave) error {
	s.l.Lock()
	defer s.l.Unlock()
	if len(slave.UUID) == 0 {
		return ErrUUIDIsNull
	}

	if _, ok := s.slaves[slave.UUID]; ok {
		log.Log.Warningf("register the same slave into binlog server again")
	}
	s.slaves[slave.UUID] = slave
	return nil
}

//GetSlaves get all slaves connected the binlog server
func (s *BinlogServer) GetSlaves() map[string]*mysql.Slave {
	s.l.RLock()
	defer s.l.RUnlock()

	slaves := make(map[string]*mysql.Slave)
	for uid, v := range s.slaves {
		slaves[uid] = v
	}
	return slaves
}

//DumpBinlogAt implements dump binlog event by slave executed gtid set
func (s *BinlogServer) DumpBinlogAt(ctx context.Context,
	startRaftIndex uint64, slaveGtids *gomysql.MysqlGTIDSet,
	eventC chan<- *storagepb.BinlogEvent, errorC chan<- error) error {
	var inExcludeGroup = false

	//new a binlog event reader from startRaftIndex, then send event to slave one by one
	reader, err := s.store.NewEntryReaderAt(startRaftIndex)
	if err != nil {
		log.Log.Errorf("NewEntryReaderAt error,err:%s,raftIndex:%d", err, startRaftIndex)
		return err
	}

	nextRaftIndex := reader.NextRaftIndex()
	log.Log.Infof("DumpBinlogAt:raftIndex:%d,slaveGtids:%s", nextRaftIndex, slaveGtids.String())
	go func() {
		for {
			//the next read raftIndex must be little than AppliedIndex
			if nextRaftIndex <= s.kingbusInfo.AppliedIndex() {
				raftEntry, err := reader.GetNext()
				if err != nil {
					log.Log.Errorf("reader.GetNext error,err:%s,nextRaftIndex:%d,AppliedIndex:%d",
						err, nextRaftIndex, s.kingbusInfo.AppliedIndex())
					select {
					case errorC <- err:
					default:
					}
					return //need quit
				}
				nextRaftIndex = reader.NextRaftIndex()

				//this entry is not binlog event
				if utils.IsBinlogEvent(raftEntry) == false {
					continue
				}
				event := utils.DecodeBinlogEvent(raftEntry)
				//filter the event in slave gtids,if the event has send to slave
				inExcludeGroup = s.skipEvent(event, slaveGtids, inExcludeGroup)
				if inExcludeGroup {
					continue
				}

				select {
				case eventC <- event:
				case <-ctx.Done():
					log.Log.Errorf("binlog server receive cancel, need quit,err:%s", ctx.Err())
					select {
					case errorC <- ctx.Err():
					default:
					}
					return //need quit
				}
			} else {
				select {
				case <-s.broadcast.Receive():
					break
				case <-ctx.Done():
					log.Log.Errorf("binlog server receive cancel, need quit,err:%s", ctx.Err())
					select {
					case errorC <- ctx.Err():
					default:
					}
					return //need quit
				}
			}
		}
	}()

	return nil
}

//skipEvent filter the event has been executed by slave
func (s *BinlogServer) skipEvent(event *storagepb.BinlogEvent, slaveGtids *gomysql.MysqlGTIDSet, inExcludeGroup bool) bool {
	switch replication.EventType(event.Type) {
	case replication.GTID_EVENT:
		//remove header
		eventBody := event.Data[replication.EventHeaderSize:]
		//remove crc32
		eventBody = eventBody[:len(eventBody)-replication.BinlogChecksumLength]

		gtidEvent := &replication.GTIDEvent{}
		if err := gtidEvent.Decode(eventBody); err != nil {
			log.Log.Errorf("Decode gtid event error,err:%s", err)
			return true
		}
		u, err := uuid.FromBytes(gtidEvent.SID)
		if err != nil {
			log.Log.Errorf("FromBytes error,err:%s,sid:%v", err, gtidEvent.SID)
			return true
		}
		gtidStr := fmt.Sprintf("%s:%d", u.String(), gtidEvent.GNO)
		currentGtidset, err := gomysql.ParseMysqlGTIDSet(gtidStr)
		if err != nil {
			log.Log.Errorf("ParseMysqlGTIDSet error,err:%s,gtid:%s", err, gtidStr)
			return true
		}
		return slaveGtids.Contain(currentGtidset)
	case replication.ROTATE_EVENT:
		return false
	}
	return inExcludeGroup
}

//LastBinlogFile return the last binlog file
func (s *BinlogServer) LastBinlogFile() string {
	return s.kingbusInfo.LastBinlogFile()
}

//LastFilePosition return the last binlog file position
func (s *BinlogServer) LastFilePosition() uint32 {
	return s.kingbusInfo.LastFilePosition()
}

//GetMySQLDumpAt return raft index, dump binlog event from this position
//TODO if gtids is empty set, and previous_gtids also is empty, ok!
func (s *BinlogServer) GetMySQLDumpAt(slaveExecutedGtids *gomysql.MysqlGTIDSet) (uint64, error) {
	return s.store.GetPreviousGtidSet(slaveExecutedGtids)
}

//CheckGtidSet the illegal of the slave executed gtid
func (s *BinlogServer) CheckGtidSet(flavor string, slaveExecutedGtidSet gomysql.GTIDSet) error {
	//get the last value of ExecutedGtidSetStr
	executedGtidSet, err := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, s.kingbusInfo.ExecutedGtidSetStr())
	if err != nil {
		log.Log.Errorf("ParseGTIDSet error,err:%s,value:%s", err, s.kingbusInfo.ExecutedGtidSetStr())
		return err
	}
	log.Log.Debugf("CheckGtidSet:slaveExecutedGtidSet is:%s,master.executedGtidSet is:%s",
		slaveExecutedGtidSet.String(), executedGtidSet.String())

	masterExecutedGtids := executedGtidSet.(*gomysql.MysqlGTIDSet)
	slaveExecutedGtids := slaveExecutedGtidSet.(*gomysql.MysqlGTIDSet)

	//slave executed gtid slaveSet must be contained in master executed gtid slaveSet,when its uuid is equal.
	for uid, slaveSet := range slaveExecutedGtids.Sets {
		if MasterSet, ok := masterExecutedGtids.Sets[uid]; ok {
			if MasterSet.Intervals.Contain(slaveSet.Intervals) == false {
				return ErrNotContain
			}
		}
	}

	//master purged gtid slaveSet must be contained in slave executed gtid slaveSet
	purgedGtidSet, err := s.store.GetGtidSet(gomysql.MySQLFlavor, storage.GtidPurgedKey)
	contain := slaveExecutedGtidSet.Contain(purgedGtidSet)
	if !contain {
		return ErrNotContain
	}
	return nil
}

//GetFde get the FORMAT_DESCRIPTION_EVENT by previous gtids log event raft index
func (s *BinlogServer) GetFde(preGtidEventIndex uint64) ([]byte, error) {
	if preGtidEventIndex == 0 {
		return nil, ErrArgs
	}
	fde, err := s.store.GetFde(preGtidEventIndex)
	if err != nil {
		log.Log.Errorf("GetFde error,err:%s,preGtidEventIndex:%d", err, preGtidEventIndex)
	}
	return fde, nil
}

//GetNextBinlogFile get next binlog file by raft index
func (s *BinlogServer) GetNextBinlogFile(startRaftIndex uint64) (string, error) {
	return s.store.GetNextBinlogFile(startRaftIndex)
}

//GetGtidSet get gtid set
func (s *BinlogServer) GetGtidSet(flavor string, key string) (gomysql.GTIDSet, error) {
	return s.store.GetGtidSet(flavor, key)
}

//GetMasterInfo get the master information connected by syncer
func (s *BinlogServer) GetMasterInfo() (*mysql.MasterInfo, error) {
	var masterInfo mysql.MasterInfo
	key := utils.StringToBytes(storage.MasterInfoKey)
	buf, err := s.store.Get(key)
	if err != nil {
		log.Log.Errorf("store.Get error,err:%s,key:%s", err, storage.MasterInfoKey)
		return nil, err
	}

	if buf == nil {
		log.Log.Warnf("store.Get MasterInfoKey not exist,return a empty masterInfo")
		return &masterInfo, nil
	}

	err = json.Unmarshal(buf, &masterInfo)
	if err != nil {
		log.Log.Errorf("Unmarshal error,err:%s,buf:%v", err, buf)
		return nil, err
	}

	return &masterInfo, nil
}

//UnregisterSlave unregister slave by uuid
func (s *BinlogServer) UnregisterSlave(uuid string) {
	s.l.Lock()
	defer s.l.Unlock()

	if _, ok := s.slaves[uuid]; ok {
		s.slaves[uuid].State = mysql.UNREGISTERED
		s.slaves[uuid].Close()
		delete(s.slaves, uuid)
	}
}
