package server

import (
	"fmt"
	"time"

	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/mysql"
	"github.com/flike/kingbus/storage"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"go.uber.org/atomic"
)

const (
	persistentCount        = 10000
	persistentTimeInterval = 4 * time.Second
)

//BinlogProgress is the progress of receiving binlog
type BinlogProgress struct {
	currentGtid  *atomic.String
	lastSaveGtid string
	//for heartbeat event
	lastBinlogFile     *atomic.String
	lastFilePosition   *atomic.Uint32
	executedGtidSetStr *atomic.String

	trxBoundaryParser *mysql.TransactionBoundaryParser

	persistentTime         time.Time
	persistentAppliedIndex uint64
	executedGtidSet        gomysql.GTIDSet
	store                  storage.Storage
}

func newBinlogProgress(store storage.Storage) (*BinlogProgress, error) {
	var err error
	p := new(BinlogProgress)

	p.trxBoundaryParser = new(mysql.TransactionBoundaryParser)
	p.trxBoundaryParser.Reset()

	p.currentGtid = atomic.NewString("")
	p.lastBinlogFile = atomic.NewString("")
	p.lastFilePosition = atomic.NewUint32(0)

	p.persistentAppliedIndex = 0
	p.persistentTime = time.Unix(0, 0)

	//get executed gtid_set
	//This value may be old, but resetBinlogProgress will update it to the latest
	p.executedGtidSet, err = store.GetGtidSet(gomysql.MySQLFlavor, storage.ExecutedGtidSetKey)
	if err != nil {
		log.Log.Errorf("newBinlogProgress:get executedGtidSet error,err:%s", err)
		return nil, err
	}
	p.executedGtidSetStr = atomic.NewString(p.executedGtidSet.String())
	p.store = store

	return p, nil
}

func (s *BinlogProgress) reset(gtids string) error {
	var err error
	s.trxBoundaryParser = new(mysql.TransactionBoundaryParser)
	s.trxBoundaryParser.Reset()

	s.currentGtid = atomic.NewString("")
	s.lastBinlogFile = atomic.NewString("")
	s.lastFilePosition = atomic.NewUint32(0)

	s.persistentAppliedIndex = 0
	s.persistentTime = time.Unix(0, 0)

	if len(gtids) != 0 {
		err = s.executedGtidSet.Update(gtids)
		if err != nil {
			return err
		}
		err = s.store.SetGtidSet(gomysql.MySQLFlavor, storage.ExecutedGtidSetKey, s.executedGtidSet)
		if err != nil {
			log.Log.Errorf("SetGtidSet error,err: %s, gtids: %s", s.executedGtidSet.String())
			return err
		}
	}

	s.executedGtidSetStr = atomic.NewString(s.executedGtidSet.String())
	return nil
}

//updateProcess update and save executedGtid set
func (s *BinlogProgress) updateProcess(raftIndex uint64, eventRawData []byte) error {
	var err error

	//parse event header
	h := new(replication.EventHeader)
	err = h.Decode(eventRawData)
	if err != nil {
		log.Log.Errorf("Decode error,err:%s,buf:%v", err, eventRawData)
		return err
	}
	//set the heartbeat info
	s.lastFilePosition.Store(h.LogPos)

	//remove header
	eventRawData = eventRawData[replication.EventHeaderSize:]
	eventLen := int(h.EventSize) - replication.EventHeaderSize
	if len(eventRawData) != eventLen {
		return fmt.Errorf("invalid data size %d in event %s, less event length %d",
			len(eventRawData), h.EventType, eventLen)
	}
	//remove crc32
	eventRawData = eventRawData[:len(eventRawData)-replication.BinlogChecksumLength]

	//the eventRawData maybe the first divided packet, but must not be query event
	//so don't worry
	eventBoundaryType, err := s.trxBoundaryParser.GetEventBoundaryType(h, eventRawData)
	if err != nil {
		log.Log.Errorf("GetEventBoundaryType error,err:%s,header:%v",
			err, *h)
		return err
	}
	//ignore updateState error, maybe a partial trx
	err = s.trxBoundaryParser.UpdateState(eventBoundaryType)
	if err != nil {
		log.Log.Warnf("trxBoundaryParser UpdateState error,err:%s,header:%v", err, *h)
		s.trxBoundaryParser.Reset()
		s.currentGtid.Store("")
		return nil
	}

	currentGtidStr := s.currentGtid.Load()
	if s.trxBoundaryParser.IsNotInsideTransaction() &&
		len(currentGtidStr) != 0 && s.lastSaveGtid != currentGtidStr {

		log.Log.Debugf("current gtid is :%s,add into executedGtidSet:%s",
			currentGtidStr, s.executedGtidSet.String())
		//update executedGtidSet
		err = s.executedGtidSet.Update(currentGtidStr)
		if err != nil {
			return err
		}
		s.lastSaveGtid = currentGtidStr
		s.executedGtidSetStr.Store(s.executedGtidSet.String())

		//save the raftIndex and executedGtidSet at the same time
		if raftIndex-s.persistentAppliedIndex > persistentCount ||
			time.Now().Sub(s.persistentTime) > persistentTimeInterval {
			err = s.store.SetBinlogProgress(raftIndex, s.executedGtidSet)
			if err != nil {
				log.Log.Errorf("SetGtidSet error,err:%s,key:%s,value:%s",
					err, storage.ExecutedGtidSetKey, s.executedGtidSet.String())
				return err
			}

			s.persistentAppliedIndex = raftIndex
			s.persistentTime = time.Now()
		}
	}
	return nil
}

//LastBinlogFile get the last file of binlog event
func (s *BinlogProgress) LastBinlogFile() string {
	return s.lastBinlogFile.Load()
}

//LastFilePosition get the last file position of binlog event
func (s *BinlogProgress) LastFilePosition() uint32 {
	return s.lastFilePosition.Load()
}

//ExecutedGtidSetStr get the executed gtid set
func (s *BinlogProgress) ExecutedGtidSetStr() string {
	return s.executedGtidSetStr.Load()
}

//CurrentGtidStr get the current gtid
func (s *BinlogProgress) CurrentGtidStr() string {
	return s.currentGtid.Load()
}

//ExecutedGtidSetClone geth the executed gtid set clone, deep copy
func (s *BinlogProgress) ExecutedGtidSetClone() gomysql.GTIDSet {
	return s.executedGtidSet.Clone()
}
