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

package storage

import (
	"encoding/binary"
	"path"

	"encoding/json"

	"bytes"

	"strings"

	"strconv"

	"fmt"

	bolt "github.com/coreos/bbolt"
	pb "github.com/coreos/etcd/raft/raftpb"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/utils"
	gomysql "github.com/siddontang/go-mysql/mysql"
)

const (
	//MetadataFile is the file name of storage metadata
	MetadataFile = "kingbus_meta.db"
	//BucketName is the bucket name using in bolt db
	BucketName = "kingbus_meta_bucket"
)

//MetaStore is a metadata store
type MetaStore struct {
	DB         *bolt.DB
	Dir        string
	BucketName []byte
}

//NewMetaStore create a meta store
func NewMetaStore(dir string) (*MetaStore, error) {
	if len(dir) == 0 {
		return nil, ErrArgsIllegal
	}

	dbFilePath := path.Join(dir, MetadataFile)
	db, err := bolt.Open(dbFilePath, 0600, nil)
	if err != nil {
		log.Log.Errorf("[stable_bolt.go-NewBoltDBStore]:bolt Open error,err:%s,dbpath:%s", err.Error(), dbFilePath)
		return nil, err
	}

	s := &MetaStore{
		Dir:        dir,
		DB:         db,
		BucketName: []byte(BucketName),
	}
	err = s.init()
	if err != nil {
		s.Close2()
		return nil, err
	}

	return s, nil
}

//create bucket if not exist
func (s *MetaStore) init() error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(s.BucketName)
		if err != nil {
			log.Log.Errorf("CreateBucketIfNotExists error:error=%s,BucketName=%s",
				err.Error(), string(s.BucketName))
			return err
		}
		log.Log.Debugf("create bucket:%s success", BucketName)
		return nil
	})
}

//Get value by key
func (s *MetaStore) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsNil
	}
	var data []byte
	err := s.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.BucketName)
		val := bucket.Get(key)
		if val != nil {
			data = make([]byte, len(val))
			copy(data, val)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return data, nil
}

//Set value by key
func (s *MetaStore) Set(key []byte, value []byte) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.BucketName)
		err := b.Put(key, value)
		if err != nil {
			log.Log.Errorf("MetaStore Set error,err:%s,key:%s", err.Error(), string(key))
			return err
		}
		return nil
	})
}

//Delete key
func (s *MetaStore) Delete(key []byte) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.BucketName)
		err := b.Delete(key)
		if err != nil {
			log.Log.Errorf("MetaStore Delete error,err:%s,key:%s", err.Error(), string(key))
			return err
		}
		return nil
	})
}

//InitialState init raft state
func (s *MetaStore) InitialState() (pb.HardState, pb.ConfState, error) {
	var hs pb.HardState
	var cs pb.ConfState

	hsValue, err := s.Get(utils.StringToBytes(HardStateKey))
	if err != nil {
		return pb.HardState{}, pb.ConfState{}, err
	}
	if hsValue != nil {
		err = json.Unmarshal(hsValue, &hs)
		if err != nil {
			log.Log.Errorf("MetaStore:initialState json Unmarshal error,err:%s,hsKey:%s,hsValue:%s",
				err.Error(), HardStateKey, utils.BytesToString(hsValue))
			return pb.HardState{}, pb.ConfState{}, err
		}
	}

	csValue, err := s.Get(utils.StringToBytes(ConfStateKey))
	if err != nil {
		return pb.HardState{}, pb.ConfState{}, err
	}
	if csValue != nil {
		err = json.Unmarshal(csValue, &cs)
		if err != nil {
			log.Log.Errorf("MetaStore:initialState json Unmarshal error,err:%s,csKey:%s,csValue:%s",
				err.Error(), ConfStateKey, utils.BytesToString(csValue))
			return pb.HardState{}, pb.ConfState{}, err
		}
	}

	return hs, cs, nil
}

//SaveHardState save hard state in raft
func (s *MetaStore) SaveHardState(st pb.HardState) error {
	value, err := json.Marshal(st)
	if err != nil {
		log.Log.Errorf("MetaStore:saveHardState error,err:%s,HardState:%v", err.Error(), st)
		return err
	}

	err = s.Set(utils.StringToBytes(HardStateKey), value)
	if err != nil {
		log.Log.Errorf("MetaStore:saveHardState error,err:%s,key:%s,value:%s",
			err.Error(), HardStateKey, utils.BytesToString(value))
		return err
	}
	return nil
}

//SetGtidSet set mysql gtid
func (s *MetaStore) SetGtidSet(flavor string, key string, gtidSet gomysql.GTIDSet) error {
	flavorKey := path.Join(flavor, key)
	value := gtidSet.Encode()
	err := s.Set(utils.StringToBytes(flavorKey), value)
	if err != nil {
		log.Log.Errorf("MetaStore:SetGtidSet error,err:%s,key:%s,value:%s",
			err, key, gtidSet.String())
		return err
	}
	return nil
}

//SetBinlogProgress save appliedIndex and executedGtidSet at the same time
func (s *MetaStore) SetBinlogProgress(appliedIndex uint64, executedGtidSet gomysql.GTIDSet) error {
	gtidsKey := utils.StringToBytes(path.Join(gomysql.MySQLFlavor, ExecutedGtidSetKey))
	gtidsValue := executedGtidSet.Encode()

	appliedIndexKey := utils.StringToBytes(AppliedIndexKey)
	appliedIndexValue := make([]byte, 8)
	binary.BigEndian.PutUint64(appliedIndexValue, appliedIndex)

	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.BucketName)
		err := b.Put(gtidsKey, gtidsValue)
		if err != nil {
			log.Log.Errorf("MetaStore SetBinlogProgress error,err:%s,key:%s", err.Error(), string(gtidsKey))
			return err
		}
		err = b.Put(appliedIndexKey, appliedIndexValue)
		if err != nil {
			log.Log.Errorf("MetaStore SetBinlogProgress error,err:%s,key:%s", err.Error(), string(appliedIndexKey))
			return err
		}
		return nil
	})
}

//GetGtidSet get gtid set
func (s *MetaStore) GetGtidSet(flavor string, key string) (gomysql.GTIDSet, error) {
	flavorKey := path.Join(flavor, key)
	value, err := s.Get(utils.StringToBytes(flavorKey))
	if err != nil {
		return nil, err
	}

	if value == nil {
		gitSet := new(gomysql.MysqlGTIDSet)
		gitSet.Sets = make(map[string]*gomysql.UUIDSet)
		return gitSet, nil
	}

	gtids, err := gomysql.DecodeMysqlGTIDSet(value)
	if err != nil {
		return nil, err
	}
	return gtids, nil
}

//SetPreviousGtidSet set previous gtid set
func (s *MetaStore) SetPreviousGtidSet(raftIndex uint64, previousGtidSet *gomysql.MysqlGTIDSet) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		var exist bool
		keyPrefix := utils.StringToBytes(PgePrefix)
		bucket := tx.Bucket(s.BucketName)
		cursor := bucket.Cursor()

		//Check if the previousGtidSet exists and if it does, do not set the value
		for k, v := cursor.Seek(keyPrefix); k != nil && bytes.HasPrefix(k, keyPrefix); k, v = cursor.Next() {
			gtids, err := gomysql.DecodeMysqlGTIDSet(v)
			if err != nil {
				return err
			}
			log.Log.Debugf("GetPreviousGtidSet:key is %s,value:%s,previous:%s",
				string(k), gtids.String(), previousGtidSet.String())
			if gtids.Equal(previousGtidSet) {
				exist = true
				break
			}
		}
		if !exist {
			raftIndexStr := fmt.Sprintf("%020d", raftIndex)
			str := path.Join(PgePrefix, raftIndexStr)
			key := utils.StringToBytes(str)
			value := previousGtidSet.Encode()
			err := bucket.Put(key, value)
			if err != nil {
				log.Log.Errorf("SetPreviousGtidSet:Put error,err:%s,key:%s", err.Error(), string(key))
				return err
			}
		}
		return nil
	})
}

func (s *MetaStore) deletePreviousGtidSet(lastRaftIndex uint64) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		keyPrefix := utils.StringToBytes(PgePrefix)
		bucket := tx.Bucket(s.BucketName)
		cursor := bucket.Cursor()

		for k, _ := cursor.Seek(keyPrefix); k != nil && bytes.HasPrefix(k, keyPrefix); k, _ = cursor.Next() {
			keyStr := utils.BytesToString(k)
			strArray := strings.Split(keyStr, "/")
			if len(strArray) != 2 {
				log.Log.Errorf("split string error,keyStr:%s", keyStr)
				return ErrKeyNotFound
			}
			raftIndex, err := strconv.ParseUint(strArray[1], 10, 64)
			if err != nil {
				return ErrKeyNotFound
			}
			//delete PreviousGtidSet before lastRaftIndex
			if raftIndex < lastRaftIndex {
				err = cursor.Delete()
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

//GetPreviousGtidSet Iterate over all Previous Gtid keys in order, and read only
//the Previous_gtids_log_event, to find the last one, that is the
//subset of slaveExecutedGtids. Since every binary log file begins with
//a Previous_gtids_log_event, that contains all GTIDs in all
//previous binary logs.
//We also ask for the first GTID in the binary log to know if we
//should send the FD event with the "created" field cleared or not.
func (s *MetaStore) GetPreviousGtidSet(slaveExecutedGtids *gomysql.MysqlGTIDSet) (uint64, error) {
	var raftIndex uint64
	var lastGtids *gomysql.MysqlGTIDSet
	var lastKey []byte

	keyPrefix := utils.StringToBytes(PgePrefix)
	err := s.DB.View(func(tx *bolt.Tx) error {
		var txErr error
		bucket := tx.Bucket(s.BucketName)
		cursor := bucket.Cursor()
		for k, v := cursor.Seek(keyPrefix); k != nil && bytes.HasPrefix(k, keyPrefix); k, v = cursor.Next() {
			gtids, err := gomysql.DecodeMysqlGTIDSet(v)
			if err != nil {
				return err
			}
			log.Log.Debugf("GetPreviousGtidSet:key is %s,value:%s", string(k), gtids.String())
			//return the first previous  gtid set event
			if len(slaveExecutedGtids.Sets) == 0 {
				lastGtids = gtids
				lastKey = k
				break
			}
			//set lastGtids when slaveExecutedGtids contain gtids
			if slaveExecutedGtids.Contain(gtids) {
				lastGtids = gtids
				lastKey = k
			}
		}

		if lastGtids == nil {
			log.Log.Errorf("GetPreviousGtidSet:lastGtids not be found,slaveExecutedGtids:%s",
				slaveExecutedGtids.String())
			return ErrNotContain
		}

		keyStr := utils.BytesToString(lastKey)
		strArray := strings.Split(keyStr, "/")
		if len(strArray) != 2 {
			log.Log.Errorf("split string error,keyStr:%s", keyStr)
			return ErrKeyNotFound
		}
		raftIndex, txErr = strconv.ParseUint(strArray[1], 10, 64)
		if txErr != nil {
			return ErrKeyNotFound
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	log.Log.Debugf("GetPreviousGtidSet:raftIndex:%d,previousGtids:%s,slaveExecutedGtids:%s",
		raftIndex, lastGtids.String(), slaveExecutedGtids.String())
	return raftIndex, nil
}

//UpdatePugedGtidset update purged gtid when segment purged or updated by master gtid_purged
func (s *MetaStore) UpdatePugedGtidset(firstIndex uint64) error {
	var gtids *gomysql.MysqlGTIDSet
	var viewErr error

	keyPrefix := utils.StringToBytes(PgePrefix)
	err := s.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(s.BucketName)
		cursor := bucket.Cursor()
		for k, v := cursor.Seek(keyPrefix); k != nil && bytes.HasPrefix(k, keyPrefix); k, v = cursor.Next() {
			keyStr := utils.BytesToString(k)
			strArray := strings.Split(keyStr, "/")
			if len(strArray) != 2 {
				log.Log.Errorf("split string error,keyStr:%s", keyStr)
				return ErrKeyNotFound
			}
			raftIndex, err := strconv.ParseUint(strArray[1], 10, 64)
			if err != nil {
				return ErrKeyNotFound
			}

			//get the proximal previousGtidSet near firstIndex
			//set this previousGtidSet into purgedGtidSet
			if firstIndex < raftIndex {
				gtids, viewErr = gomysql.DecodeMysqlGTIDSet(v)
				if viewErr != nil {
					return viewErr
				}
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if gtids == nil || len(gtids.String()) == 0 {
		log.Log.Warningf("UpdatePugedGtidset:gtids is nil,raftIndex:%d", firstIndex)
		return nil
	}

	gtidPurged, err := s.GetGtidSet(gomysql.MySQLFlavor, GtidPurgedKey)
	if err != nil {
		log.Log.Fatalf("UpdatePugedGtidset:get gtidPurged error,err:%s", err)
	}
	err = gtidPurged.Update(gtids.String())
	if err != nil {
		log.Log.Fatalf("UpdatePugedGtidset:get gtidPurged error,err:%s,gtids:%s", err, gtids.String())
	}

	err = s.SetGtidSet(gomysql.MySQLFlavor, GtidPurgedKey, gtidPurged)
	if err != nil {
		return err
	}

	err = s.deletePreviousGtidSet(firstIndex)
	if err != nil {
		return err
	}

	log.Log.Debugf("set purged gtid set,firstIndex:%d,purgedGtidSet:%s", firstIndex, gtidPurged.String())
	return nil
}

//GetFde get FORMAT_DESCRIPTION_EVENT
func (s *MetaStore) GetFde(preGtidEventIndex uint64) ([]byte, error) {
	var fde []byte
	var viewErr error
	var raftIndex uint64

	keyPrefix := utils.StringToBytes(FdePrefix)
	err := s.DB.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(s.BucketName)
		cursor := bucket.Cursor()

		//get the proximal fde near preGtidEventIndex
		for k, v := cursor.Seek(keyPrefix); k != nil && bytes.HasPrefix(k, keyPrefix); k, v = cursor.Next() {
			keyStr := utils.BytesToString(k)
			strArray := strings.Split(keyStr, "/")
			if len(strArray) != 2 {
				log.Log.Errorf("split string error,keyStr:%s", keyStr)
				return ErrKeyNotFound
			}
			raftIndex, viewErr = strconv.ParseUint(strArray[1], 10, 64)
			if viewErr != nil {
				return ErrKeyNotFound
			}

			if raftIndex < preGtidEventIndex {
				fde = make([]byte, len(v))
				copy(fde, v)
			} else {
				break
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	if len(fde) == 0 {
		log.Log.Errorf("GetFde:fde not be found,preGtidEventIndex:%d", preGtidEventIndex)
		return nil, ErrNotContain
	}

	return fde, nil
}

//GetNextBinlogFile get the next binlog file name
func (s *MetaStore) GetNextBinlogFile(startRaftIndex uint64) (string, error) {
	var fileName string

	err := s.DB.View(func(tx *bolt.Tx) error {
		var viewErr error
		var raftIndex uint64

		bucket := tx.Bucket(s.BucketName)
		cursor := bucket.Cursor()
		keyPrefix := utils.StringToBytes(NextBinlogPrefix)
		for k, v := cursor.Seek(keyPrefix); k != nil && bytes.HasPrefix(k, keyPrefix); k, v = cursor.Next() {
			keyStr := utils.BytesToString(k)
			strArray := strings.Split(keyStr, "/")
			if len(strArray) != 2 {
				log.Log.Errorf("split string error,keyStr:%s", keyStr)
				return ErrKeyNotFound
			}
			raftIndex, viewErr = strconv.ParseUint(strArray[1], 10, 64)
			if viewErr != nil {
				return ErrKeyNotFound
			}

			if raftIndex < startRaftIndex {
				fileName = utils.BytesToString(v)
			} else {
				//if startRaftIndex little than first NextBinlogPrefix, we need generate the file name
				if len(fileName) == 0 {
					fileName = generatePreviousLogName(utils.BytesToString(v))
				}
				//find the last raftIndex little than startRaftIndex
				break
			}
		}
		return nil
	})

	if err != nil {
		return "", err
	}
	if len(fileName) == 0 {
		log.Log.Errorf("key in not found,startRaftIndex:%d", startRaftIndex)
		return "", ErrKeyNotFound
	}
	log.Log.Debugf("GetNextBinlogFile:fileName:%s,startRaftIndex:%d", fileName, startRaftIndex)
	return fileName, nil
}

func generatePreviousLogName(binlogFileName string) string {
	strArray := strings.Split(binlogFileName, ".")
	if len(strArray) != 2 {
		log.Log.Fatalf("binlog file name illegal,binlogFileName:%s", binlogFileName)
	}

	num, err := strconv.ParseUint(strArray[1], 10, 64)
	if err != nil {
		log.Log.Fatalf("binlog file name illegal,binlogFileName:%s", binlogFileName)
	}

	return fmt.Sprintf("%s.%06d", strArray[0], num-1)
}

//Close2 close meta store
func (s *MetaStore) Close2() error {
	return s.DB.Close()
}
