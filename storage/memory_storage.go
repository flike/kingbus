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
	"os"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/utils"
)

//MemoryStorage is Storage implemented in memory
type MemoryStorage struct {
	MetaStorage
	memDB *raft.MemoryStorage
}

//NewMemoryStorage create a memory storage
func NewMemoryStorage(dir string) (*MemoryStorage, error) {
	var err error
	if len(dir) == 0 {
		return nil, ErrArgsIllegal
	}

	if utils.DirExist(dir) == false {
		err = os.MkdirAll(dir, DirMode)
		if err != nil {
			return nil, err
		}
	}
	s := new(MemoryStorage)
	s.memDB = raft.NewMemoryStorage()
	s.MetaStorage, err = NewMetaStore(dir)
	if err != nil {
		log.Log.Errorf("NewMemoryStorage:NewMetaStorage error,err:%s,dir:%s", err.Error(), dir)
		return nil, err
	}
	return s, nil
}

//需要从metadata storage中构造出信息来，然后返回给restartNode
//如果存在信息则返回，否则返回空值
//func (s *MemoryStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
//	return s.metaDB.initialState()
//}

//Entries get raft entries in [lo,hi)
func (s *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	entries, err := s.memDB.Entries(lo, hi, maxSize)
	if err != nil {
		log.Log.Errorf("MemoryStorage:entries error,err:%s,lo:%d,hi:%d,maxSize:%d",
			err.Error(), lo, hi, maxSize)
		return nil, err
	}
	return entries, nil
}

//Term get raft term of raft index
func (s *MemoryStorage) Term(i uint64) (uint64, error) {
	term, err := s.memDB.Term(i)
	if err != nil {
		log.Log.Errorf("MemoryStorage:Term error,err:%s,i:%d",
			err.Error(), i)
		return 0, err
	}
	return term, nil
}

//LastIndex get last raft index
func (s *MemoryStorage) LastIndex() (uint64, error) {
	lastIndex, err := s.memDB.LastIndex()
	if err != nil {
		log.Log.Errorf("MemoryStorage:LastIndex error,err:%s", err.Error())
		return 0, err
	}
	return lastIndex, nil
}

//FirstIndex get first raft index
func (s *MemoryStorage) FirstIndex() (uint64, error) {
	firstIndex, err := s.memDB.FirstIndex()
	if err != nil {
		log.Log.Errorf("MemoryStorage:FirstIndex error,err:%s", err.Error())
		return 0, err
	}
	return firstIndex, nil
}

//Snapshot do not support
func (s *MemoryStorage) Snapshot() (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, nil
}

//SaveRaftEntries save raft entries in storage
func (s *MemoryStorage) SaveRaftEntries(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	err := s.memDB.Append(entries)
	if err != nil {
		log.Log.Errorf("MemoryStorage:SaveRaftEntries append error,err:%s,entries:%v", err.Error(), entries)
		return err
	}
	return nil
}

//TruncateSuffix not implement
func (s *MemoryStorage) TruncateSuffix(i uint64) error {
	return nil
}

//StartPurgeLog not implement
func (s *MemoryStorage) StartPurgeLog() {

}

//StopPurgeLog not implement
func (s *MemoryStorage) StopPurgeLog() {
}

//Close memory storage
func (s *MemoryStorage) Close() error {
	var err error
	err = s.MetaStorage.Close2()
	if err != nil {
		return err
	}

	return nil
}

//NewEntryReaderAt not implement
func (s *MemoryStorage) NewEntryReaderAt(raftIndex uint64) (EntryReader, error) {
	return nil, nil
}

//MemoryEventReader not implement
type MemoryEventReader struct {
}

//GetNext not implement
func (s *MemoryEventReader) GetNext() (*raftpb.Entry, error) {
	return nil, nil
}

//NextRaftIndex not implement
func (s *MemoryEventReader) NextRaftIndex() uint64 {
	return 0
}
