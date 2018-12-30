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
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/flike/kingbus/utils"

	"hash/crc32"

	etcdraft "github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/storage/storagepb"
	"go.uber.org/atomic"
)

var (
	noLimit uint64 = math.MaxUint64
	//SegmentSize is the size of segment file
	SegmentSize     int64 = 1024 * 1024 * 1024 //1GB
	commonSliceSize       = 200
)

const (
	//FileMode is default file mode
	FileMode = 0600
	//DirMode is default dir mode
	DirMode = 0700
)

//DiskStorage is the Storage store data in disk
type DiskStorage struct {
	Dir string

	Mu       sync.RWMutex
	Segments []*Segment

	ReserveSegmentCount int
	purgeStarted        *atomic.Bool
	purgeCtx            context.Context
	purgeCancel         context.CancelFunc
	purgeQuitC          chan struct{}

	MetaStorage
}

//NewDiskStorage create a disk storage
func NewDiskStorage(dir string, reserveSizeInGB int) (*DiskStorage, error) {
	var err error
	if len(dir) == 0 {
		return nil, ErrArgsIllegal
	}

	s := new(DiskStorage)
	s.Dir = dir
	s.ReserveSegmentCount = reserveSizeInGB * 1024 * 1024 * 1024 / int(SegmentSize)

	if utils.DirExist(s.Dir) == false {
		err = os.MkdirAll(s.Dir, DirMode)
		if err != nil {
			return nil, err
		}
	}

	s.MetaStorage, err = NewMetaStore(s.Dir)
	if err != nil {
		log.Log.Errorf("NewMetaStore error,err:%s,dir:%s", err, dir)
		return nil, err
	}

	s.initSegments()
	s.purgeStarted = atomic.NewBool(false)
	//set recover flag
	s.setRecover(true)
	go s.StartPurgeLog()
	return s, nil
}

func (s *DiskStorage) initSegments() {
	//dir exist,but not include log file
	if utils.ExistLog(s.Dir) == false {
		s.Segments = make([]*Segment, 0, 100)
		return
	}

	//dir exist,and include log file
	needRecover := s.getRecover()
	log.Log.Debugf("initSegments:needRecover is:%v", needRecover)
	s.Segments = openSegments(s.Dir, needRecover)
	return
}

//Entries get raft entries in [lo,hi)
func (s *DiskStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	last := hi - 1
	beginReadTime := time.Now()
	//check the args availability
	if last < lo {
		return nil, ErrArgsIllegal
	}
	err := s.checkRaftIndex(lo)
	if err != nil {
		log.Log.Errorf("checkRaftIndex error,err:%s,lo:%d,hi:%d", err, lo, hi)
		return nil, err
	}
	err = s.checkRaftIndex(last)
	if err != nil {
		log.Log.Errorf("checkRaftIndex error,err:%s,lo:%d,hi:%d", err, lo, hi)
		return nil, err
	}

	//read entries from segments
	segments, err := s.getReadSegments(lo, last)
	if err != nil {
		log.Log.Errorf("checkRaftIndex error,err:%s,lo:%d,hi:%d", err, lo, hi)
		return nil, err
	}
	entries, err := s.readSegments(segments, lo, last, maxSize)
	if err != nil {
		log.Log.Errorf("checkRaftIndex error,err:%s,lo:%d,hi:%d", err, lo, hi)
		return nil, err
	}

	//cut the raft entries with size
	entries = s.cutRaftEntries(entries, maxSize)
	readLatency.Update(int64(time.Now().Sub(beginReadTime) / time.Millisecond))
	return entries, nil
}

//todo test one entry large than maxSize
func (s *DiskStorage) cutRaftEntries(entries []raftpb.Entry, maxSize uint64) []raftpb.Entry {
	if len(entries) == 0 {
		return entries
	}
	size := len(entries[0].Data)
	var limit int
	for limit = 1; limit < len(entries); limit++ {
		size += len(entries[limit].Data)
		if uint64(size) > maxSize {
			break
		}
	}

	//check raft entries continuity
	for i := 0; i < len(entries)-1; i++ {
		if entries[i].Index+1 != entries[i+1].Index {
			for j := 0; j < len(entries); j++ {
				log.Log.Infof("j:%d, raftlog[j].Term:%d, raftlog[j].Index:%d,"+
					"raftlog[j].Type:%d, raftlog[j].Data:%v", j, entries[j].Term, entries[j].Index,
					entries[j].Type, entries[j].Data)
			}
			log.Log.Fatalf("cutRaftEntries:read raft log from storage is not continuous,i:%d", i)
		}
	}

	readTps.Mark(int64(len(entries)))
	readThroughput.Mark(int64(size))

	return entries[:limit]
}

//Term get raft term of raft index
func (s *DiskStorage) Term(i uint64) (uint64, error) {
	if i == 0 {
		return 0, nil
	}

	err := s.checkRaftIndex(i)
	if err != nil {
		log.Log.Errorf("checkRaftIndex error,err:%s,i:%d", err, i)
		return 0, err
	}

	entries, err := s.Entries(i, i+1, noLimit)
	if err != nil {
		return 0, err
	}
	if len(entries) == 0 {
		log.Log.Errorf("get term by index %d occur ErrUnavailable", i)
		return 0, etcdraft.ErrUnavailable
	}
	return entries[0].Term, nil
}

//LastIndex get last raft index
func (s *DiskStorage) LastIndex() (uint64, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	if len(s.Segments) == 0 {
		return 0, nil
	}
	return s.Segments[len(s.Segments)-1].getLastIndex(), nil
}

//FirstIndex get first raft index
func (s *DiskStorage) FirstIndex() (uint64, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	if len(s.Segments) == 0 {
		return 1, nil
	}
	return s.Segments[0].getFirstIndex(), nil
}

//Snapshot not implement
func (s *DiskStorage) Snapshot() (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, etcdraft.ErrSnapshotTemporarilyUnavailable
}

//TruncateSuffix truncate raft entry to the raft index is i, include i.
func (s *DiskStorage) TruncateSuffix(raftIndex uint64) error {
	var (
		truncateSegment *Segment
		segments        []*Segment
		err             error
	)

	s.Mu.Lock()
	//find the segment index of i
	segmentIndex := -1
	for i := len(s.Segments) - 1; 0 <= i; i-- {
		l := s.Segments[i].getFirstIndex()
		h := s.Segments[i].getLastIndex()

		if l <= raftIndex && raftIndex <= h {
			segmentIndex = i
			truncateSegment = s.Segments[i]
			if i < len(s.Segments)-1 {
				segments = s.Segments[i+1:]
				//delete the purge segments
				s.Segments = s.Segments[:i+1]
			}
			break
		}
	}
	s.Mu.Unlock()

	if segmentIndex == -1 {
		return ErrOutOfBound
	}

	//purge segments
	if len(segments) != 0 {
		err = s.purgeSegments(segments, false)
		if err != nil {
			log.Log.Errorf("purgeSegments error,err:%s,len(segments):%v", err, len(segments))
			return err
		}
	}

	if truncateSegment != nil {
		err = truncateSegment.truncateSuffix(raftIndex)
		if err != nil {
			log.Log.Errorf("truncateSuffix error,err:%s,segmentPath:%s,raftIndex:%d",
				err, truncateSegment.LogFile.filePath, raftIndex)
			return err
		}
	}

	return nil
}

func (s *DiskStorage) purgeSegments(segments []*Segment, delay bool) error {
	if delay {
		<-time.After(time.Second * 4)
	}

	//do not need lock,
	firstIndex, err := s.FirstIndex()
	if err != nil {
		return err
	}

	err = s.MetaStorage.UpdatePugedGtidset(firstIndex)
	if err != nil {
		return err
	}
	for i := 0; i < len(segments); i++ {
		err := segments[i].remove()
		if err != nil {
			log.Log.Errorf("segment remove error,err:%s,segmentPath:%s", err, segments[i].LogFile.filePath)
			return err
		}
	}

	return nil
}

//SaveRaftEntries save raft entries in storage
func (s *DiskStorage) SaveRaftEntries(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	beginSaveTime := time.Now()

	startIndex := entries[0].Index
	records, recordsSize := entriesToRecords(entries)
	if SegmentSize < int64(recordsSize) {
		log.Log.Fatalf("recordsSize[%d] is large than SegmentSize[%d]", recordsSize, SegmentSize)
	}

	writeSegment, isNew, err := s.getWriteSegment(startIndex, recordsSize)
	if err != nil {
		log.Log.Errorf("getWriteSegment error,err:%s,index:%d,size:%d", err, entries[0].Index, recordsSize)
		return err
	}

	err = writeSegment.appendRecords(records, startIndex)
	if err != nil {
		log.Log.Errorf("appendRaftEntries,error,err:%s,len(entries):%d", err, len(entries))
		return err
	}

	if isNew {
		s.Mu.Lock()
		s.Segments = append(s.Segments, writeSegment)
		s.Mu.Unlock()
	}

	writeTps.Mark(int64(len(records)))
	writeThroughput.Mark(int64(recordsSize))
	writeLatency.Update(int64(time.Now().Sub(beginSaveTime) / time.Millisecond))

	return nil
}

func entriesToRecords(entries []raftpb.Entry) ([]*storagepb.Record, int) {
	var size int
	var err error

	records := make([]*storagepb.Record, 0, len(entries))
	for i := 0; i < len(entries); i++ {
		r := new(storagepb.Record)

		r.Data, err = entries[i].Marshal()
		if err != nil {
			log.Log.Fatalf("entry marshal error,err:%s,entry:%s", err, entries[i])
		}
		r.Crc = crc32.ChecksumIEEE(r.Data)
		records = append(records, r)

		size = size + r.Size() + RecordLengthSize
	}
	return records, size
}

//Close storage
func (s *DiskStorage) Close() error {
	if s.purgeStarted.Load() == true {
		//close log purger
		s.StopPurgeLog()
	}

	//close segments
	s.Mu.Lock()
	for i := 0; i < len(s.Segments); i++ {
		s.Segments[i].close()
	}
	s.Mu.Unlock()

	//set normal quit flag, don't need recover
	s.setRecover(false)
	//close meta storage
	s.MetaStorage.Close2()
	return nil
}

//StartPurgeLog implements purge the expired log files
func (s *DiskStorage) StartPurgeLog() {
	//already started
	if s.purgeStarted.Load() == true {
		return
	}

	s.purgeCtx, s.purgeCancel = context.WithCancel(context.Background())
	s.purgeQuitC = make(chan struct{})
	s.purgeStarted.Store(true)
	timer := time.NewTimer(time.Second * 30)

	defer func() {
		close(s.purgeQuitC)
		timer.Stop()
	}()

	var deleteSegments []*Segment
	for {
		select {
		case <-s.purgeCtx.Done():
			log.Log.Info("log purger quit")
			return
		case <-timer.C:
			// check and purge segments
			s.Mu.Lock()
			if s.ReserveSegmentCount < len(s.Segments) {
				deleteCount := len(s.Segments) - s.ReserveSegmentCount
				deleteSegments = s.Segments[:deleteCount]
				s.Segments = s.Segments[deleteCount:]
			}
			s.Mu.Unlock()

			if len(deleteSegments) == 0 {
				continue
			}
			err := s.purgeSegments(deleteSegments, true)
			if err != nil {
				log.Log.Fatalf("purgeSegments error, err:%s, deleteSegments:%v", err, deleteSegments)
			}
		}
	}
}

//StopPurgeLog stop purge log
func (s *DiskStorage) StopPurgeLog() {
	if s.purgeStarted.Load() == false {
		return
	}
	s.purgeCancel()
	<-s.purgeQuitC
	s.purgeStarted.Store(false)
}

func (s *DiskStorage) getRecover() bool {
	v, err := s.MetaStorage.Get(utils.StringToBytes(NeedRecoverKey))
	if err != nil {
		log.Log.Fatalf("MetaStorage Get error,err:%s,key:%s", err, NeedRecoverKey)
	}
	//key is not exist
	if v == nil {
		return false
	}
	if utils.BytesToString(v) == "true" {
		return true
	}
	return false
}

func (s *DiskStorage) setRecover(needRecover bool) {
	var v []byte
	if needRecover {
		v = utils.StringToBytes("true")
	} else {
		v = utils.StringToBytes("false")
	}
	err := s.MetaStorage.Set(utils.StringToBytes(NeedRecoverKey), v)
	if err != nil {
		log.Log.Fatalf("MetaStorage Set error,err:%s,key:%s", err, NeedRecoverKey)
	}
}

//[first,last]
func (s *DiskStorage) getReadSegments(first uint64, last uint64) ([]*Segment, error) {
	var (
		firstSegmentIndex = -1
		lastSegmentIndex  = -1
	)
	segments := make([]*Segment, 0, 2)

	s.Mu.RLock()
	defer s.Mu.RUnlock()

	for i := len(s.Segments) - 1; 0 <= i; i-- {
		l := s.Segments[i].getFirstIndex()
		h := s.Segments[i].getLastIndex()

		if l <= first && first <= h {
			firstSegmentIndex = i
		}
		if l <= last && last <= h {
			lastSegmentIndex = i
		}

		if firstSegmentIndex != -1 && lastSegmentIndex != -1 {
			break
		}
	}

	if firstSegmentIndex == -1 {
		return nil, etcdraft.ErrCompacted
	}

	if lastSegmentIndex == -1 || firstSegmentIndex > lastSegmentIndex {
		return nil, ErrOutOfBound
	}

	//deep copy, so we can use segments independently
	for i := firstSegmentIndex; i <= lastSegmentIndex; i++ {
		segments = append(segments, s.Segments[i])
	}
	return segments, nil
}

func (s *DiskStorage) readSegments(segments []*Segment, first uint64,
	last uint64, maxSize uint64) ([]raftpb.Entry, error) {
	var entries []raftpb.Entry

	remainingSize := maxSize
	readEntriesCount := int(last - first + 1)

	//create entries with a limit size
	if readEntriesCount < commonSliceSize {
		entries = make([]raftpb.Entry, 0, readEntriesCount)
	} else {
		entries = make([]raftpb.Entry, 0, commonSliceSize)
	}

	for i := 0; i < len(segments); i++ {
		var begin, end uint64

		segmentFirstIndex := segments[i].getFirstIndex()
		segmentLastIndex := segments[i].getLastIndex()

		if segmentFirstIndex < first {
			begin = first
		} else {
			begin = segmentFirstIndex
		}

		if segmentLastIndex < last {
			end = segmentLastIndex
		} else {
			end = last
		}

		segmentEntries, remainingSize, err := segments[i].readRaftEntries(begin, end, remainingSize)
		if err != nil {
			log.Log.Errorf("readRaftEntries error,err:%s,i:%d,begin:%d,end:%d,remainingSize:%d,", err, i, begin, end, remainingSize)
			return nil, err
		}
		entries = append(entries, segmentEntries...)
		//if read size is large than maxSize, should break the loop
		if remainingSize == 0 {
			break
		}
	}
	return entries, nil
}

//get the writing segment, if the writing segment is full
//rolling to the new segment
func (s *DiskStorage) getWriteSegment(firstIndex uint64, recordsSize int) (*Segment, bool, error) {
	var writeSegment *Segment

	s.Mu.Lock()
	if 0 < len(s.Segments) {
		writeSegment = s.Segments[len(s.Segments)-1]
	}
	s.Mu.Unlock()

	//first write raft entries into this segment
	if writeSegment == nil {
		newSegment, err := s.rollingSegment(nil, firstIndex)
		if err != nil {
			log.Log.Errorf("rollingSegment error,err:%s,firstIndex:%d", err, firstIndex)
			return nil, false, err
		}
		return newSegment, true, nil
	}

	//the write segment is full, need rolling
	if writeSegment.needRolling(recordsSize) {
		newSegment, err := s.rollingSegment(writeSegment, firstIndex)
		if err != nil {
			log.Log.Errorf("rollingSegment error,err:%s,firstIndex:%d", err, firstIndex)
			return nil, false, err
		}
		return newSegment, true, nil
	}

	return writeSegment, false, nil
}

//as the raft lib SaveRaftEntries executed serially,
//so do not need lock in rollingSegment
func (s *DiskStorage) rollingSegment(prevLastSegment *Segment, firstIndex uint64) (*Segment, error) {
	if prevLastSegment != nil {
		err := prevLastSegment.changeToReadonly()
		if err != nil {
			log.Log.Errorf("changeToReadonly error,err:%s,segmentPath:%s", err, prevLastSegment.LogFile.filePath)
			return nil, err
		}
	}

	newSegmentName := fmt.Sprintf(ReadWriteSegmentPattern, firstIndex)
	segment := newSegment(s.Dir, newSegmentName)

	return segment, nil
}

//DiskEntryReader is a raft entry reader
type DiskEntryReader struct {
	//the index of reader read at
	indexReadAt uint64
	store       *DiskStorage
}

//NewEntryReaderAt create a DiskEntryReader at raftIndex
func (s *DiskStorage) NewEntryReaderAt(raftIndex uint64) (EntryReader, error) {
	err := s.checkRaftIndex(raftIndex)
	if err != nil {
		log.Log.Errorf("checkRaftIndex error,err:%s,raftIndex:%d", err, raftIndex)
		return nil, err
	}

	reader := new(DiskEntryReader)
	reader.indexReadAt = raftIndex
	reader.store = s

	return reader, nil
}

//GetNext get the next raft entry
func (r *DiskEntryReader) GetNext() (*raftpb.Entry, error) {
	entries, err := r.store.Entries(r.indexReadAt, r.indexReadAt+1, noLimit)
	if err != nil {
		log.Log.Errorf("get entries error,err:%s,index:%d", err, r.indexReadAt)
		return nil, err
	}

	r.indexReadAt++
	return &entries[0], nil
}

//NextRaftIndex get the next raft index
func (r *DiskEntryReader) NextRaftIndex() uint64 {
	return r.indexReadAt
}

func (s *DiskStorage) checkRaftIndex(raftIndex uint64) error {
	firstIndex, err := s.FirstIndex()
	if err != nil {
		return err
	}
	lastIndex, err := s.LastIndex()
	if err != nil {
		return err
	}

	if raftIndex < firstIndex {
		log.Log.Errorf("checkRaftIndex:raftIndex:%d,firstIndex:%d,lastIndex:%d", raftIndex, firstIndex, lastIndex)
		return etcdraft.ErrCompacted
	}

	if raftIndex > lastIndex {
		log.Log.Errorf("checkRaftIndex:raftIndex:%d,firstIndex:%d,lastIndex:%d", raftIndex, firstIndex, lastIndex)
		return ErrOutOfBound
	}
	return nil
}
