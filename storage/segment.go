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
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/flike/kingbus/storage/storagepb"

	"github.com/flike/kingbus/log"

	"github.com/flike/kingbus/utils"

	"github.com/coreos/etcd/raft/raftpb"
)

//SegmentStatus is the status of segment file
type SegmentStatus int8

const (
	//SegmentReadOnly represents segment file read only
	SegmentReadOnly SegmentStatus = iota
	//SegmentRDWR represents segment file read write
	SegmentRDWR
	//SegmentClosed represents segment file is closed
	SegmentClosed
)

const (
	//ReadonlySegmentPattern is the read only segment file name pattern
	ReadonlySegmentPattern = "%020d-%020d.log"
	//ReadonlyIndexPattern is the read only index file name pattern
	ReadonlyIndexPattern = "%020d-%020d.index"
	//ReadWriteSegmentPattern is the read write segment file name pattern
	ReadWriteSegmentPattern = "%020d-inprogress.log"
	//ReadWriteIndexPattern is the read only index file name pattern
	ReadWriteIndexPattern = "%020d-inprogress.index"
	//RecordLengthSize is the record length size
	RecordLengthSize int = 4
)

//Segment is segment file
type Segment struct {
	Mu         sync.RWMutex
	FirstIndex uint64
	LastIndex  uint64

	LogFile  *MmapFile
	LogIndex *Index
	Status   SegmentStatus
}

func newSegment(dir string, name string) *Segment {
	s := new(Segment)

	s.FirstIndex = 0
	s.LastIndex = 0
	s.LogFile = newMmapFile(dir, name, SegmentSize)

	indexFileName := utils.GetIndexName(name)
	s.LogIndex = newIndex(dir, indexFileName)
	s.Status = SegmentRDWR
	return s
}

//todo test the order is right?
func openSegments(dir string, needRecover bool) []*Segment {
	//files have been sorted by name
	files, err := utils.ReadDir(dir)
	if err != nil {
		log.Log.Fatalf("ReadDir error,err:%s,dir:%s", err, dir)
	}

	logFiles := make([]os.FileInfo, 0, 10)
	for _, file := range files {
		if strings.HasSuffix(file.Name(), utils.LogSuffix) {
			logFiles = append(logFiles, file)
		}
	}

	fileCount := len(logFiles)
	segments := make([]*Segment, 0, 10)
	if fileCount == 0 {
		return segments
	}

	for i := 0; i < fileCount-1; i++ {
		s := openSegment(dir, logFiles[i].Name(), SegmentReadOnly, false)
		segments = append(segments, s)
	}

	//the last segment if need rebuild index file,
	//depend on the needRecover flag
	s := openSegment(dir, logFiles[fileCount-1].Name(), SegmentRDWR, needRecover)
	segments = append(segments, s)

	return segments
}

func openSegment(dir string, name string, status SegmentStatus, needRecover bool) *Segment {
	s := new(Segment)
	indexFileName := utils.GetIndexName(name)

	s.LogFile = openMmapFile(dir, name)
	if status == SegmentReadOnly {
		n, err := fmt.Sscanf(name, ReadonlySegmentPattern, &(s.FirstIndex), &(s.LastIndex))
		if err != nil || n != 2 {
			log.Log.Fatalf("Sscanf error,err:%s,name:%s", err, name)
		}
		s.LogIndex = openIndex(dir, indexFileName, SegmentReadOnly)
	} else {
		n, err := fmt.Sscanf(name, ReadWriteSegmentPattern, &(s.FirstIndex))
		if err != nil || n != 1 {
			log.Log.Fatalf("Sscanf error,err:%s,name:%s", err, name)
		}
		if needRecover {
			s.LogIndex = s.rebuildIndex()
		} else {
			s.LogIndex = openIndex(dir, indexFileName, SegmentRDWR)
		}

		lastIndexEntry := s.LogIndex.getLastRaftEntry(SegmentRDWR)
		s.LastIndex = lastIndexEntry.RaftIndex

		//set write position
		lastRecordPosition := int(lastIndexEntry.FilePosition)
		lastRecordLen := binary.LittleEndian.Uint32(s.LogFile.mappedData[lastRecordPosition : lastRecordPosition+RecordLengthSize])
		nextWritePos := int(lastIndexEntry.FilePosition) + int(lastRecordLen) + RecordLengthSize
		s.LogFile.setPosition(nextWritePos)
	}
	s.Status = status
	return s
}

func (s *Segment) rebuildIndex() *Index {
	var record storagepb.Record
	var raftEntry raftpb.Entry

	//1.remove old index file
	indexPath := utils.GetIndexPath(s.LogFile.filePath)
	err := os.Remove(indexPath)
	if err != nil {
		log.Log.Fatalf("Remove file error,err:%s,filePath:%s", err, indexPath)
	}

	//2.new index
	logIndex := newIndex(path.Dir(indexPath), path.Base(indexPath))

	//3.parse segment and rebuild index
	position := 0
	segmentMaxSize := int(SegmentSize)

	for {
		if segmentMaxSize <= position+RecordLengthSize {
			break
		}
		recordLen := binary.LittleEndian.Uint32(s.LogFile.mappedData[position : position+RecordLengthSize])
		position += RecordLengthSize
		if segmentMaxSize <= position || segmentMaxSize < position+int(recordLen) {
			break
		}

		recordBuf := s.LogFile.mappedData[position : position+int(recordLen)]
		if len(recordBuf) == 0 {
			break
		}

		err := record.Unmarshal(recordBuf)
		if err != nil {
			log.Log.Errorf("record Unmarshal error,err:%s,buf:%v", err, recordBuf)
			break
		}

		crc := crc32.ChecksumIEEE(record.Data)
		if crc != record.Crc {
			log.Log.Errorf("crc not equal,record.crc:%d,calculatedCrc:%d,buf:%v", record.Crc, crc, recordBuf)
			break
		}

		err = raftEntry.Unmarshal(record.Data)
		if err != nil {
			log.Log.Errorf("entry Unmarshal error,err:%s,buf:%v", err, record.Data)
			break
		}
		indexEntry := &IndexEntry{
			RaftIndex:    raftEntry.Index,
			FilePosition: uint32(position - RecordLengthSize),
		}

		err = logIndex.appendIndexEntry(indexEntry)
		if err != nil {
			log.Log.Fatalf("appendIndexEntry error,err:%s,indexEntry:%v", err, *indexEntry)
		}
		position += int(recordLen)
		//log.Log.Debugf("rebuildIndex:position is %d,segmentMaxSize is %d", position, segmentMaxSize)
	}

	log.Log.Debugf("rebuildIndex success,indexName:%s,entry size:%d",
		logIndex.readWrite.filePath, len(logIndex.readWrite.entries))
	return logIndex
}

func (s *Segment) getFirstIndex() uint64 {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.FirstIndex
}

func (s *Segment) getLastIndex() uint64 {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.LastIndex
}

//append raft entries into segment
func (s *Segment) appendRecords(records []*storagepb.Record, startIndex uint64) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	var err error

	if s.Status != SegmentRDWR {
		return ErrNotWritable
	}
	//check the continuity of raft index in segment
	if s.LastIndex != 0 && s.LastIndex+1 != startIndex {
		return ErrNotContinuous
	}

	for i := 0; i < len(records); i++ {
		err = s.appendRecord(records[i], startIndex+uint64(i))
		if err != nil {
			return err
		}
	}

	//sync log file
	err = s.LogFile.sync()
	if err != nil {
		return err
	}

	//update the segment raft index range
	if s.FirstIndex == uint64(0) {
		s.FirstIndex = startIndex
	}
	if s.LastIndex < startIndex+uint64(len(records)-1) {
		s.LastIndex = startIndex + uint64(len(records)-1)
	}

	return nil
}

func (s *Segment) appendRecord(record *storagepb.Record, raftIndex uint64) error {
	var err error

	recordBuf, err := record.Marshal()
	if err != nil {
		return err
	}

	recordLengthBuf := make([]byte, RecordLengthSize)
	binary.LittleEndian.PutUint32(recordLengthBuf, uint32(record.Size()))

	startPos, err := s.LogFile.append(recordLengthBuf)
	if err != nil {
		log.Log.Errorf("append recordLengthBuf error,err:%s", err)
		return err
	}
	_, err = s.LogFile.append(recordBuf)
	if err != nil {
		log.Log.Errorf("append recordBuf error,err:%s", err)
		return err
	}

	//append raftIndex entry
	indexEntry := &IndexEntry{
		RaftIndex:    raftIndex,
		FilePosition: uint32(startPos),
	}

	err = s.LogIndex.appendIndexEntry(indexEntry)
	if err != nil {
		log.Log.Errorf("append raftIndex entry error,err:%s", err)
		return err
	}

	return nil
}

func (s *Segment) readRaftEntries(first uint64, last uint64, maxSize uint64) ([]raftpb.Entry, uint64, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	var (
		entries  []raftpb.Entry
		readSize uint64
	)

	if s.Status == SegmentClosed {
		return nil, 0, ErrClosed
	}

	readEntriesCount := int(last - first + 1)

	//create entries with a limit size
	if readEntriesCount < commonSliceSize {
		entries = make([]raftpb.Entry, 0, readEntriesCount)
	} else {
		entries = make([]raftpb.Entry, 0, commonSliceSize)
	}

	//get the file position of first
	position, err := s.LogIndex.getRaftEntryPosition(first, s.FirstIndex, s.Status)
	if err != nil {
		return nil, 0, err
	}

	for {
		entry, nextReadPosition := s.LogFile.readRaftEntry(position)
		entries = append(entries, entry)
		readSize += uint64(entry.Size())

		if readSize > maxSize {
			break
		}
		if entry.Index == last {
			break
		}
		if entry.Index == s.LastIndex {
			break
		}
		//update the read position
		position = nextReadPosition
	}
	if readSize > maxSize {
		return entries, 0, nil
	}

	return entries, maxSize - readSize, nil
}

func (s *Segment) needRolling(size int) bool {
	s.Mu.RLock()
	defer s.Mu.RUnlock()
	return s.LogFile.writePosition+size > s.LogFile.maxBytes
}

func (s *Segment) close() {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	s.Status = SegmentClosed
	s.FirstIndex = 0
	s.LastIndex = 0

	err := s.LogFile.close()
	if err != nil {
		log.Log.Fatalf("close segment file error,err:%s,filePath:%s", err, s.LogFile.filePath)
	}
	err = s.LogIndex.close()
	if err != nil {
		log.Log.Fatalf("close index file error,err:%s,segmentPath:%s", err, s.LogFile.filePath)
	}
}

func (s *Segment) truncateSuffix(i uint64) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	//truncate segment file
	segmentPos, err := s.LogIndex.getRaftEntryPosition(i, s.FirstIndex, s.Status)
	if err != nil {
		return err
	}
	err = s.LogFile.truncateSuffix(segmentPos)
	if err != nil {
		return err
	}

	//truncate index file
	if s.Status == SegmentRDWR {
		err = s.LogIndex.truncateSuffix(i, s.FirstIndex, SegmentRDWR)
		if err != nil {
			return err
		}
	} else {
		//rename LogFile
		newName := fmt.Sprintf(ReadWriteSegmentPattern, s.FirstIndex)
		err := s.LogFile.rename(newName)
		if err != nil {
			return err
		}

		err = s.LogIndex.truncateSuffix(i, s.FirstIndex, SegmentReadOnly)
		if err != nil {
			return err
		}

		s.Status = SegmentRDWR
	}

	//update segment range
	if i == s.FirstIndex {
		s.FirstIndex = 0
		s.LastIndex = 0
	} else {
		s.LastIndex = i - 1
	}
	return nil
}

func (s *Segment) changeToReadonly() error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if s.Status != SegmentRDWR {
		return ErrArgsIllegal
	}

	//change the name from "%020d-inprogress.log" to "%020d-%020d.log"
	newName := fmt.Sprintf(ReadonlySegmentPattern, s.FirstIndex, s.LastIndex)
	err := s.LogFile.rename(newName)
	if err != nil {
		return err
	}

	err = s.LogIndex.changeToReadonly()
	if err != nil {
		return err
	}

	s.Status = SegmentReadOnly
	return nil
}

func (s *Segment) remove() error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	s.Status = SegmentClosed
	s.FirstIndex = 0
	s.LastIndex = 0

	err := s.LogFile.remove()
	if err != nil {
		log.Log.Errorf("remove segment error,err:%s,filePath:%s", err, s.LogFile.filePath)
		return err
	}
	err = s.LogIndex.remove()
	if err != nil {
		log.Log.Errorf("remove logIndex error,err:%s", err)
		return err
	}

	return nil
}
