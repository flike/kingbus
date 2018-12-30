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
	"hash/crc32"
	"os"
	"path"
	"syscall"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/storage/storagepb"
	"github.com/flike/kingbus/utils"

	"github.com/coreos/etcd/raft/raftpb"
)

//MmapFile is the file mmaped in os
type MmapFile struct {
	file          *os.File
	filePath      string
	mappedData    []byte
	writePosition int
	syncPosition  int
	maxBytes      int
}

func newMmapFile(dir string, name string, fileSize int64) *MmapFile {
	var err error

	f := new(MmapFile)
	f.filePath = path.Join(dir, name)
	f.file, err = os.OpenFile(f.filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, FileMode)
	if err != nil {
		log.Log.Fatalf("OpenFile error,err:%s,filePath:%s", err, f.filePath)
	}

	err = fileutil.Preallocate(f.file, fileSize, true)
	if err != nil {
		log.Log.Fatalf("Preallocate error,err:%s,filePath:%s,fileSize:%d", err, f.filePath, fileSize)
	}

	f.mappedData, err = syscall.Mmap(int(f.file.Fd()), 0, int(fileSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_NORESERVE)
	if err != nil {
		log.Log.Fatalf("Mmap error,err:%s,filePath:%s,fileSize:%d", err, f.filePath, fileSize)
	}

	f.writePosition = 0
	f.syncPosition = 0
	f.maxBytes = int(fileSize)

	return f
}

func openMmapFile(dir string, name string) *MmapFile {
	var err error

	f := new(MmapFile)
	f.filePath = path.Join(dir, name)
	f.file, err = os.OpenFile(f.filePath, os.O_RDWR, FileMode)
	if err != nil {
		log.Log.Fatalf("OpenFile error,err:%s,filePath:%s", err, f.filePath)
	}

	fileInfo, err := f.file.Stat()
	if err != nil {
		log.Log.Fatalf("Stat error,err:%s,filePath:%s", err, f.filePath)
	}
	fileSize := fileInfo.Size()

	err = fileutil.Preallocate(f.file, fileSize, true)
	if err != nil {
		log.Log.Fatalf("Preallocate error,err:%s,filePath:%s,fileSize:%d", err, f.filePath, fileSize)
	}

	f.mappedData, err = syscall.Mmap(int(f.file.Fd()), 0, int(fileSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_NORESERVE)
	if err != nil {
		log.Log.Fatalf("Mmap error,err:%s,filePath:%s,fileSize:%d", err, f.filePath, fileSize)
	}

	f.writePosition = 0
	f.syncPosition = 0
	f.maxBytes = int(fileInfo.Size())

	return f
}

func (f *MmapFile) append(data []byte) (int, error) {
	var startPos int
	startPos = f.writePosition
	copy(f.mappedData[startPos:startPos+int(len(data))], data)
	f.writePosition += len(data)
	return startPos, nil
}

func (f *MmapFile) sync() error {
	err := utils.Syncfilerange(
		f.file.Fd(),
		int64(f.syncPosition),
		int64(f.writePosition-f.syncPosition),
		utils.SYNC_FILE_RANGE_WAIT_BEFORE|utils.SYNC_FILE_RANGE_WRITE|utils.SYNC_FILE_RANGE_WAIT_AFTER,
	)
	if err != nil {
		log.Log.Errorf("Syncfilerange error,err:%s,file:%s",
			err, f.file.Name())
		return err
	}
	f.syncPosition = f.writePosition
	return nil
}

func (f *MmapFile) setPosition(position int) {
	if position < 0 {
		log.Log.Fatalf("setPosition error,position is %d", position)
	}
	f.writePosition = position
	f.syncPosition = position
}

func (f *MmapFile) readRaftEntry(position int) (raftpb.Entry, int) {
	var record storagepb.Record
	var entry raftpb.Entry

	recordLen := binary.LittleEndian.Uint32(f.mappedData[position : position+RecordLengthSize])
	position += RecordLengthSize
	endPosition := position + int(recordLen)

	recordBuf := f.mappedData[position:endPosition]
	err := record.Unmarshal(recordBuf)
	if err != nil {
		log.Log.Fatalf("record Unmarshal error,err:%s,buf:%v", err, recordBuf)
	}
	crc := crc32.ChecksumIEEE(record.Data)
	if crc != record.Crc {
		log.Log.Fatalf("crc not equal,record.crc:%d,calculatedCrc:%d,buf:%v", record.Crc, crc, recordBuf)
	}

	err = entry.Unmarshal(record.Data)
	if err != nil {
		log.Log.Fatalf("entry Unmarshal error,err:%s,buf:%v", err, record.Data)
	}
	return entry, endPosition
}

func (f *MmapFile) readIndexEntry(position int) *IndexEntry {
	var entry IndexEntry
	indexEntryBuf := f.mappedData[position : position+IndexEntrySize]
	err := entry.Unmarshal(indexEntryBuf)
	if err != nil {
		log.Log.Fatalf("index entry Unmarshal error,err:%s,buf:%v", err, indexEntryBuf)
	}
	return &entry
}

func (f *MmapFile) close() error {
	err := syscall.Munmap(f.mappedData)
	if err != nil {
		log.Log.Errorf("Munmap error,err:%s,file:%s", err, f.file.Name())
		return err
	}
	err = f.file.Close()
	if err != nil {
		log.Log.Errorf("Close error,err:%s,file:%s", err, f.file.Name())
		return err
	}
	return nil
}

func (f *MmapFile) remove() error {
	err := f.close()
	if err != nil {
		log.Log.Errorf("close error,err:%s,file:%s", err, f.file.Name())
		return err
	}
	err = os.Remove(f.filePath)
	if err != nil {
		return err
	}
	return nil
}

func (f *MmapFile) truncateSuffix(position int) error {
	if f.writePosition < position {
		return ErrOutOfBound
	}
	zeroDataLen := f.writePosition - position
	zeroBuf := make([]byte, zeroDataLen)
	copy(f.mappedData[position:position+zeroDataLen], zeroBuf)
	err := utils.Syncfilerange(
		f.file.Fd(),
		int64(position),
		int64(zeroDataLen),
		utils.SYNC_FILE_RANGE_WAIT_BEFORE|utils.SYNC_FILE_RANGE_WRITE|utils.SYNC_FILE_RANGE_WAIT_AFTER,
	)
	if err != nil {
		log.Log.Errorf("Syncfilerange error,err=%s,position=%d,length=%d",
			err, position, zeroDataLen)
		return err
	}

	f.writePosition = position
	f.syncPosition = position
	return nil
}

func (f *MmapFile) rename(name string) error {
	dir := path.Dir(f.filePath)
	newPath := path.Join(dir, name)
	err := os.Rename(f.filePath, newPath)
	if err != nil {
		return err
	}
	f.filePath = newPath
	return nil
}
