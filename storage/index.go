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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/flike/kingbus/log"
)

//IndexEntry is index entry in index file
type IndexEntry struct {
	RaftIndex    uint64
	FilePosition uint32
}

//Index is a index of segment file
type Index struct {
	readonly  *MmapFile
	readWrite *RWFile
}

//RWFile is a index file with RW mode
type RWFile struct {
	entries  []*IndexEntry
	writer   *bufio.Writer
	file     *os.File
	filePath string
}

const (
	//IndexEntrySize is the size of IndexEntry
	IndexEntrySize = 12
)

//Marshal encode index entry into byte
func (i *IndexEntry) Marshal() ([]byte, error) {
	b := make([]byte, IndexEntrySize)

	binary.LittleEndian.PutUint64(b[0:8], i.RaftIndex)
	binary.LittleEndian.PutUint32(b[8:12], i.FilePosition)
	return b, nil
}

//Unmarshal decode byte into index entry
func (i *IndexEntry) Unmarshal(data []byte) error {
	i.RaftIndex = binary.LittleEndian.Uint64(data[0:8])
	i.FilePosition = binary.LittleEndian.Uint32(data[8:12])
	return nil
}

func newIndex(dir string, name string) *Index {
	return &Index{
		readWrite: newRWFile(dir, name),
	}
}

func openIndex(dir string, name string, status SegmentStatus) *Index {
	index := new(Index)
	if status == SegmentReadOnly {
		index.readonly = openMmapFile(dir, name)
	} else {
		index.readWrite = openRWFile(dir, name)
	}
	return index
}

func (d *Index) getRaftEntryPosition(i uint64, firstIndex uint64, status SegmentStatus) (int, error) {
	var indexEntry *IndexEntry
	if i < firstIndex {
		return 0, ErrOutOfBound
	}
	position := int(i-firstIndex) * IndexEntrySize
	if status == SegmentReadOnly {
		indexEntry = d.readonly.readIndexEntry(position)
		if indexEntry.RaftIndex != i {
			log.Log.Fatalf("readIndexEntry raftIndex is %d,not %d,position:%d,file:%s",
				indexEntry.RaftIndex, i, position, d.readonly.file.Name())
		}
	} else {
		indexEntry = d.readWrite.entries[int(i-firstIndex)]
		if indexEntry.RaftIndex != i {
			log.Log.Fatalf("readIndexEntry in lastSegmentIndex, raftIndex is %d,not %d,position:%d",
				indexEntry.RaftIndex, i, position)
		}
	}
	return int(indexEntry.FilePosition), nil
}

func (d *Index) appendIndexEntry(entry *IndexEntry) error {
	buf, err := entry.Marshal()
	if err != nil {
		return err
	}
	_, err = d.readWrite.writer.Write(buf)
	if err != nil {
		return err
	}
	d.readWrite.entries = append(d.readWrite.entries, entry)
	return nil
}

func (d *Index) close() error {
	if d.readonly != nil {
		return d.readonly.close()
	}
	return d.readWrite.close()
}

func (d *Index) getLastRaftEntry(status SegmentStatus) *IndexEntry {
	if status != SegmentRDWR {
		log.Log.Fatalf("status want %v, but is %v", SegmentRDWR, status)
	}
	entries := d.readWrite.entries
	return entries[len(entries)-1]
}

func (d *Index) truncateSuffix(i uint64, firstIndex uint64, status SegmentStatus) error {
	var err error
	if i < firstIndex {
		return ErrOutOfBound
	}
	position := int(i-firstIndex) * IndexEntrySize

	if status == SegmentReadOnly {
		dir := path.Dir(d.readonly.filePath)
		//rename index file
		newName := fmt.Sprintf(ReadWriteIndexPattern, firstIndex)
		err = d.readonly.rename(newName)
		if err != nil {
			return err
		}

		err = d.readonly.close()
		if err != nil {
			return err
		}
		d.readonly = nil

		//reopen index file
		d.readWrite = openRWFile(dir, newName)
	} else {
		//flush all the buffered data before, and then truncate
		err = d.readWrite.writer.Flush()
		if err != nil {
			return err
		}
	}

	//truncate file
	err = d.readWrite.file.Truncate(int64(position))
	if err != nil {
		return err
	}
	newPos, err := d.readWrite.file.Seek(int64(position), io.SeekStart)
	if err != nil {
		return err
	}
	log.Log.Debugf("truncateSuffix:file seek to %d", newPos)

	d.readWrite.entries = d.readWrite.entries[:i-firstIndex]

	return nil
}

func (d *Index) changeToReadonly() error {
	entries := d.readWrite.entries
	firstIndex := entries[0].RaftIndex
	lastIndex := entries[len(entries)-1].RaftIndex

	//rename the index file
	dir := path.Dir(d.readWrite.filePath)
	newName := fmt.Sprintf(ReadonlyIndexPattern, firstIndex, lastIndex)
	err := d.readWrite.rename(newName)
	if err != nil {
		return err
	}

	//close the rw index file
	err = d.readWrite.close()
	if err != nil {
		return err
	}
	d.readWrite = nil

	//open with mmap
	d.readonly = openMmapFile(dir, newName)
	return nil
}

func (d *Index) remove() error {
	if d.readonly == nil && d.readWrite != nil {
		return d.readWrite.remove()
	}
	if d.readonly != nil && d.readWrite == nil {
		return d.readonly.remove()
	}
	return nil
}

func newRWFile(dir string, name string) *RWFile {
	var err error

	rw := new(RWFile)
	rw.entries = make([]*IndexEntry, 0, 10000)
	rw.filePath = path.Join(dir, name)
	rw.file, err = os.OpenFile(rw.filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, FileMode)
	if err != nil {
		log.Log.Fatalf("OpenFile error,err:%s,filePath:%s", err, rw.filePath)
	}
	newPos, err := rw.file.Seek(0, io.SeekStart)
	if err != nil {
		log.Log.Fatalf("Seek error,err:%s,filePath:%s", err, rw.filePath)
	}
	log.Log.Debugf("newRWFile:file seek to %d", newPos)

	rw.writer = bufio.NewWriterSize(rw.file, IndexEntrySize*1024*1024)
	return rw
}

func openRWFile(dir string, name string) *RWFile {
	var err error

	rw := new(RWFile)
	rw.entries = make([]*IndexEntry, 0, 10000)
	rw.filePath = path.Join(dir, name)
	rw.file, err = os.OpenFile(rw.filePath, os.O_RDWR, FileMode)
	if err != nil {
		log.Log.Fatalf("OpenFile error,err:%s,filePath:%s", err, rw.filePath)
	}

	var pos int64
	buf := make([]byte, IndexEntrySize)
	for {
		n, err := rw.file.ReadAt(buf, pos)
		if err != nil {
			if err == io.EOF {
				if n == 0 {
					break
				} else {
					log.Log.Fatalf("index file[%s] torn write,pos:%d,buf:%v,n:%d", rw.file.Name(), pos, buf, n)
				}
			}
			log.Log.Fatalf("index file[%s]  ReadAt error,err:%s,pos:%d", rw.file.Name(), err, pos)
		}
		indexEntry := new(IndexEntry)
		err = indexEntry.Unmarshal(buf)
		if err != nil {
			log.Log.Fatalf("Unmarshal buf error,err:%s,buf:%v", err, buf)
		}
		rw.entries = append(rw.entries, indexEntry)
		pos += int64(IndexEntrySize)
	}

	newPos, err := rw.file.Seek(0, io.SeekEnd)
	if err != nil {
		log.Log.Fatalf("Seek error,err:%s,filePath:%s", err, rw.filePath)
	}
	log.Log.Debugf("openRWFile:file seek to %d", newPos)

	rw.writer = bufio.NewWriterSize(rw.file, IndexEntrySize*1024*1024)
	return rw
}

func (rw *RWFile) remove() error {
	err := rw.close()
	if err != nil {
		return err
	}
	err = os.Remove(rw.filePath)
	if err != nil {
		return err
	}
	return nil
}

func (rw *RWFile) rename(name string) error {
	dir := path.Dir(rw.filePath)
	newPath := path.Join(dir, name)
	err := os.Rename(rw.filePath, newPath)
	if err != nil {
		return err
	}
	rw.filePath = newPath
	return nil
}

func (rw *RWFile) close() error {
	rw.entries = nil

	err := rw.writer.Flush()
	if err != nil {
		return err
	}

	err = rw.file.Close()
	if err != nil {
		return err
	}
	return nil
}
