package storage

import (
	"fmt"
	"github.com/flike/kingbus/log"
	"hash/crc32"
	"math/rand"
	"os"
	"path"
	"testing"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/flike/kingbus/storage/storagepb"
	"github.com/flike/kingbus/utils"
	"github.com/stretchr/testify/assert"
)

var tempDataDir = "/tmp/kingbus/data"
var tempLogDir = "/tmp/kingbus/log"

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func init() {
	//reset segment maxsize
	SegmentSize = 10 * 1024
	log.InitLoggers(tempLogDir, "DEBUG")
}

func clearAndInitDir() {
	if utils.DirExist(tempDataDir) {
		err := os.RemoveAll(tempDataDir)
		if err != nil {
			panic(err)
		}
	}
	err := os.MkdirAll(tempDataDir, 0700)
	if err != nil {
		panic("err")
	}

}

func RandStringBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

func newRecordsForTest(start uint64, dataSize int, count int) []*storagepb.Record {
	var err error

	records := make([]*storagepb.Record, 0, count)
	for i := 0; i < count; i++ {
		var raftEntry raftpb.Entry
		record := new(storagepb.Record)

		raftEntry.Index = start + uint64(i)
		raftEntry.Term = 2
		raftEntry.Type = raftpb.EntryNormal
		raftEntry.Data = RandStringBytes(dataSize)

		record.Data, err = raftEntry.Marshal()
		if err != nil {
			panic(fmt.Sprintf("raftEntry Marshal error:%s", err))
		}
		record.Crc = crc32.ChecksumIEEE(record.Data)
		records = append(records, record)
	}
	return records
}

func newRaftEntriesForTest(start uint64, dataSize int, count int) []raftpb.Entry {
	entries := make([]raftpb.Entry, 0, count)
	for i := 0; i < count; i++ {
		var raftEntry raftpb.Entry

		raftEntry.Index = start + uint64(i)
		raftEntry.Term = 2
		raftEntry.Type = raftpb.EntryNormal
		raftEntry.Data = RandStringBytes(dataSize)

		entries = append(entries, raftEntry)
	}
	return entries
}

func TestNewSegment(t *testing.T) {
	clearAndInitDir()
	newSegmentName := fmt.Sprintf(ReadWriteSegmentPattern, 1)
	segment := newSegment(tempDataDir, newSegmentName)
	defer segment.close()

	assert.Equal(t, segment.FirstIndex, uint64(0))

	filePath := path.Join(tempDataDir, newSegmentName)
	assert.Equal(t, segment.LogFile.filePath, filePath)

	assert.Equal(t, segment.Status, SegmentRDWR)
}

//func TestOpenSegments(t *testing.T) {
//
//}
//
//func TestOpenSegmentsWithRecover(t *testing.T) {
//
//}

func TestRebuildIndex(t *testing.T) {

}

func TestAppendRecords(t *testing.T) {
	clearAndInitDir()
	newSegmentName := fmt.Sprintf(ReadWriteSegmentPattern, 10)
	segment := newSegment(tempDataDir, newSegmentName)
	defer segment.close()

	records := newRecordsForTest(10, 100, 10)
	err := segment.appendRecords(records, 10)
	assert.Nil(t, err)

	assert.Equal(t, segment.FirstIndex, uint64(10))
	assert.Equal(t, segment.LastIndex, uint64(19))
}

func TestReadRaftEntries(t *testing.T) {
	clearAndInitDir()
	newSegmentName := fmt.Sprintf(ReadWriteSegmentPattern, 10)
	segment := newSegment(tempDataDir, newSegmentName)
	defer segment.close()

	records := newRecordsForTest(10, 100, 10)
	err := segment.appendRecords(records, 10)
	assert.Nil(t, err)

	entires, _, err := segment.readRaftEntries(10, 19, noLimit)
	assert.Nil(t, err)
	assert.Equal(t, len(entires), 10)
}

func TestTruncateSuffix(t *testing.T) {
	clearAndInitDir()
	newSegmentName := fmt.Sprintf(ReadWriteSegmentPattern, 10)
	segment := newSegment(tempDataDir, newSegmentName)
	defer segment.close()

	//10-29
	records := newRecordsForTest(10, 100, 20)
	err := segment.appendRecords(records, 10)
	assert.Nil(t, err)

	err = segment.truncateSuffix(15)
	assert.Nil(t, err)

	assert.Equal(t, segment.FirstIndex, uint64(10))
	assert.Equal(t, segment.LastIndex, uint64(14))
}

func TestChangeToReadonly(t *testing.T) {
	clearAndInitDir()
	newSegmentName := fmt.Sprintf(ReadWriteSegmentPattern, 10)
	segment := newSegment(tempDataDir, newSegmentName)
	defer segment.close()

	//10-19
	records := newRecordsForTest(10, 100, 10)
	err := segment.appendRecords(records, 10)
	assert.Nil(t, err)

	err = segment.changeToReadonly()
	assert.Nil(t, err)

	assert.Equal(t, segment.FirstIndex, uint64(10))
	assert.Equal(t, segment.LastIndex, uint64(19))
	assert.Equal(t, segment.Status, SegmentReadOnly)
}

func TestRemove(t *testing.T) {
	clearAndInitDir()
	newSegmentName := fmt.Sprintf(ReadWriteSegmentPattern, 10)
	segment := newSegment(tempDataDir, newSegmentName)

	//10-19
	records := newRecordsForTest(10, 100, 10)
	err := segment.appendRecords(records, 10)
	assert.Nil(t, err)

	err = segment.remove()
	assert.Nil(t, err)
}
