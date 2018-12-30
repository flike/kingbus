package storage

import (
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newIndexEntriesForTest(start uint64, count int) []*IndexEntry {
	entries := make([]*IndexEntry, 0, count)
	for i := 0; i < count; i++ {
		entry := new(IndexEntry)
		entry.RaftIndex = start + uint64(i)
		entry.FilePosition = uint32((int(start) + i) * 100)
		entries = append(entries, entry)
	}
	return entries
}

func TestNewIndex(t *testing.T) {
	clearAndInitDir()
	indexName := fmt.Sprintf(ReadWriteIndexPattern, 1)
	indexFile := newIndex(tempDataDir, indexName)
	defer indexFile.close()

	assert.Nil(t, indexFile.readonly)

	filePath := path.Join(tempDataDir, indexName)
	assert.Equal(t, indexFile.readWrite.filePath, filePath)
}

//func TestOpenIndexWithRead(t *testing.T) {
//
//}
//
//func TestOpenIndexWithWrite(t *testing.T) {
//
//}

func TestGetRaftEntryPosition(t *testing.T) {
	clearAndInitDir()
	indexName := fmt.Sprintf("%020d-inprogress.index", 1)
	indexFile := newIndex(tempDataDir, indexName)
	defer indexFile.close()

	assert.Nil(t, indexFile.readonly)

	entries := newIndexEntriesForTest(100, 10)
	for i := 0; i < len(entries); i++ {
		err := indexFile.appendIndexEntry(entries[i])
		assert.Equal(t, err, nil)
	}
	pos, err := indexFile.getRaftEntryPosition(101, 100, SegmentRDWR)
	assert.Equal(t, err, nil)
	assert.Equal(t, pos, 101*100)
}

func TestAppendIndexEntry(t *testing.T) {
	clearAndInitDir()
	indexName := fmt.Sprintf("%020d-inprogress.index", 1)
	indexFile := newIndex(tempDataDir, indexName)
	defer indexFile.close()

	assert.Nil(t, indexFile.readonly)

	entries := newIndexEntriesForTest(100, 10)
	for i := 0; i < len(entries); i++ {
		err := indexFile.appendIndexEntry(entries[i])
		assert.Equal(t, err, nil)
	}
	entry := indexFile.getLastRaftEntry(SegmentRDWR)
	assert.Equal(t, entry.RaftIndex, uint64(109))
	assert.Equal(t, entry.FilePosition, uint32(109*100))
}

func TestIndexTruncateSuffix(t *testing.T) {
	clearAndInitDir()
	indexName := fmt.Sprintf("%020d-inprogress.index", 1)
	indexFile := newIndex(tempDataDir, indexName)
	defer indexFile.close()

	assert.Nil(t, indexFile.readonly)

	entries := newIndexEntriesForTest(100, 10)
	for i := 0; i < len(entries); i++ {
		err := indexFile.appendIndexEntry(entries[i])
		assert.Equal(t, err, nil)
	}

	//truncate
	err := indexFile.truncateSuffix(105, 100, SegmentRDWR)
	assert.Nil(t, err)

	entry := indexFile.getLastRaftEntry(SegmentRDWR)
	assert.Equal(t, entry.RaftIndex, uint64(104))
	assert.Equal(t, entry.FilePosition, uint32(104*100))
}

func TestIndexChangeToReadonly(t *testing.T) {
	clearAndInitDir()
	indexName := fmt.Sprintf("%020d-inprogress.index", 1)
	indexFile := newIndex(tempDataDir, indexName)
	defer indexFile.close()

	assert.Nil(t, indexFile.readonly)

	entries := newIndexEntriesForTest(100, 10)
	for i := 0; i < len(entries); i++ {
		err := indexFile.appendIndexEntry(entries[i])
		assert.Equal(t, err, nil)
	}

	err := indexFile.changeToReadonly()
	assert.Nil(t, err)

	assert.Nil(t, indexFile.readWrite)

	readonlyName := fmt.Sprintf("%020d-%020d.index", 100, 109)
	filePath := path.Join(tempDataDir, readonlyName)
	assert.Equal(t, indexFile.readonly.filePath, filePath)
}

//func TestOpenRWFile(t *testing.T) {
//
//}
