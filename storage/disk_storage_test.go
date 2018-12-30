package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

//case1: new dist storage with no file
func TestNewDiskStorage1(t *testing.T) {
	clearAndInitDir()
	store, err := NewDiskStorage(tempDataDir, 1)
	assert.Nil(t, err)
	assert.Equal(t, len(store.Segments), 0)
	store.Close()
}

//case2: new dist storage with with segments
func TestNewDiskStorage2(t *testing.T) {
	clearAndInitDir()
	store, err := NewDiskStorage(tempDataDir, 1)
	assert.Nil(t, err)

	start := uint64(10)
	for len(store.Segments) < 4 {
		entries := newRaftEntriesForTest(start, 100, 10)
		err = store.SaveRaftEntries(entries)
		assert.Nil(t, err)
		start += 10
	}

	store.Close()

	//open store
	store, err = NewDiskStorage(tempDataDir, 1)
	assert.Nil(t, err)

	firstIndex, err := store.FirstIndex()
	assert.Nil(t, err)
	lastIndex, err := store.LastIndex()
	assert.Nil(t, err)

	assert.Equal(t, firstIndex, uint64(10))

	segmentCount := len(store.Segments)
	assert.Equal(t, store.Segments[segmentCount-1].Status, SegmentRDWR)

	entries, err := store.Entries(firstIndex, lastIndex+1, noLimit)
	assert.Nil(t, err)

	for i := 0; i < len(entries); i++ {
		assert.Equal(t, entries[i].Index, firstIndex+uint64(i))
	}
	store.Close()
}

//case3:entries in one segment
func TestDiskStorage_Entries1(t *testing.T) {
	clearAndInitDir()
	store, err := NewDiskStorage(tempDataDir, 1)
	assert.Nil(t, err)

	start := uint64(10)
	for len(store.Segments) < 4 {
		entries := newRaftEntriesForTest(start, 100, 10)
		err = store.SaveRaftEntries(entries)
		assert.Nil(t, err)
		start += 10
	}

	t.Logf("segments length:%d", len(store.Segments))
	entries, err := store.Entries(10, 51, noLimit)
	assert.Nil(t, err)

	count := len(entries)
	assert.Equal(t, count, 41)
	for i := 0; i < count; i++ {
		assert.Equal(t, entries[i].Index, uint64(10+i))
	}

	store.Close()
}

//case4:entries across segments
func TestDiskStorage_Entries2(t *testing.T) {
	clearAndInitDir()
	store, err := NewDiskStorage(tempDataDir, 1)
	assert.Nil(t, err)

	start := uint64(10)
	for len(store.Segments) < 4 {
		entries := newRaftEntriesForTest(start, 100, 10)
		err = store.SaveRaftEntries(entries)
		assert.Nil(t, err)
		start += 10
	}

	t.Logf("segments length:%d", len(store.Segments))

	lastIndex, err := store.LastIndex()
	assert.Nil(t, err)

	entries, err := store.Entries(10, lastIndex+1, noLimit)
	assert.Nil(t, err)

	count := len(entries)
	for i := 0; i < count; i++ {
		assert.Equal(t, entries[i].Index, uint64(10+i))
	}

	store.Close()
}

//case5:one entry large than maxSize
func TestDiskStorage_Entries3(t *testing.T) {
	clearAndInitDir()
	store, err := NewDiskStorage(tempDataDir, 1)
	assert.Nil(t, err)

	entries := newRaftEntriesForTest(10, 500, 10)
	err = store.SaveRaftEntries(entries)
	assert.Nil(t, err)

	t.Logf("segments length:%d", len(store.Segments))
	entries, err = store.Entries(10, 11, 100)
	assert.Nil(t, err)

	count := len(entries)
	assert.Equal(t, count, 1)
	for i := 0; i < count; i++ {
		assert.Equal(t, entries[i].Index, uint64(10+i))
	}

	store.Close()
}

//case6:get term
func TestDiskStorage_Term(t *testing.T) {
	clearAndInitDir()
	store, err := NewDiskStorage(tempDataDir, 4)
	assert.Nil(t, err)

	start := uint64(10)
	for len(store.Segments) < 4 {
		entries := newRaftEntriesForTest(start, 100, 10)
		err = store.SaveRaftEntries(entries)
		assert.Nil(t, err)
		start += 10
	}

	entries, err := store.Entries(10, 11, noLimit)
	assert.Nil(t, err)

	assert.Equal(t, entries[0].Term, uint64(2))
	store.Close()
}

//case10:truncate suffix in one segment
func TestDiskStorage_TruncateSuffix1(t *testing.T) {
	clearAndInitDir()
	store, err := NewDiskStorage(tempDataDir, 4)
	assert.Nil(t, err)

	start := uint64(10)
	for len(store.Segments) < 4 {
		entries := newRaftEntriesForTest(start, 100, 10)
		err = store.SaveRaftEntries(entries)
		assert.Nil(t, err)
		start += 10
	}

	t.Logf("segments length:%d", len(store.Segments))
	count := len(store.Segments)

	last := store.Segments[count-1].getLastIndex()
	first := store.Segments[count-1].getFirstIndex()

	i := first + (last-first)/2
	err = store.TruncateSuffix(i)
	assert.Nil(t, err)

	lastIndex, err := store.LastIndex()
	assert.Nil(t, err)
	assert.Equal(t, lastIndex, uint64(i-1))
}

//case11:truncate suffix across segments
func TestDiskStorage_TruncateSuffix2(t *testing.T) {
	clearAndInitDir()
	store, err := NewDiskStorage(tempDataDir, 4)
	assert.Nil(t, err)

	start := uint64(10)
	for len(store.Segments) < 4 {
		entries := newRaftEntriesForTest(start, 100, 10)
		err = store.SaveRaftEntries(entries)
		assert.Nil(t, err)
		start += 10
	}

	t.Logf("segments length:%d", len(store.Segments))
	count := len(store.Segments)

	last := store.Segments[count-2].getLastIndex()
	first := store.Segments[count-2].getFirstIndex()

	i := first + (last-first)/2
	err = store.TruncateSuffix(i)
	assert.Nil(t, err)

	lastIndex, err := store.LastIndex()
	assert.Nil(t, err)
	assert.Equal(t, lastIndex, uint64(i-1))
}

//case12:test start/stop purge log
func TestDiskStorage_StartPurgeLog(t *testing.T) {

}

//case13:test new entry reader
func TestDiskStorage_NewEntryReaderAt(t *testing.T) {
	clearAndInitDir()
	store, err := NewDiskStorage(tempDataDir, 4)
	assert.Nil(t, err)

	start := uint64(10)
	for len(store.Segments) < 4 {
		entries := newRaftEntriesForTest(start, 100, 10)
		err = store.SaveRaftEntries(entries)
		assert.Nil(t, err)
		start += 10
	}

	t.Logf("segments length:%d", len(store.Segments))

	firstIndex, err := store.FirstIndex()
	assert.Nil(t, err)
	lastIndex, err := store.LastIndex()
	assert.Nil(t, err)

	assert.Equal(t, firstIndex, uint64(10))

	reader, err := store.NewEntryReaderAt(10)
	assert.Nil(t, err)

	for i := 0; i < int(lastIndex-firstIndex+1); i++ {
		entry, err := reader.GetNext()
		assert.Nil(t, err)
		assert.Equal(t, entry.Index, uint64(10+i))
	}
}
