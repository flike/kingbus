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
	"github.com/coreos/etcd/raft/raftpb"
	gomysql "github.com/siddontang/go-mysql/mysql"
)

const (
	//HardStateKey represents hard state in raft
	HardStateKey = "hard_state"
	//ConfStateKey represents conf state in raft
	ConfStateKey = "conf_state"

	//ExecutedGtidSetKey represents executed gtid set key
	ExecutedGtidSetKey = "executed_gtids"
	//GtidPurgedKey represents gtid purged key
	GtidPurgedKey = "gtid_purged"
	//FdePrefix is the key prefix of FORMAT_DESCRIPTION_EVENT
	FdePrefix = "fde"
	//NextBinlogPrefix is the key prefix of nex binlog file name
	NextBinlogPrefix = "next_binlog"
	//MasterInfoKey is master info key
	MasterInfoKey = "master_info"
	//PgePrefix if the key prefix of previous gtid event
	PgePrefix = "pre_gtids"
	//SyncerArgsKey is the key of syncer start args
	SyncerArgsKey = "syncer_args"
	//NeedRecoverKey is the key of storage need recover
	NeedRecoverKey = "ds_need_recover"
	//RaftClusterKey is the key of raft cluster information
	RaftClusterKey = "raft_cluster"
	//AppliedIndexKey is the key of applied index
	AppliedIndexKey = "apply_index"
)

//Storage is kingbus server storage
type Storage interface {
	// entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// entries returns at least one entry if any.
	Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error)
	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.SaveHardState
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first log entry that is
	// possibly available via entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	FirstIndex() (uint64, error)
	// Snapshot returns the most recent snapshot.
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	Snapshot() (raftpb.Snapshot, error)
	SaveRaftEntries(entries []raftpb.Entry) error
	TruncateSuffix(i uint64) error
	StartPurgeLog()
	StopPurgeLog()

	MetaStorage
	NewEntryReaderAt(raftIndex uint64) (EntryReader, error)
	Close() error
}

//MetaStorage is metadata storage interface
type MetaStorage interface {
	InitialState() (raftpb.HardState, raftpb.ConfState, error)
	SaveHardState(st raftpb.HardState) error

	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error

	//key is key+flavor
	SetGtidSet(flavor string, key string, gtidSet gomysql.GTIDSet) error
	GetGtidSet(flavor string, key string) (gomysql.GTIDSet, error)
	SetBinlogProgress(appliedIndex uint64, executedGtidSet gomysql.GTIDSet) error

	GetFde(preGtidEventIndex uint64) ([]byte, error)

	//get the raft index of PreviousGtidSet
	GetPreviousGtidSet(slaveExecutedGtids *gomysql.MysqlGTIDSet) (uint64, error)
	SetPreviousGtidSet(raftIndex uint64, previousGtidSet *gomysql.MysqlGTIDSet) error

	GetNextBinlogFile(startRaftIndex uint64) (string, error)
	UpdatePugedGtidset(firstIndex uint64) error

	Close2() error
}

//EntryReader is a raft entry reader interface
type EntryReader interface {
	GetNext() (*raftpb.Entry, error)
	NextRaftIndex() uint64
}
