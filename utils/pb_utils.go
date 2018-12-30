// Copyright 2016 The etcd Authors. All rights reserved.
// Use of this source code is governed by a Apache License(Version 2.0)
// that can be found in the LICENSES/etcd-LICENSE file.

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

package utils

import (
	"fmt"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/flike/kingbus/storage/storagepb"
)

const (
	//MySQLBinlogEventType is mysql binlog event flag
	MySQLBinlogEventType byte = iota + 1
	//NodeAttributesType for update the node admin url
	NodeAttributesType
	//SyncerArgsType is syncer start args flag
	SyncerArgsType
	//MasterInfoType is master info flag
	MasterInfoType
	//MasterGtidPurgedType is master gtid_purged flag
	MasterGtidPurgedType
)

//Marshaler implements stuct to byte
type Marshaler interface {
	Marshal() (data []byte, err error)
}

//Unmarshaler implements byte to struct
type Unmarshaler interface {
	Unmarshal(data []byte) error
}

//MustMarshal execute occur error will panic
func MustMarshal(m Marshaler) []byte {
	d, err := m.Marshal()
	if err != nil {
		panic(fmt.Sprintf("marshal should never fail (%v)", err))
	}
	return d
}

//MustUnmarshal execute occur error will panic
func MustUnmarshal(um Unmarshaler, data []byte) {
	if err := um.Unmarshal(data); err != nil {
		panic(fmt.Sprintf("unmarshal should never fail (%v)", err))
	}
}

//EncodeBinlogEvent encode binlog event to data
func EncodeBinlogEvent(e *storagepb.BinlogEvent) ([]byte, error) {
	data, err := e.Marshal()
	if err != nil {
		return nil, err
	}
	dataWithType := make([]byte, 1, len(data)+1)
	dataWithType[0] = MySQLBinlogEventType
	dataWithType = append(dataWithType, data...)
	return dataWithType, nil
}

//IsBinlogEvent check the raft entry is binlog event
func IsBinlogEvent(e *raftpb.Entry) bool {
	if e.Type == raftpb.EntryNormal {
		if len(e.Data) != 0 && e.Data[0] == MySQLBinlogEventType {
			return true
		}
	}
	return false
}

//DecodeBinlogEvent decode raft entry to binlog event
func DecodeBinlogEvent(e *raftpb.Entry) *storagepb.BinlogEvent {
	binlogEvent := new(storagepb.BinlogEvent)
	//ignore the type:data[0]
	MustUnmarshal(binlogEvent, e.Data[1:])
	return binlogEvent
}
