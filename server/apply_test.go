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

package server

import (
	"testing"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/flike/kingbus/raft"
)

func TestApplyEmptyEntry(t *testing.T) {
	initEnv()
	cluster := StartClusterInTest()
	defer StopClusterInTest(cluster)

	//empty entries
	notifyCh := make(chan struct{}, 1)
	applyEmpty := &raft.ApplyEntry{
		Entries: make([]raftpb.Entry, 0),
		Notifyc: notifyCh,
	}
	cluster[0].applyAll(applyEmpty)
	select {
	case <-notifyCh:
	default:
		t.Fatal("should not apply empty entry")
	}
}
