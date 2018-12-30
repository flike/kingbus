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

package membership

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/flike/kingbus/log"

	"os"

	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/flike/kingbus/storage"
)

const (
	TestDataDir = "/tmp/kingbus/kingbus_data"
)

var tempLogDir = "/tmp/kingbus/log"

func init() {
	log.InitLoggers(tempLogDir, "DEBUG")
}

func TestClusterMember(t *testing.T) {
	membs := []*Member{
		newTestMember(1, nil, "node1"),
		newTestMember(2, nil, "node2"),
	}
	tests := []struct {
		id    types.ID
		match bool
	}{
		{1, true},
		{2, true},
		{3, false},
	}
	for i, tt := range tests {
		c := newTestCluster(membs)
		m := c.Member(tt.id)
		if g := m != nil; g != tt.match {
			t.Errorf("#%d: find member = %v, want %v", i, g, tt.match)
		}
		if m != nil && m.ID != tt.id {
			t.Errorf("#%d: id = %x, want %x", i, m.ID, tt.id)
		}
	}
}

func TestClusterMemberByName(t *testing.T) {
	membs := []*Member{
		newTestMember(1, nil, "node1"),
		newTestMember(2, nil, "node2"),
	}
	tests := []struct {
		name  string
		match bool
	}{
		{"node1", true},
		{"node2", true},
		{"node3", false},
	}
	for i, tt := range tests {
		c := newTestCluster(membs)
		m := c.MemberByName(tt.name)
		if g := m != nil; g != tt.match {
			t.Errorf("#%d: find member = %v, want %v", i, g, tt.match)
		}
		if m != nil && m.Name != tt.name {
			t.Errorf("#%d: name = %v, want %v", i, m.Name, tt.name)
		}
	}
}

func TestClusterMemberIDs(t *testing.T) {
	c := newTestCluster([]*Member{
		newTestMember(1, nil, ""),
		newTestMember(4, nil, ""),
		newTestMember(100, nil, ""),
	})
	w := []types.ID{1, 4, 100}
	g := c.MemberIDs()
	if !reflect.DeepEqual(w, g) {
		t.Errorf("IDs = %+v, want %+v", g, w)
	}
}

func TestClusterPeerURLs(t *testing.T) {
	tests := []struct {
		mems  []*Member
		wurls []string
	}{
		// single peer with a single address
		{
			mems: []*Member{
				newTestMember(1, []string{"http://192.0.2.1"}, ""),
			},
			wurls: []string{"http://192.0.2.1"},
		},

		// single peer with a single address with a port
		{
			mems: []*Member{
				newTestMember(1, []string{"http://192.0.2.1:8001"}, ""),
			},
			wurls: []string{"http://192.0.2.1:8001"},
		},

		// several members explicitly unsorted
		{
			mems: []*Member{
				newTestMember(2, []string{"http://192.0.2.3", "http://192.0.2.4"}, ""),
				newTestMember(3, []string{"http://192.0.2.5", "http://192.0.2.6"}, ""),
				newTestMember(1, []string{"http://192.0.2.1", "http://192.0.2.2"}, ""),
			},
			wurls: []string{"http://192.0.2.1", "http://192.0.2.2", "http://192.0.2.3", "http://192.0.2.4", "http://192.0.2.5", "http://192.0.2.6"},
		},

		// no members
		{
			mems:  []*Member{},
			wurls: []string{},
		},

		// peer with no peer urls
		{
			mems: []*Member{
				newTestMember(3, []string{}, ""),
			},
			wurls: []string{},
		},
	}

	for i, tt := range tests {
		c := newTestCluster(tt.mems)
		urls := c.PeerURLs()
		if !reflect.DeepEqual(urls, tt.wurls) {
			t.Errorf("#%d: PeerURLs = %v, want %v", i, urls, tt.wurls)
		}
	}
}

func TestClusterValidateAndAssignIDsBad(t *testing.T) {
	tests := []struct {
		clmembs []*Member
		membs   []*Member
	}{
		{
			// unmatched length
			[]*Member{
				newTestMember(1, []string{"http://127.0.0.1:2379"}, ""),
			},
			[]*Member{},
		},
		{
			// unmatched peer urls
			[]*Member{
				newTestMember(1, []string{"http://127.0.0.1:2379"}, ""),
			},
			[]*Member{
				newTestMember(1, []string{"http://127.0.0.1:4001"}, ""),
			},
		},
		{
			// unmatched peer urls
			[]*Member{
				newTestMember(1, []string{"http://127.0.0.1:2379"}, ""),
				newTestMember(2, []string{"http://127.0.0.2:2379"}, ""),
			},
			[]*Member{
				newTestMember(1, []string{"http://127.0.0.1:2379"}, ""),
				newTestMember(2, []string{"http://127.0.0.2:4001"}, ""),
			},
		},
	}
	for i, tt := range tests {
		ecl := newTestCluster(tt.clmembs)
		lcl := newTestCluster(tt.membs)
		if err := ValidateClusterAndAssignIDs(lcl, ecl); err == nil {
			t.Errorf("#%d: unexpected update success", i)
		}
	}
}

func TestClusterValidateAndAssignIDs(t *testing.T) {
	tests := []struct {
		clmembs []*Member
		membs   []*Member
		wids    []types.ID
	}{
		{
			[]*Member{
				newTestMember(1, []string{"http://127.0.0.1:2379"}, ""),
				newTestMember(2, []string{"http://127.0.0.2:2379"}, ""),
			},
			[]*Member{
				newTestMember(3, []string{"http://127.0.0.1:2379"}, ""),
				newTestMember(4, []string{"http://127.0.0.2:2379"}, ""),
			},
			[]types.ID{3, 4},
		},
	}
	for i, tt := range tests {
		lcl := newTestCluster(tt.clmembs)
		ecl := newTestCluster(tt.membs)
		if err := ValidateClusterAndAssignIDs(lcl, ecl); err != nil {
			t.Errorf("#%d: unexpect update error: %v", i, err)
		}
		if !reflect.DeepEqual(lcl.MemberIDs(), tt.wids) {
			t.Errorf("#%d: ids = %v, want %v", i, lcl.MemberIDs(), tt.wids)
		}
	}
}

func TestClusterValidateConfigurationChange(t *testing.T) {
	initEnv()
	memoryStorage, err := storage.NewMemoryStorage(TestDataDir)
	if err != nil {
		t.Fatal(err)
	}
	cl := NewCluster()
	cl.SetStore(memoryStorage)
	for i := 1; i <= 4; i++ {
		attr := RaftAttributes{PeerURLs: []string{fmt.Sprintf("http://127.0.0.1:%d", i)}}
		cl.AddMember(&Member{ID: types.ID(i), RaftAttributes: attr})
	}
	cl.RemoveMember(4)

	attr := RaftAttributes{PeerURLs: []string{fmt.Sprintf("http://127.0.0.1:%d", 1)}}
	ctx, err := json.Marshal(&Member{ID: types.ID(5), RaftAttributes: attr})
	if err != nil {
		t.Fatal(err)
	}

	attr = RaftAttributes{PeerURLs: []string{fmt.Sprintf("http://127.0.0.1:%d", 5)}}
	ctx5, err := json.Marshal(&Member{ID: types.ID(5), RaftAttributes: attr})
	if err != nil {
		t.Fatal(err)
	}

	attr = RaftAttributes{PeerURLs: []string{fmt.Sprintf("http://127.0.0.1:%d", 3)}}
	ctx2to3, err := json.Marshal(&Member{ID: types.ID(2), RaftAttributes: attr})
	if err != nil {
		t.Fatal(err)
	}

	attr = RaftAttributes{PeerURLs: []string{fmt.Sprintf("http://127.0.0.1:%d", 5)}}
	ctx2to5, err := json.Marshal(&Member{ID: types.ID(2), RaftAttributes: attr})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		cc   raftpb.ConfChange
		werr error
	}{
		{
			raftpb.ConfChange{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: 3,
			},
			nil,
		},
		{
			raftpb.ConfChange{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: 4,
			},
			ErrIDRemoved,
		},
		{
			raftpb.ConfChange{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: 4,
			},
			ErrIDRemoved,
		},
		{
			raftpb.ConfChange{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: 1,
			},
			ErrIDExists,
		},
		{
			raftpb.ConfChange{
				Type:    raftpb.ConfChangeAddNode,
				NodeID:  5,
				Context: ctx,
			},
			ErrPeerURLexists,
		},
		{
			raftpb.ConfChange{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: 5,
			},
			ErrIDNotFound,
		},
		{
			raftpb.ConfChange{
				Type:    raftpb.ConfChangeAddNode,
				NodeID:  5,
				Context: ctx5,
			},
			nil,
		},
		{
			raftpb.ConfChange{
				Type:    raftpb.ConfChangeUpdateNode,
				NodeID:  5,
				Context: ctx,
			},
			ErrIDNotFound,
		},
		// try to change the peer url of 2 to the peer url of 3
		{
			raftpb.ConfChange{
				Type:    raftpb.ConfChangeUpdateNode,
				NodeID:  2,
				Context: ctx2to3,
			},
			ErrPeerURLexists,
		},
		{
			raftpb.ConfChange{
				Type:    raftpb.ConfChangeUpdateNode,
				NodeID:  2,
				Context: ctx2to5,
			},
			nil,
		},
	}
	for i, tt := range tests {
		err := cl.ValidateConfigurationChange(tt.cc)
		if err != tt.werr {
			t.Errorf("#%d: validateConfigurationChange error = %v, want %v", i, err, tt.werr)
		}
	}
}

func TestClusterGenID(t *testing.T) {
	cs := newTestCluster([]*Member{
		newTestMember(1, nil, ""),
		newTestMember(2, nil, ""),
	})

	cs.genID()
	if cs.GetID() == 0 {
		t.Fatalf("cluster.ID = %v, want not 0", cs.GetID())
	}
	previd := cs.GetID()

	initEnv()
	memoryStorage, err := storage.NewMemoryStorage(TestDataDir)
	if err != nil {
		t.Fatal(err)
	}
	cs.SetStore(memoryStorage)
	cs.AddMember(newTestMember(3, nil, ""))
	cs.genID()
	if cs.GetID() == previd {
		t.Fatalf("cluster.ID = %v, want not %v", cs.GetID(), previd)
	}
}

func TestNodeToMemberBad(t *testing.T) {

}

func TestClusterAddMember(t *testing.T) {

}

func TestClusterMembers(t *testing.T) {
	cls := &RaftCluster{
		MembersMap: map[types.ID]*Member{
			1:   {ID: 1},
			20:  {ID: 20},
			100: {ID: 100},
			5:   {ID: 5},
			50:  {ID: 50},
		},
	}
	w := []*Member{
		{ID: 1},
		{ID: 5},
		{ID: 20},
		{ID: 50},
		{ID: 100},
	}
	if g := cls.Members(); !reflect.DeepEqual(g, w) {
		t.Fatalf("Members()=%#v, want %#v", g, w)
	}
}

func TestClusterRemoveMember(t *testing.T) {

}

func TestClusterUpdateAttributes(t *testing.T) {
	name := "etcd"
	tests := []struct {
		mems    []*Member
		removed map[types.ID]bool
		wmems   []*Member
	}{
		// update attributes of existing member
		{
			[]*Member{
				newTestMember(1, nil, ""),
			},
			nil,
			[]*Member{
				newTestMember(1, nil, name),
			},
		},
		// update attributes of removed member
		{
			nil,
			map[types.ID]bool{types.ID(1): true},
			nil,
		},
	}
	for i, tt := range tests {
		c := newTestCluster(tt.mems)
		c.RemovedMap = tt.removed
		initEnv()
		memoryStorage, err := storage.NewMemoryStorage(TestDataDir)
		if err != nil {
			t.Fatal(err)
		}
		c.SetStore(memoryStorage)
		c.UpdateAttributes(types.ID(1), Attributes{Name: name})
		if g := c.Members(); !reflect.DeepEqual(g, tt.wmems) {
			t.Errorf("#%d: members = %+v, want %+v", i, g, tt.wmems)
		}
	}
}

func TestNodeToMember(t *testing.T) {

}

func newTestCluster(membs []*Member) *RaftCluster {
	c := &RaftCluster{MembersMap: make(map[types.ID]*Member), RemovedMap: make(map[types.ID]bool)}
	for _, m := range membs {
		c.MembersMap[m.ID] = m
	}

	return c
}

func TestIsReadyToAddNewMember(t *testing.T) {
	tests := []struct {
		members []*Member
		want    bool
	}{
		{
			// 0/3 members ready, should fail
			[]*Member{
				newTestMember(1, nil, ""),
				newTestMember(2, nil, ""),
				newTestMember(3, nil, ""),
			},
			false,
		},
		{
			// 1/2 members ready, should fail
			[]*Member{
				newTestMember(1, nil, "1"),
				newTestMember(2, nil, ""),
			},
			false,
		},
		{
			// 1/3 members ready, should fail
			[]*Member{
				newTestMember(1, nil, "1"),
				newTestMember(2, nil, ""),
				newTestMember(3, nil, ""),
			},
			false,
		},
		{
			// 1/1 members ready, should succeed (special case of 1-member cluster for recovery)
			[]*Member{
				newTestMember(1, nil, "1"),
			},
			true,
		},
		{
			// 2/3 members ready, should fail
			[]*Member{
				newTestMember(1, nil, "1"),
				newTestMember(2, nil, "2"),
				newTestMember(3, nil, ""),
			},
			false,
		},
		{
			// 3/3 members ready, should be fine to add one member and retain quorum
			[]*Member{
				newTestMember(1, nil, "1"),
				newTestMember(2, nil, "2"),
				newTestMember(3, nil, "3"),
			},
			true,
		},
		{
			// 3/4 members ready, should be fine to add one member and retain quorum
			[]*Member{
				newTestMember(1, nil, "1"),
				newTestMember(2, nil, "2"),
				newTestMember(3, nil, "3"),
				newTestMember(4, nil, ""),
			},
			true,
		},
		{
			// empty cluster, it is impossible but should fail
			[]*Member{},
			false,
		},
	}
	for i, tt := range tests {
		c := newTestCluster(tt.members)
		if got := c.IsReadyToAddNewMember(); got != tt.want {
			t.Errorf("%d: isReadyToAddNewMember returned %t, want %t", i, got, tt.want)
		}
	}
}

func TestIsReadyToRemoveMember(t *testing.T) {
	tests := []struct {
		members  []*Member
		removeID uint64
		want     bool
	}{
		{
			// 1/1 members ready, should fail
			[]*Member{
				newTestMember(1, nil, "1"),
			},
			1,
			false,
		},
		{
			// 0/3 members ready, should fail
			[]*Member{
				newTestMember(1, nil, ""),
				newTestMember(2, nil, ""),
				newTestMember(3, nil, ""),
			},
			1,
			false,
		},
		{
			// 1/2 members ready, should be fine to remove unstarted member
			// (isReadyToRemoveMember() logic should return success, but operation itself would fail)
			[]*Member{
				newTestMember(1, nil, "1"),
				newTestMember(2, nil, ""),
			},
			2,
			true,
		},
		{
			// 2/3 members ready, should fail
			[]*Member{
				newTestMember(1, nil, "1"),
				newTestMember(2, nil, "2"),
				newTestMember(3, nil, ""),
			},
			2,
			false,
		},
		{
			// 3/3 members ready, should be fine to remove one member and retain quorum
			[]*Member{
				newTestMember(1, nil, "1"),
				newTestMember(2, nil, "2"),
				newTestMember(3, nil, "3"),
			},
			3,
			true,
		},
		{
			// 3/4 members ready, should be fine to remove one member
			[]*Member{
				newTestMember(1, nil, "1"),
				newTestMember(2, nil, "2"),
				newTestMember(3, nil, "3"),
				newTestMember(4, nil, ""),
			},
			3,
			true,
		},
		{
			// 3/4 members ready, should be fine to remove unstarted member
			[]*Member{
				newTestMember(1, nil, "1"),
				newTestMember(2, nil, "2"),
				newTestMember(3, nil, "3"),
				newTestMember(4, nil, ""),
			},
			4,
			true,
		},
	}
	for i, tt := range tests {
		c := newTestCluster(tt.members)
		if got := c.IsReadyToRemoveMember(tt.removeID); got != tt.want {
			t.Errorf("%d: isReadyToAddNewMember returned %t, want %t", i, got, tt.want)
		}
	}
}

func initEnv() {
	err := os.RemoveAll(TestDataDir)
	if err != nil {
		panic(err)
	}
}
