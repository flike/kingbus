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
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"errors"

	"github.com/coreos/etcd/pkg/netutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/storage"
	"github.com/flike/kingbus/utils"
)

// RaftCluster is a list of MembersMap that belong to the same raft cluster
type RaftCluster struct {
	ID    types.ID        `json:"id"`
	Store storage.Storage `json:"-"`

	sync.Mutex `json:"-"`
	MembersMap map[types.ID]*Member `json:"members_map"`
	// RemovedMap contains the ids of RemovedMap MembersMap in the cluster.
	// RemovedMap id cannot be reused.
	RemovedMap map[types.ID]bool `json:"removed_map"`
}

//GetRaftClusterFromStorage get the raft cluster information from storage
func GetRaftClusterFromStorage(s storage.Storage) (*RaftCluster, error) {
	var r RaftCluster
	value, err := s.Get(utils.StringToBytes(storage.RaftClusterKey))
	if err != nil {
		log.Log.Panicf("get storeMembers should never fail: %s", err)
	}

	//key is not exist
	if value == nil {
		return nil, storage.ErrKeyNotFound
	}

	err = json.Unmarshal(value, &r)
	if err != nil {
		log.Log.Panicf("json unmarshal should never fail,err:%s,value:%v", err, value)
	}
	return &r, nil
}

//SaveToStorage implements save the raft cluster information into storage
func (c *RaftCluster) SaveToStorage() error {
	value, err := json.Marshal(c)
	if err != nil {
		return err
	}
	key := utils.StringToBytes(storage.RaftClusterKey)
	err = c.Store.Set(key, value)
	if err != nil {
		log.Log.Panic("SaveToStorage error,err:%s,c:%v", err.Error(), *c)
	}
	return nil
}

//NewClusterFromURLsMap implements init raft cluster from config
func NewClusterFromURLsMap(urlsmap types.URLsMap) (*RaftCluster, error) {
	c := NewCluster()
	for name, urls := range urlsmap {
		m := NewMember(name, urls, nil, nil)
		if _, ok := c.MembersMap[m.ID]; ok {
			return nil, fmt.Errorf("member exists with identical id %v", m)
		}
		if uint64(m.ID) == raft.None {
			return nil, fmt.Errorf("cannot use %x as member id", raft.None)
		}
		c.MembersMap[m.ID] = m
	}
	c.genID()
	return c, nil
}

//NewClusterFromMembers implements init member map
func NewClusterFromMembers(id types.ID, membs []*Member) *RaftCluster {
	c := NewCluster()
	c.ID = id
	for _, m := range membs {
		c.MembersMap[m.ID] = m
	}
	return c
}

//NewCluster create a new cluster struct
func NewCluster() *RaftCluster {
	return &RaftCluster{
		MembersMap: make(map[types.ID]*Member),
		RemovedMap: make(map[types.ID]bool),
	}
}

//GetID get the id of raft cluster
func (c *RaftCluster) GetID() types.ID { return c.ID }

//Members get the members of raft cluster
func (c *RaftCluster) Members() []*Member {
	c.Lock()
	defer c.Unlock()
	var ms MembersByID
	for _, m := range c.MembersMap {
		ms = append(ms, m.Clone())
	}
	sort.Sort(ms)
	return []*Member(ms)
}

//Member get the member by id
func (c *RaftCluster) Member(id types.ID) *Member {
	c.Lock()
	defer c.Unlock()
	return c.MembersMap[id].Clone()
}

// MemberByName returns a Member with the given name if exists.
// If more than one member has the given name, it will panic.
func (c *RaftCluster) MemberByName(name string) *Member {
	c.Lock()
	defer c.Unlock()
	var memb *Member
	for _, m := range c.MembersMap {
		if m.Name == name {
			if memb != nil {
				log.Log.Panicf("two MembersMap with the given name %q exist", name)
			}
			memb = m
		}
	}
	return memb.Clone()
}

//MemberIDs get the IDs of members
func (c *RaftCluster) MemberIDs() []types.ID {
	c.Lock()
	defer c.Unlock()
	var ids []types.ID
	for _, m := range c.MembersMap {
		ids = append(ids, m.ID)
	}
	sort.Sort(types.IDSlice(ids))
	return ids
}

//IsIDRemoved check the id of member is removed
func (c *RaftCluster) IsIDRemoved(id types.ID) bool {
	c.Lock()
	defer c.Unlock()
	return c.RemovedMap[id]
}

// PeerURLs returns a list of all peer addresses.
// The returned list is sorted in ascending lexicographical order.
func (c *RaftCluster) PeerURLs() []string {
	c.Lock()
	defer c.Unlock()
	urls := make([]string, 0)
	for _, p := range c.MembersMap {
		urls = append(urls, p.PeerURLs...)
	}
	sort.Strings(urls)
	return urls
}

//String implements dump raft cluster into string
func (c *RaftCluster) String() string {
	c.Lock()
	defer c.Unlock()
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "{ClusterID:%s ", c.ID)
	var ms []string
	for _, m := range c.MembersMap {
		ms = append(ms, fmt.Sprintf("%+v", m))
	}
	fmt.Fprintf(b, "MembersMap:[%s] ", strings.Join(ms, " "))
	var ids []string
	for id := range c.RemovedMap {
		ids = append(ids, id.String())
	}
	fmt.Fprintf(b, "RemovedMemberIDs:[%s]}", strings.Join(ids, " "))
	return b.String()
}

func (c *RaftCluster) genID() {
	mIDs := c.MemberIDs()
	b := make([]byte, 8*len(mIDs))
	for i, id := range mIDs {
		binary.BigEndian.PutUint64(b[8*i:], uint64(id))
	}
	hash := sha1.Sum(b)
	c.ID = types.ID(binary.BigEndian.Uint64(hash[:8]))
}

//SetID set the id of raft cluster
func (c *RaftCluster) SetID(id types.ID) { c.ID = id }

//SetStore set the storage of raft cluster
func (c *RaftCluster) SetStore(st storage.Storage) { c.Store = st }

// ValidateConfigurationChange takes a proposed ConfChange and
// ensures that it is still valid.
func (c *RaftCluster) ValidateConfigurationChange(cc raftpb.ConfChange) error {
	members, removed := membersFromStore(c.Store)
	id := types.ID(cc.NodeID)
	if removed[id] {
		return ErrIDRemoved
	}
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		if members[id] != nil {
			return ErrIDExists
		}
		urls := make(map[string]bool)
		for _, m := range members {
			for _, u := range m.PeerURLs {
				urls[u] = true
			}
		}
		m := new(Member)
		if err := json.Unmarshal(cc.Context, m); err != nil {
			log.Log.Panicf("unmarshal member should never fail: %v", err)
		}
		for _, u := range m.PeerURLs {
			if urls[u] {
				return ErrPeerURLexists
			}
		}
	case raftpb.ConfChangeRemoveNode:
		if members[id] == nil {
			return ErrIDNotFound
		}
	case raftpb.ConfChangeUpdateNode:
		if members[id] == nil {
			return ErrIDNotFound
		}
		urls := make(map[string]bool)
		for _, m := range members {
			if m.ID == id {
				continue
			}
			for _, u := range m.PeerURLs {
				urls[u] = true
			}
		}
		m := new(Member)
		if err := json.Unmarshal(cc.Context, m); err != nil {
			log.Log.Panicf("unmarshal member should never fail: %v", err)
		}
		for _, u := range m.PeerURLs {
			if urls[u] {
				return ErrPeerURLexists
			}
		}
	default:
		log.Log.Panicf("ConfChange type should be either AddNode, RemoveNode or UpdateNode")
	}
	return nil
}

// AddMember adds a new Member into the cluster, and saves the given member's
// raftAttributes into the Store. The given member should have empty attributes.
// A Member with a matching id must not exist.
func (c *RaftCluster) AddMember(m *Member) {
	c.Lock()
	defer c.Unlock()

	c.MembersMap[m.ID] = m
	err := c.SaveToStorage()
	if err != nil {
		log.Log.Fatalf("SaveToStorage error,err:%s,c:%v", err.Error(), *c)
	}
	log.Log.Debugf("add member %s %v to cluster %s", m.ID, m.PeerURLs, c.ID)
}

// RemoveMember removes a member from the Store.
// The given id MUST exist, or the function panics.
func (c *RaftCluster) RemoveMember(id types.ID) {
	c.Lock()
	defer c.Unlock()

	delete(c.MembersMap, id)
	c.RemovedMap[id] = true
	err := c.SaveToStorage()
	if err != nil {
		log.Log.Fatalf("SaveToStorage error,err:%s,c:%v", err.Error(), *c)
	}
	log.Log.Debugf("remove member %s from cluster %s", id.String(), c.ID.String())
}

//UpdateAttributes update the attributes of raft cluster
func (c *RaftCluster) UpdateAttributes(id types.ID, attr Attributes) {
	c.Lock()
	defer c.Unlock()
	if m, ok := c.MembersMap[id]; ok {
		m.Attributes = attr
		err := c.SaveToStorage()
		if err != nil {
			log.Log.Fatalf("SaveToStorage error,err:%s,c:%v", err.Error(), *c)
		}
		log.Log.Debugf("UpdateAttributes: update %s attributes:%v", id.String(), attr)
		return
	}
	_, ok := c.RemovedMap[id]
	if !ok {
		log.Log.Panicf("error updating attributes of unknown member %s", id)
	}
	log.Log.Warnf("skipped updating attributes of RemovedMap member %s", id)
}

//UpdateRaftAttributes update the raft attributes of raft cluster
func (c *RaftCluster) UpdateRaftAttributes(id types.ID, raftAttr RaftAttributes) {
	c.Lock()
	defer c.Unlock()

	c.MembersMap[id].RaftAttributes = raftAttr
	err := c.SaveToStorage()
	if err != nil {
		log.Log.Fatalf("SaveToStorage error,err:%s,c:%v", err.Error(), *c)
	}

	log.Log.Debugf("updated member %s %v in cluster %s", id, raftAttr.PeerURLs, c.ID)
}

//IsReadyToAddNewMember check the raft cluster is ready to add member
func (c *RaftCluster) IsReadyToAddNewMember() bool {
	nmembers := 1
	nstarted := 0

	for _, member := range c.MembersMap {
		if member.IsStarted() {
			nstarted++
		}
		nmembers++
	}

	if nstarted == 1 && nmembers == 2 {
		// a case of adding a new node to 1-member cluster for restoring cluster data
		// https://github.com/coreos/etcd/blob/master/Documentation/v2/admin_guide.md#restoring-the-cluster

		log.Log.Debugf("The number of started member is 1. This cluster can accept add member request.")
		return true
	}

	nquorum := nmembers/2 + 1
	if nstarted < nquorum {
		log.Log.Warnf("Reject add member request: the number of started member (%d) will be less than the quorum number of the cluster (%d)", nstarted, nquorum)
		return false
	}

	return true
}

//IsReadyToRemoveMember check the raft cluster is ready to remove member
func (c *RaftCluster) IsReadyToRemoveMember(id uint64) bool {
	nmembers := 0
	nstarted := 0

	for _, member := range c.MembersMap {
		if uint64(member.ID) == id {
			continue
		}

		if member.IsStarted() {
			nstarted++
		}
		nmembers++
	}

	nquorum := nmembers/2 + 1
	if nstarted < nquorum {
		log.Log.Warnf("Reject remove member request: the number of started member (%d) will be less than the quorum number of the cluster (%d)", nstarted, nquorum)
		return false
	}

	return true
}

func membersFromStore(st storage.Storage) (map[types.ID]*Member, map[types.ID]bool) {
	members := make(map[types.ID]*Member)
	removed := make(map[types.ID]bool)
	raftCluster, err := GetRaftClusterFromStorage(st)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			return members, removed
		}
		log.Log.Panic("GetRaftClusterFromStorage,err:%s", err.Error())
	}

	return raftCluster.MembersMap, raftCluster.RemovedMap
}

// ValidateClusterAndAssignIDs validates the local cluster by matching the PeerURLs
// with the existing cluster. If the validation succeeds, it assigns the IDs
// from the existing cluster to the local cluster.
// If the validation fails, an error will be returned.
func ValidateClusterAndAssignIDs(local *RaftCluster, existing *RaftCluster) error {
	ems := existing.Members()
	lms := local.Members()
	if len(ems) != len(lms) {
		return fmt.Errorf("member count is unequal")
	}
	sort.Sort(MembersByPeerURLs(ems))
	sort.Sort(MembersByPeerURLs(lms))

	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	for i := range ems {
		if ok, err := netutil.URLStringsEqual(ctx, ems[i].PeerURLs, lms[i].PeerURLs); !ok {
			return fmt.Errorf("unmatched member while checking PeerURLs (%v)", err)
		}
		lms[i].ID = ems[i].ID
	}
	local.MembersMap = make(map[types.ID]*Member)
	for _, m := range lms {
		local.MembersMap[m.ID] = m
	}
	return nil
}

// If logerr is true, it prints out more error messages.
func getClusterFromRemotePeers(urls []string, timeout time.Duration, logerr bool, rt http.RoundTripper) (*RaftCluster, error) {
	cc := &http.Client{
		Transport: rt,
		Timeout:   timeout,
	}
	for _, u := range urls {
		resp, err := cc.Get(u + "/members")
		if err != nil {
			if logerr {
				log.Log.Warnf("could not get cluster response from %s: %v", u, err)
			}
			continue
		}
		b, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			if logerr {
				log.Log.Warnf("could not read the body of cluster response: %v", err)
			}
			continue
		}
		var membs []*Member
		if err = json.Unmarshal(b, &membs); err != nil {
			if logerr {
				log.Log.Warnf("could not unmarshal cluster response: %v", err)
			}
			continue
		}
		id, err := types.IDFromString(resp.Header.Get("X-Etcd-Cluster-ID"))
		if err != nil {
			if logerr {
				log.Log.Warnf("could not parse the cluster ID from cluster res: %v", err)
			}
			continue
		}

		// check the length of membership MembersMap
		// if the membership MembersMap are present then prepare and return raft cluster
		// if membership MembersMap are not present then the raft cluster formed will be
		// an invalid empty cluster hence return failed to get raft cluster member(s) from the given urls error
		if len(membs) > 0 {
			return NewClusterFromMembers(id, membs), nil
		}

		return nil, errors.New("failed to get raft cluster member(s) from the given url")
	}
	return nil, fmt.Errorf("could not retrieve cluster information from the given urls")
}

// GetRemotePeerURLs returns peer urls of remote MembersMap in the cluster. The
// returned list is sorted in ascending lexicographical order.
func GetRemotePeerURLs(cl *RaftCluster, local string) []string {
	us := make([]string, 0)
	for _, m := range cl.Members() {
		if m.Name == local {
			continue
		}
		us = append(us, m.PeerURLs...)
	}
	sort.Strings(us)
	return us
}

// IsMemberBootstrapped tries to check if the given member has been bootstrapped
// in the given cluster.
func IsMemberBootstrapped(cl *RaftCluster, member string, rt http.RoundTripper, timeout time.Duration) bool {
	rcl, err := getClusterFromRemotePeers(getRemotePeerURLs(cl, member), timeout, false, rt)
	if err != nil {
		return false
	}
	id := cl.MemberByName(member).ID
	m := rcl.Member(id)
	if m == nil {
		return false
	}
	return false
}

// getRemotePeerURLs returns peer urls of remote MembersMap in the cluster. The
// returned list is sorted in ascending lexicographical order.
func getRemotePeerURLs(cl *RaftCluster, local string) []string {
	us := make([]string, 0)
	for _, m := range cl.Members() {
		if m.Name == local {
			continue
		}
		us = append(us, m.PeerURLs...)
	}
	sort.Strings(us)
	return us
}

// GetClusterFromRemotePeers takes a set of URLs representing etcd peers, and
// attempts to construct a Cluster by accessing the MembersMap endpoint on one of
// these URLs. The first URL to provide a response is used. If no URLs provide
// a response, or a Cluster cannot be successfully created from a received
// response, an error is returned.
// Each request has a 10-second timeout. Because the upper limit of TTL is 5s,
// 10 second is enough for building connection and finishing request.
func GetClusterFromRemotePeers(urls []string, rt http.RoundTripper) (*RaftCluster, error) {
	return getClusterFromRemotePeers(urls, 10*time.Second, true, rt)
}
