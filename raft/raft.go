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

package raft

import (
	"sync"
	"time"

	"sync/atomic"

	"encoding/binary"

	"github.com/coreos/etcd/pkg/contention"
	etcdraft "github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/storage"
	"github.com/flike/kingbus/utils"
)

const (
	// max number of in-flight Snapshot messages etcdserver allows to have
	// This number is more than enough for most clusters with 5 machines.
	maxInFlightMsgSnap = 16
)

//Node represents a raft node
type Node struct {
	tickMu *sync.Mutex
	NodeConfig

	// a chan to send/receive Snapshot
	msgSnapC chan raftpb.Message

	// a chan to send out ApplyEntry
	applyc       chan ApplyEntry
	appliedIndex uint64 // must use atomic operations to access; keep 64-bit aligned.

	committedIndex uint64 // must use atomic operations to access,UpdateCommittedIndex
	term           uint64 // must use atomic operations to access

	// a chan to send out readState
	readStateC chan etcdraft.ReadState

	// utility
	ticker *time.Ticker
	// contention detectors for raft Heartbeat message
	td *contention.TimeoutDetector

	stopped chan struct{}
	done    chan struct{}
}

// ApplyEntry contains entries, Snapshot to be applied. Once
// an ApplyEntry is consumed, the entries will be persisted to
// to raft storage concurrently; the application must read
// raftDone before assuming the raft messages are stable.
type ApplyEntry struct {
	Entries  []raftpb.Entry
	Snapshot raftpb.Snapshot
	// Notifyc synchronizes etcd server applies with the raft node
	Notifyc chan struct{}
}

//NodeConfig is the config of raft node
type NodeConfig struct {
	// to check if msg receiver is removed from cluster
	IsIDRemoved func(id uint64) bool
	etcdraft.Node
	//raftStorage *raft.MemoryStorage
	//storage     Storage
	//store raft log
	Storage storage.Storage
	//MetadataStorage MetadataStorage
	Heartbeat time.Duration // for logging
	// Transport specifies the Transport to send and receive msgs to members.
	// Sending messages MUST NOT block. It is okay to drop messages, since
	// clients should timeout and reissue their messages.
	// If Transport is nil, server will panic.
	Transport rafthttp.Transporter

	PeerListener []*peerListener
}

//NewNode create a raft node
func NewNode(cfg NodeConfig) *Node {
	r := &Node{
		tickMu:     new(sync.Mutex),
		NodeConfig: cfg,
		// set up contention detectors for raft Heartbeat message.
		// expect to send a Heartbeat within 2 Heartbeat intervals.
		td:         contention.NewTimeoutDetector(2 * cfg.Heartbeat),
		readStateC: make(chan etcdraft.ReadState, 1),
		msgSnapC:   make(chan raftpb.Message, maxInFlightMsgSnap),
		applyc:     make(chan ApplyEntry),
		stopped:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	if r.Heartbeat == 0 {
		r.ticker = &time.Ticker{}
	} else {
		r.ticker = time.NewTicker(r.Heartbeat)
	}
	return r
}

// raft.Node does not have locks in Raft package
func (r *Node) tick() {
	r.tickMu.Lock()
	r.Tick()
	r.tickMu.Unlock()
}

//GetAppliedIndex get the applied raft index
func (r *Node) GetAppliedIndex() uint64 {
	return atomic.LoadUint64(&r.appliedIndex)
}

//SetAppliedIndex set the applied raft index, not used
func (r *Node) SetAppliedIndex(v uint64) {
	atomic.StoreUint64(&r.appliedIndex, v)
}

//SetCommittedIndex set the committed raft index
func (r *Node) SetCommittedIndex(v uint64) {
	atomic.StoreUint64(&r.committedIndex, v)
}

//GetCommittedIndex get the committed raft index
func (r *Node) GetCommittedIndex() uint64 {
	return atomic.LoadUint64(&r.committedIndex)
}

//SetTerm set the raft term
func (r *Node) SetTerm(v uint64) {
	atomic.StoreUint64(&r.term, v)
}

//GetTerm get the raft term
func (r *Node) GetTerm() uint64 {
	return atomic.LoadUint64(&r.term)
}

//MustGetAppliedIndex get the applied raft index, if occur error, fatal
func MustGetAppliedIndex(s storage.Storage) uint64 {
	value, err := s.Get(utils.StringToBytes(storage.AppliedIndexKey))
	if err != nil {
		log.Log.Fatalf("MustGetAppliedIndex:Get error,err:%s,key:%s", err.Error(), storage.AppliedIndexKey)
	}
	if len(value) == 0 {
		log.Log.Warningf("key:%s not exist,return 0", storage.AppliedIndexKey)
		return 0
	}

	return binary.BigEndian.Uint64(value)
}

// ReadyHandler contains a set of EtcdServer operations to be called by Node,
// and helps decouple state machine logic from Raft algorithms.
// TODO: add a state machine interface to ApplyEntry the commit entries and do Snapshot/recover
type ReadyHandler struct {
	GetLead              func() (lead uint64)
	UpdateLead           func(lead uint64)
	UpdateLeadership     func(leadChange bool)
	UpdateCommittedIndex func(uint64)
}

//Stop implements stop raft node
func (r *Node) Stop() {
	select {
	case r.stopped <- struct{}{}:
	case <-r.done:
		return
	}
	<-r.done
}

//Apply return the apply channel
func (r *Node) Apply() chan ApplyEntry {
	return r.applyc
}

//DoneC return the done channel
func (r *Node) DoneC() chan struct{} {
	return r.done
}

//MsgSnapC return snapshot channel
func (r *Node) MsgSnapC() chan raftpb.Message {
	return r.msgSnapC
}

//Run must keep run as fast as possible
//If execution is too slow, ticker will suffer,
//then tickHeartbeat(tickElection) will be influenced
func (r *Node) Run(rh *ReadyHandler) {
	internalTimeout := time.Second

	go func() {
		defer r.onStop()
		isLead := false

		for {
			select {
			case <-r.ticker.C:
				r.tick()
			case rd := <-r.Ready():
				if rd.SoftState != nil {
					leadChange := rd.SoftState.Lead != etcdraft.None && rh.GetLead() != rd.SoftState.Lead
					rh.UpdateLead(rd.SoftState.Lead)
					isLead = rd.SoftState.RaftState == etcdraft.StateLeader
					rh.UpdateLeadership(leadChange)
					r.td.Reset()
				}

				if len(rd.ReadStates) != 0 {
					select {
					case r.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
					case <-time.After(internalTimeout):
						log.Log.Warnf("timed out sending read state")
					case <-r.stopped:
						return
					}
				}

				notifyc := make(chan struct{}, 1)
				ap := ApplyEntry{
					Entries:  rd.CommittedEntries,
					Snapshot: rd.Snapshot,
					Notifyc:  notifyc,
				}
				//1. update committed index
				updateCommittedIndex(&ap, rh)

				//2. the leader can write to its disk in parallel with replicating to the followers and them
				// writing to their disks.
				// For more details, check raft thesis 10.2.1
				if isLead {
					// gofail: var raftBeforeLeaderSend struct{}
					r.Transport.Send(r.processMessages(rd.Messages))
				}

				//3. if not empty, save HardState
				if !etcdraft.IsEmptyHardState(rd.HardState) {
					if err := r.Storage.SaveHardState(rd.HardState); err != nil {
						log.Log.Fatalf("raft save state error: %v", err.Error())
					}
				}

				//4. save rd.entries into storage
				if err := r.appendRaftEntries(rd.Entries); err != nil {
					log.Log.Fatalf("appendRaftEntries error: %v", err.Error())
				}

				//5. apply committed entries, must after appendRaftEntries,
				// keep appliedIndex little than storage.LastIndex
				// since rd.Entries maybe committed in other nodes.
				select {
				case r.applyc <- ap:
				case <-r.stopped:
					return
				}

				if !isLead {
					// finish processing incoming messages before we signal raftdone chan
					msgs := r.processMessages(rd.Messages)

					// Candidate or follower needs to wait for all pending configuration
					// changes to be applied before sending messages.
					// Otherwise we might incorrectly count votes (e.g. votes from removed members).
					// Also slow machine's follower raft-layer could proceed to become the leader
					// on its own single-node cluster, before ApplyEntry-layer applies the config change.
					// We simply wait for ALL pending entries to be applied for now.
					// We might improve this later on if it causes unnecessary long blocking issues.
					waitApply := false
					for _, ent := range rd.CommittedEntries {
						if ent.Type == raftpb.EntryConfChange {
							waitApply = true
							break
						}
					}
					if waitApply {
						<-notifyc
					}

					// gofail: var raftBeforeFollowerSend struct{}
					r.Transport.Send(msgs)
				}
				r.Advance()
			case <-r.stopped:
				return
			}
		}
	}()
}

func (r *Node) appendRaftEntries(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	lastIndex, err := r.Storage.LastIndex()
	if err != nil {
		return err
	}
	if lastIndex >= entries[0].Index {
		err = r.Storage.TruncateSuffix(entries[0].Index)
		if err != nil {
			log.Log.Errorf("TruncateSuffix error,err:%s,index:%d", err, entries[0].Index)
			return err
		}
		log.Log.Infof("appendRaftEntries:TruncateSuffix to %d,entries[len-1].raftIndex:%d,lastIndex:%d",
			entries[0].Index, entries[len(entries)-1].Index, lastIndex)
	}
	err = r.Storage.SaveRaftEntries(entries)
	if err != nil {
		log.Log.Errorf("SaveRaftEntries error,err:%s,len(entries):%d", err, len(entries))
		return err
	}
	return nil
}

func updateCommittedIndex(ap *ApplyEntry, rh *ReadyHandler) {
	var ci uint64
	if len(ap.Entries) != 0 {
		ci = ap.Entries[len(ap.Entries)-1].Index
	}
	if ci != 0 {
		rh.UpdateCommittedIndex(ci)
	}
}

func (r *Node) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if r.IsIDRemoved(ms[i].To) {
			ms[i].To = 0
		}

		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {
			// There are two separate data store: the store for v2, and the KV for v3.
			// The msgSnap only contains the most recent Snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store Snapshot and KV Snapshot.
			select {
			case r.msgSnapC <- ms[i]:
			default:
				// drop msgSnap if the inflight chan if full.
			}
			ms[i].To = 0
		}
		if ms[i].Type == raftpb.MsgHeartbeat {
			ok, exceed := r.td.Observe(ms[i].To)
			if !ok {
				// TODO: limit request rate.
				log.Log.Warnf("failed to send out Heartbeat on time (exceeded the %v timeout for %v)", r.Heartbeat, exceed)
				log.Log.Warnf("server is likely overloaded")
			}
		}
	}
	return ms
}

func (r *Node) onStop() {
	r.ticker.Stop()
	r.Transport.Stop()
	if err := r.StopPeerListener(); err != nil {
		log.Log.Fatalf("StopPeerListener error,err:%s", err.Error())
	}
	if err := r.Storage.Close(); err != nil {
		log.Log.Fatalf("Storage close storage error: %v", err)
	}
	close(r.done)
}

// AdvanceTicks advances ticks of Raft node.
// This can be used for fast-forwarding election
// ticks in multi data-center deployments, thus
// speeding up election process.
func AdvanceTicks(n etcdraft.Node, electionTicks int) {
	for i := 0; i < electionTicks-1; i++ {
		n.Tick()
	}
}
