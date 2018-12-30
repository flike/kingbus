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

package server

import (
	"encoding/json"
	"fmt"
	"path"

	"github.com/flike/kingbus/storage/storagepb"
	uuid "github.com/satori/go.uuid"

	"github.com/flike/kingbus/config"
	"github.com/flike/kingbus/utils"

	"time"

	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/pkg/types"
	etcdraft "github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/mysql"
	"github.com/flike/kingbus/raft"
	"github.com/flike/kingbus/raft/membership"
	"github.com/flike/kingbus/storage"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

const (
	warnApplyDuration = 100 * time.Millisecond
)

func (s *KingbusServer) applyAll(ap *raft.ApplyEntry) {
	if len(ap.Entries) == 0 {
		ap.Notifyc <- struct{}{}
		return
	}

	s.applyEntries(ap)
	ap.Notifyc <- struct{}{}
}

func (s *KingbusServer) applyEntries(ap *raft.ApplyEntry) {
	if len(ap.Entries) == 0 {
		return
	}
	firstIndex := ap.Entries[0].Index
	appliedIndex := s.raftNode.GetAppliedIndex()
	if firstIndex > appliedIndex+1 {
		log.Log.Panicf("first index of committed entry[%d] should <= appliedIndex[%d] + 1", firstIndex, appliedIndex)
	}
	var ents []raftpb.Entry
	//raft index to slice index
	if appliedIndex+1-firstIndex < uint64(len(ap.Entries)) {
		ents = ap.Entries[appliedIndex+1-firstIndex:]
	}
	if len(ents) == 0 {
		return
	}
	var shouldstop bool
	if shouldstop = s.apply(ents); shouldstop {
		log.Log.Infof("the member has been permanently removed from the cluster")
		go s.Stop()
	}
}

type confChangeResponse struct {
	membs []*membership.Member
	err   error
}

// apply takes entries received from Raft (after it has been committed) and
// applies them to the current state of the EtcdServer.
// The given entries should not be empty.
func (s *KingbusServer) apply(es []raftpb.Entry) bool {
	var shouldStop bool
	for i := range es {
		e := es[i]
		switch e.Type {
		case raftpb.EntryNormal:
			s.applyEntryNormal(&e)
			s.raftNode.SetAppliedIndex(e.Index)
			s.raftNode.SetTerm(e.Term)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			pbutil.MustUnmarshal(&cc, e.Data)
			removedSelf, err := s.applyConfChange(cc)
			s.raftNode.SetAppliedIndex(e.Index)
			s.raftNode.SetTerm(e.Term)
			shouldStop = shouldStop || removedSelf
			s.wait.Trigger(cc.ID, &confChangeResponse{s.cluster.Members(), err})
		default:
			log.Log.Panicf("entry type should be either EntryNormal or EntryConfChange")
		}
	}
	return shouldStop
}

// applyEntryNormal apples an EntryNormal type raftpb request to the EtcdServer
func (s *KingbusServer) applyEntryNormal(e *raftpb.Entry) {
	if e.Type == raftpb.EntryNormal && len(e.Data) == 0 {
		return
	}

	//get data type
	//apply data accord with data type
	dataType := e.Data[0]
	switch dataType {
	case utils.MySQLBinlogEventType:
		s.applyMySQLBinlogEvent(e)
		s.applyBroadcast.Send()
	case utils.NodeAttributesType:
		s.applyNodeAttributes(e)
	case utils.SyncerArgsType:
		s.applySyncerArgs(e)
	case utils.MasterInfoType:
		s.applyMasterInfo(e)
	case utils.MasterGtidPurgedType:
		s.applyMasterGtidPurged(e)
	default:
		log.Log.Fatalf("apply data not support,entry:%v,entry.data:%s", *e, string(e.Data))
	}
}

//applyMySQLBinlogEvent implements apply mysql binlog event
func (s *KingbusServer) applyMySQLBinlogEvent(e *raftpb.Entry) {
	var err error

	binlogEvent := new(storagepb.BinlogEvent)
	pbutil.MustUnmarshal(binlogEvent, e.Data[1:])
	eventType := replication.EventType(binlogEvent.Type)
	applyEps.Mark(1)

	//update syncer executed gtid set
	if binlogEvent.DividedCount == 0 || (binlogEvent.DividedCount != 0 && binlogEvent.DividedSeqNum == 0) {
		err = s.binlogProgress.updateProcess(e.Index, binlogEvent.Data)
		if err != nil {
			log.Log.Fatalf("resetBinlogProgress error,err:%s,eventType:%v,data:%v",
				err, eventType, binlogEvent.Data)
		}
	}

	if needApply(eventType) {
		err := s.applyBinlogEvent(binlogEvent.Data, eventType, e.Index)
		if err != nil {
			log.Log.Fatalf("apply binlog event error,err:%v,eventType:%v,binlogEvent:%v",
				err, eventType, *binlogEvent)
		}
	}
}

//when syncer start, syncer will propose the master gtidPurged into cluster.
//all nodes will apply the gtidPurged, and initialize binlogProgress
func (s *KingbusServer) applyMasterGtidPurged(e *raftpb.Entry) {
	var args config.MasterGtidPurged
	var err error
	err = json.Unmarshal(e.Data[1:], &args)
	if err != nil {
		log.Log.Fatalf("applyMasterGtidPurged error,err:%v,e:%v",
			err, *e)
	}
	//1.update gtidPurged
	gtids, err := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, args.GtidPurged)
	if err != nil {
		log.Log.Fatalf("gomysql.ParseGTIDSet error,err: %s, gtids: %s", args.GtidPurged)
	}
	masterGtidPurged := gtids.(*gomysql.MysqlGTIDSet)

	gtidPurged, err := s.store.GetGtidSet(gomysql.MySQLFlavor, storage.GtidPurgedKey)
	if err != nil {
		log.Log.Fatalf("NewSyncer:get gtidPurged error,err:%s", err)
	}
	syncerGtidPurged := gtidPurged.(*gomysql.MysqlGTIDSet)
	//add the set which uuid not in syncer gtidPurged
	for uuid, set := range masterGtidPurged.Sets {
		if _, ok := syncerGtidPurged.Sets[uuid]; !ok {
			syncerGtidPurged.AddSet(set)
		}
	}
	err = s.store.SetGtidSet(gomysql.MySQLFlavor, storage.GtidPurgedKey, syncerGtidPurged)
	if err != nil {
		log.Log.Fatalf("SetGtidSet error,err: %s, gtids: %s", gtidPurged.String())
	}

	//reset  binlogProgress
	err = s.binlogProgress.reset(args.GtidPurged)
	if err != nil {
		log.Log.Fatalf("binlogProgress reset error,err: %s, gtids: %s", args.GtidPurged)
	}

	log.Log.Infof("applyMasterGtidPurged successfully,MasterGtidPurged:%v,executedGtidSet:%s,"+
		"gtidPurged:%s", args, s.binlogProgress.ExecutedGtidSetStr(), gtidPurged.String())
}

//applyBinlogEvent must be idempotent
func (s *KingbusServer) applyBinlogEvent(eventRawData []byte, eventType replication.EventType, raftIndex uint64) error {
	var err error

	//save fde, don't need parse
	if eventType == replication.FORMAT_DESCRIPTION_EVENT {
		raftIndexStr := fmt.Sprintf("%020d", raftIndex)
		fdeKey := path.Join(storage.FdePrefix, raftIndexStr)
		key := utils.StringToBytes(fdeKey)
		err = s.store.Set(key, eventRawData)
		if err != nil {
			log.Log.Errorf("SaveToStorage error,err:%s,key:%s,value:%v",
				err, fdeKey, eventRawData)
			return err
		}
		log.Log.Debugf("save fde into meta storage,raftIndex:%d", raftIndex)
		return nil
	}

	//remove event header, parse event body, and save
	eventRawData = eventRawData[replication.EventHeaderSize:]
	//remove crc32
	eventRawData = eventRawData[:len(eventRawData)-replication.BinlogChecksumLength]

	switch eventType {
	case replication.GTID_EVENT:
		//update current_gitd
		e := &replication.GTIDEvent{}
		if err := e.Decode(eventRawData); err != nil {
			log.Log.Errorf("Decode GtidEvent error,err:%s,eventRawData:%v", err, eventRawData)
			return err
		}
		u, err := uuid.FromBytes(e.SID)
		if err != nil {
			return err
		}
		s.binlogProgress.currentGtid.Store(fmt.Sprintf("%s:%d", u.String(), e.GNO))
		log.Log.Debugf("apply a GTID_EVENT,raftIndex:%d,s.s.currentGtid:%s", raftIndex, s.binlogProgress.currentGtid.Load())
	case replication.ROTATE_EVENT:
		e := &replication.RotateEvent{}
		if err := e.Decode(eventRawData); err != nil {
			return err
		}

		raftIndexStr := fmt.Sprintf("%020d", raftIndex)
		nextBinlogFileKey := path.Join(storage.NextBinlogPrefix, raftIndexStr)
		key := utils.StringToBytes(nextBinlogFileKey)
		value := e.NextLogName
		err = s.store.Set(key, value)
		if err != nil {
			log.Log.Errorf("SaveToStorage error,err:%s,key:%s,value:%s",
				err, nextBinlogFileKey, string(value))
			return err
		}
		//the the heartbeat event info
		s.binlogProgress.lastBinlogFile.Store(utils.BytesToString(e.NextLogName))
		log.Log.Debugf("apply a ROTATE_EVENT,raftIndex:%d,value:%s", raftIndex, string(value))
	case replication.PREVIOUS_GTIDS_EVENT:
		e := &replication.PreviousGtidsLogEvent{}
		if err := e.Decode(eventRawData); err != nil {
			return err
		}
		err = s.store.SetPreviousGtidSet(raftIndex, e.GSet)
		if err != nil {
			log.Log.Errorf("SetPreviousGtidSet error,err:%s,value:%s",
				err, e.GSet.String())
			return err
		}
		log.Log.Debugf("apply a PREVIOUS_GTIDS_EVENT,previousGtidSet:%s", e.GSet.String())
	default:
		log.Log.Errorf("do not need apply in kingbus")
		return ErrUnsupport
	}
	return err
}

func (s *KingbusServer) applyNodeAttributes(e *raftpb.Entry) {
	var attributes config.Attributes
	var ms membership.Attributes
	err := json.Unmarshal(e.Data[1:], &attributes)
	if err != nil {
		log.Log.Fatalf("applyNodeAttributes error,err:%v,e:%v",
			err, *e)
	}
	ms.Name = attributes.Name
	ms.AdminURLs = make([]string, len(attributes.AdminURLs))
	copy(ms.AdminURLs, attributes.AdminURLs)
	s.cluster.UpdateAttributes(attributes.ID, ms)
	log.Log.Infof("applyNodeAttributes successfully,ms:%v", ms)
}

func (s *KingbusServer) applySyncerArgs(e *raftpb.Entry) {
	var syncerArgs config.SyncerArgs
	err := json.Unmarshal(e.Data[1:], &syncerArgs)
	if err != nil {
		log.Log.Fatalf("applySyncerArgs error,err:%v,e:%v",
			err, *e)
	}

	err = s.store.Set(utils.StringToBytes(storage.SyncerArgsKey), e.Data[1:])
	if err != nil {
		log.Log.Fatalf("set SyncerArgsKey error,err:%v,e:%v",
			err, *e)
	}
	log.Log.Infof("applySyncerArgs successfully,syncerArgs:%v", syncerArgs)
}

// applyConfChange applies a ConfChange to the server. It is only
// invoked with a ConfChange that has already passed through Raft
func (s *KingbusServer) applyConfChange(cc raftpb.ConfChange) (bool, error) {
	if err := s.cluster.ValidateConfigurationChange(cc); err != nil {
		cc.NodeID = etcdraft.None
		confState := s.raftNode.ApplyConfChange(cc)
		saveConfState(s.cluster.Store, confState)
		return false, err
	}

	//save confState for restore
	confState := s.raftNode.ApplyConfChange(cc)
	defer saveConfState(s.cluster.Store, confState)

	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		m := new(membership.Member)
		if err := json.Unmarshal(cc.Context, m); err != nil {
			log.Log.Fatalf("unmarshal member should never fail: %v", err)
		}
		if cc.NodeID != uint64(m.ID) {
			log.Log.Fatalf("nodeID should always be equal to member id")
		}
		s.cluster.AddMember(m)
		if m.ID != s.id {
			s.raftNode.Transport.AddPeer(m.ID, m.PeerURLs)
		}
	case raftpb.ConfChangeRemoveNode:
		id := types.ID(cc.NodeID)
		s.cluster.RemoveMember(id)
		if id == s.id {
			return true, nil
		}
		s.raftNode.Transport.RemovePeer(id)
	case raftpb.ConfChangeUpdateNode:
		m := new(membership.Member)
		if err := json.Unmarshal(cc.Context, m); err != nil {
			log.Log.Fatalf("unmarshal member should never fail: %v", err)
		}
		if cc.NodeID != uint64(m.ID) {
			log.Log.Fatalf("nodeID should always be equal to member id")
		}
		s.cluster.UpdateRaftAttributes(m.ID, m.RaftAttributes)
		if m.ID != s.id {
			s.raftNode.Transport.UpdatePeer(m.ID, m.PeerURLs)
		}
		//todo  ConfChangeAddLearnerNode
	}
	return false, nil
}

func (s *KingbusServer) applyMasterInfo(e *raftpb.Entry) {
	var info mysql.MasterInfo
	err := json.Unmarshal(e.Data[1:], &info)
	if err != nil {
		log.Log.Fatalf("applyMasterInfo error,err:%v,e:%v",
			err, *e)
	}
	key := utils.StringToBytes(storage.MasterInfoKey)
	err = s.store.Set(key, e.Data[1:])
	if err != nil {
		log.Log.Fatalf("applyFakeRotateEventArgs set store error,err:%s,key:%s,value:%s",
			err, storage.MasterInfoKey, e.Data[1:])
	}

	log.Log.Debugf("applyMasterInfo,raftIndex:%d,masterInfo:%v", e.Index, info)
}

func saveConfState(store storage.Storage, confState *raftpb.ConfState) {
	if confState == nil {
		return
	}
	log.Log.Debugf("saveConfState:confState is %v", *confState)

	value, err := json.Marshal(confState)
	if err != nil {
		log.Log.Fatalf("marshal confState error,err:%s,confState:%v", err, *confState)
	}
	key := utils.StringToBytes(storage.ConfStateKey)
	err = store.Set(key, value)
	if err != nil {
		log.Log.Fatalf("SaveToStorage error,err:%s,key:%s,value:%v", err, storage.ConfStateKey, *confState)
	}
}

func needApply(eventType replication.EventType) bool {
	return eventType == replication.GTID_EVENT ||
		eventType == replication.FORMAT_DESCRIPTION_EVENT ||
		eventType == replication.ROTATE_EVENT ||
		eventType == replication.PREVIOUS_GTIDS_EVENT
}
