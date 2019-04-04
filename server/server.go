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
	"context"
	"fmt"
	"net/url"
	"time"

	"errors"

	"encoding/json"

	"sync"

	"strings"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/idutil"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/pkg/wait"
	etcdraft "github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/flike/kingbus/api"
	"github.com/flike/kingbus/config"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/mysql"
	"github.com/flike/kingbus/raft"
	"github.com/flike/kingbus/raft/membership"
	"github.com/flike/kingbus/storage"
	"github.com/flike/kingbus/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rcrowley/go-metrics"
	"go.uber.org/atomic"
)

const (
	// Never overflow the rafthttp buffer, which is 4096.
	maxInflightMsgs = 512
	// HealthInterval is the minimum time the cluster should be healthy
	// before accepting add member requests.
	HealthInterval = 5 * time.Second
	//DialTimeout is the timeout of dial
	DialTimeout = 5 * time.Second
	//ProposeMaxRetryCount is the max count of propose retry times
	ProposeMaxRetryCount = 1000
)

//KingbusServer is a instance run all sub servers
type KingbusServer struct {
	Cfg            *config.KingbusServerConfig
	adminSvr       *api.AdminServer
	syncer         *Syncer
	master         *BinlogServer
	binlogProgress *BinlogProgress
	prometheusSvr  *PrometheusServer

	id       types.ID
	raftNode *raft.Node
	cluster  *membership.RaftCluster
	store    storage.Storage

	// must use atomic operations to access; keep 64-bit aligned.
	lead            *atomic.Uint64
	leadElectedTime time.Time
	//lock for leadElectedTime
	Mu     sync.RWMutex
	errorc chan error

	// wait is use for waiting response
	wait     wait.Wait
	reqIDGen *idutil.Generator

	//use for broadcasting the apply event
	applyBroadcast *utils.Broadcast

	//metics
	stats  *stats.ServerStats
	lstats *stats.LeaderStats

	//stopping is use to fast fail for client request
	stopping chan struct{}
	started  *atomic.Bool
}

func (s *KingbusServer) starRaft(cfg config.RaftNodeConfig) error {
	var (
		etcdRaftNode etcdraft.Node
		id           types.ID
		cl           *membership.RaftCluster
		remotes      []*membership.Member
		appliedIndex uint64
	)

	prt, err := rafthttp.NewRoundTripper(transport.TLSInfo{}, DialTimeout)
	if err != nil {
		return err
	}

	store, err := storage.NewDiskStorage(cfg.DataDir, cfg.ReserveDataSize)
	if err != nil {
		log.Log.Fatalf("NewKingbusServer:NewDiskStorage error,err:%s,dir:%s", err.Error(), cfg.DataDir)
	}

	//store, err := storage.NewMemoryStorage(cfg.DataDir)
	//if err != nil {
	//	log.Log.Fatalf("NewKingbusServer:NewMemoryStorage error,err:%s,dir:%s", err.Error(), cfg.DataDir)
	//}

	defer func() {
		//close storage when occur error
		if err != nil {
			store.Close()
		}
	}()

	logExist := utils.ExistLog(cfg.DataDir)
	switch {
	case !logExist && !cfg.NewCluster:
		if err = cfg.VerifyJoinExisting(); err != nil {
			return err
		}
		cl, err = membership.NewClusterFromURLsMap(cfg.InitialPeerURLsMap)
		if err != nil {
			return err
		}
		remotePeerURLs := membership.GetRemotePeerURLs(cl, cfg.Name)
		existingCluster, gerr := membership.GetClusterFromRemotePeers(remotePeerURLs, prt)
		if gerr != nil {
			return fmt.Errorf("cannot fetch cluster info from peer urls: %v", gerr)
		}
		if err = membership.ValidateClusterAndAssignIDs(cl, existingCluster); err != nil {
			return fmt.Errorf("error validating peerURLs %s: %v", existingCluster, err)
		}

		remotes = existingCluster.Members()
		cl.SetID(existingCluster.GetID())
		cl.SetStore(store)
		id, etcdRaftNode = startEtcdRaftNode(cfg, store, cl, nil)
	case !logExist && cfg.NewCluster:
		if err = cfg.VerifyBootstrap(); err != nil {
			return err
		}
		cl, err = membership.NewClusterFromURLsMap(cfg.InitialPeerURLsMap)
		if err != nil {
			return err
		}
		m := cl.MemberByName(cfg.Name)
		if membership.IsMemberBootstrapped(cl, cfg.Name, prt, DialTimeout) {
			return fmt.Errorf("member %s has already been bootstrapped", m.ID)
		}

		cl.SetStore(store)
		id, etcdRaftNode = startEtcdRaftNode(cfg, store, cl, cl.MemberIDs())
	case logExist:
		if err = utils.IsDirWriteable(cfg.DataDir); err != nil {
			return fmt.Errorf("cannot write to member directory: %v", err)
		}
		//node restart, read states from storage
		//get applied index
		appliedIndex = raft.MustGetAppliedIndex(store)
		cfg.AppliedIndex = appliedIndex
		id, etcdRaftNode, cl = restartEtcdNode(cfg, store)
		cl.SetStore(store)
	default:
		return fmt.Errorf("unsupported bootstrap config")
	}

	s.raftNode = raft.NewNode(
		raft.NodeConfig{
			IsIDRemoved: func(id uint64) bool { return cl.IsIDRemoved(types.ID(id)) },
			Node:        etcdRaftNode,
			Heartbeat:   cfg.HeartbeatMs,
			Storage:     store,
		},
	)
	//committedIndex,term will update by fsm(UpdateCommittedIndex,SetTerm)
	//set appliedIndex when applyEntries will check the entry continuity
	s.raftNode.SetAppliedIndex(appliedIndex)

	s.id = id
	s.wait = wait.New()
	s.reqIDGen = idutil.NewGenerator(uint16(id), time.Now())
	s.stopping = make(chan struct{})
	s.errorc = make(chan error)
	s.applyBroadcast = utils.NewBroadcast()
	s.stats = stats.NewServerStats(cfg.Name, id.String())
	s.lstats = stats.NewLeaderStats(id.String())
	s.store = store

	tr := &rafthttp.Transport{
		TLSInfo:     transport.TLSInfo{},
		DialTimeout: DialTimeout,
		ID:          id,
		URLs:        cfg.PeerURLs,
		ClusterID:   cl.GetID(),
		Raft:        s,
		ServerStats: s.stats,
		LeaderStats: s.lstats,
		ErrorC:      s.errorc,
	}
	if err = tr.Start(); err != nil {
		return err
	}
	// add all remotes into transport
	//Add remotes to rafthttp, who help newly joined members catch up the
	//progress of the cluster. It supports basic message sending to remote, and
	//has no stream connection for simplicity. remotes will not be used
	//after the latest peers have been added into rafthttp.
	for _, m := range remotes {
		if m.ID != id {
			tr.AddRemote(m.ID, m.PeerURLs)
		}
	}
	for _, m := range cl.Members() {
		if m.ID != id {
			tr.AddPeer(m.ID, m.PeerURLs)
		}
	}
	s.raftNode.Transport = tr
	s.cluster = cl

	return nil
}

func (s *KingbusServer) startRaftPeer(peerURLs types.URLs) error {
	err := s.raftNode.NewPeerListener(peerURLs)
	if err != nil {
		return err
	}
	s.raftNode.SetPeerHandler()
	s.raftNode.PeerHandlerServe()
	log.Log.Infof("startRaftPeer success")
	return nil
}

func (s *KingbusServer) startAdminServer(urls types.URLs) error {
	if len(urls) != 1 {
		return ErrArgs
	}
	addr := urls[0].Host
	s.adminSvr = api.NewAdminServer(addr, s, s.cluster)
	return nil
}

func (s *KingbusServer) startPrometheus(addr string) error {
	if len(addr) == 0 {
		log.Log.Errorf("metrics addr is nil")
		return ErrArgs
	}
	s.prometheusSvr = NewPrometheusServer(addr, metrics.DefaultRegistry, prometheus.DefaultRegisterer, time.Second)
	return nil
}

//NewKingbusServer create a kingbus server
func NewKingbusServer(cfg *config.KingbusServerConfig) (*KingbusServer, error) {
	s := new(KingbusServer)

	s.Cfg = cfg
	s.lead = atomic.NewUint64(0)

	err := s.starRaft(s.Cfg.RaftNodeCfg)
	if err != nil {
		log.Log.Errorf("NewKingbusServer:startRaft error,err:%s", err)
		return nil, err
	}

	err = s.startRaftPeer(s.Cfg.RaftNodeCfg.PeerURLs)
	if err != nil {
		log.Log.Errorf("NewKingbusServer:startRaftPeer error,err:%s", err)
		return nil, err
	}

	err = s.startAdminServer(s.Cfg.AdminURLs)
	if err != nil {
		log.Log.Errorf("NewKingbusServer:startAdminServer error,err:%s", err)
		return nil, err
	}

	err = s.newBinlogProgress()
	if err != nil {
		log.Log.Errorf("NewKingbusServer:newBinlogProgress error,err:%s", err)
		return nil, err
	}

	err = s.startPrometheus(s.Cfg.MetricsAddr)
	if err != nil {
		log.Log.Errorf("NewKingbusServer:startPrometheus error,err:%s", err)
		return nil, err
	}
	s.started = atomic.NewBool(false)

	return s, nil
}

func (s *KingbusServer) updateLeadership(leadChange bool) {
	log.Log.Debugf("updateLeadership:leadChange:%v,s.IsLeader:%v,s.IsSyncerStarted:%v,",
		leadChange, s.IsLeader(), s.IsSyncerStarted())
	log.Log.Debugf("updateLeadership:appliedIndex:%d,committedIndex:%d", s.getAppliedIndex(), s.getCommittedIndex())
	//when kingbus is not lead, should stop syncer
	if s.IsLeader() == false && s.IsSyncerStarted() {
		log.Log.Infof("server[%s] is not leader,stop syncer", s.Cfg.RaftNodeCfg.PeerURLs.String())
		s.StopServer(config.SyncerServerType)
		return
	}

	//lead change and this kingbus became leader, then start syncer automatically
	//delay starting the syncer in new Leader for 10s(must ,
	//to ensure that the syncer on the old Leader has stopped for a long time.
	//You must perform this operation asynchronously,
	//otherwise you will block the raft process and trigger the leader election.
	if leadChange && s.IsLeader() {
		go func() {
			t := time.Now()
			s.Mu.Lock()
			s.leadElectedTime = t
			s.Mu.Unlock()

			<-time.After(time.Second * 10)
			value, err := s.store.Get(utils.StringToBytes(storage.SyncerArgsKey))
			if err != nil {
				log.Log.Errorf("store Get value error,err:%s,key:%s,not start syncer",
					err, storage.SyncerArgsKey)
				return
			}

			if value == nil {
				log.Log.Infof("syncer didn't start, ignore start syncer")
				return
			}

			var syncerArgs config.SyncerArgs
			err = json.Unmarshal(value, &syncerArgs)
			if err != nil {
				log.Log.Fatalf("json unmarshal error,err:%s,value:%v", err, value)
			}
			err = s.StartServer(config.SyncerServerType, &syncerArgs)
			if err != nil {
				log.Log.Errorf("Start syncer error,err:%s,syncerAgs:%v", err, syncerArgs)
				return
			}
			log.Log.Infof("server[%s] become leader,start syncer automatically", s.Cfg.RaftNodeCfg.PeerURLs[0].Host)
		}()
	}

	// TODO: remove the nil checking
	// current test utility does not provide the stats
	if s.stats != nil {
		s.stats.BecomeLeader()
	}
}

func (s *KingbusServer) runRaft() {
	rh := &raft.ReadyHandler{
		GetLead:          func() (lead uint64) { return s.getLead() },
		UpdateLead:       func(lead uint64) { s.setLead(lead) },
		UpdateLeadership: s.updateLeadership,
		UpdateCommittedIndex: func(ci uint64) {
			cci := s.getCommittedIndex()
			if ci > cci {
				s.setCommittedIndex(ci)
			}
		},
	}
	s.raftNode.Run(rh)

	for {
		select {
		case ap := <-s.raftNode.Apply():
			s.applyAll(&ap)
		case <-s.raftNode.DoneC():
			log.Log.Infof("runRaft:quit success")
			return
		}
	}
}

//Run kingbus server
func (s *KingbusServer) Run() {
	if s.started.Load() {
		return
	}
	go s.runRaft()
	go s.adminSvr.Run()
	go s.SyncAdminURL()
	go s.prometheusSvr.Run()
	s.started.Store(true)
	for {
		select {
		case err := <-s.errorc:
			log.Log.Errorf("KingbusServer.Start: run error,need quit. error:%s", err)
			s.Stop()
			return
		case <-s.stopping:
			log.Log.Infof("KingbusServer.Start quit")
			return
		}
	}
}

//Stop kingbus server
func (s *KingbusServer) Stop() {
	if s.started.Load() == false {
		return
	}
	s.adminSvr.Stop()
	s.prometheusSvr.Stop()
	s.StopServer(config.SyncerServerType)
	s.StopServer(config.BinlogServerType)
	s.raftNode.Stop()
	close(s.stopping)

	s.started.Store(false)
	return
}

func startEtcdRaftNode(cfg config.RaftNodeConfig, store storage.Storage, cl *membership.RaftCluster, ids []types.ID) (
	id types.ID, n etcdraft.Node) {
	member := cl.MemberByName(cfg.Name)
	peers := make([]etcdraft.Peer, len(ids))

	for i, id := range ids {
		ctx, err := json.Marshal((*cl).Member(id))
		if err != nil {
			log.Log.Panicf("marshal member should never fail: %v", err)
		}
		peers[i] = etcdraft.Peer{ID: uint64(id), Context: ctx}
	}
	id = member.ID
	log.Log.Infof("starting member %s in cluster %s", id, cl.GetID())

	c := &etcdraft.Config{
		ID:                        uint64(id),
		ElectionTick:              int(cfg.ElectionTimeoutMs / cfg.HeartbeatMs),
		HeartbeatTick:             1,
		Storage:                   store,
		MaxSizePerMsg:             cfg.MaxRequestBytes,
		MaxInflightMsgs:           maxInflightMsgs,
		CheckQuorum:               true,
		PreVote:                   cfg.PreVote,
		DisableProposalForwarding: true,
		Logger:                    log.Log,
	}

	n = etcdraft.StartNode(c, peers)
	raft.AdvanceTicks(n, c.ElectionTick)
	return id, n
}

func restartEtcdNode(cfg config.RaftNodeConfig, store storage.Storage) (
	types.ID, etcdraft.Node, *membership.RaftCluster) {
	cl, err := membership.GetRaftClusterFromStorage(store)
	if err != nil {
		if err != nil {
			log.Log.Panic("GetRaftClusterFromStorage error:%s", err.Error())
		}
	}

	log.Log.Debugf("restartEtcdNode:get raft cluster from storage,cluster:%v", cl.String())

	//get id from raftCluster
	member := cl.MemberByName(cfg.Name)
	if member == nil {
		log.Log.Fatalf("restartEtcdNode:member not in raft cluster,cluster:%v,memberName:%s",
			cl.String(), cfg.Name)
	}
	c := &etcdraft.Config{
		ID:                        uint64(member.ID),
		ElectionTick:              int(cfg.ElectionTimeoutMs / cfg.HeartbeatMs),
		HeartbeatTick:             1,
		Applied:                   cfg.AppliedIndex, //set appliedIndex
		Storage:                   store,
		MaxSizePerMsg:             cfg.MaxRequestBytes,
		MaxInflightMsgs:           maxInflightMsgs,
		CheckQuorum:               true,
		PreVote:                   cfg.PreVote,
		DisableProposalForwarding: true,
		Logger:                    log.Log,
	}

	n := etcdraft.RestartNode(c)
	return member.ID, n, cl
}

//SyncAdminURL sync the admin url between raft cluster
//since only propose data in lead node, other followers should wait
//apply the lead admin url,then send http api request to lead admin url.
func (s *KingbusServer) SyncAdminURL() {
	var id types.ID
	var leaderAdminURL string

	id = s.id
	urls := s.Cfg.AdminURLs.StringSlice()
	attributes := config.Attributes{
		ID:   id,
		Name: s.cluster.MembersMap[id].Name,
	}
	attributes.AdminURLs = make([]string, len(urls))
	copy(attributes.AdminURLs, urls)

	//Waiting for the leader to generate
	for uint64(s.Leader()) == etcdraft.None {
		time.Sleep(time.Millisecond * 500)
		log.Log.Debugf("SyncAdminURL:cluster has no leader,wait for 500ms and then check again")
	}

	if s.IsLeader() {
		data, err := attributes.EncodeWithType()
		if err != nil {
			log.Log.Errorf("SyncAdminURL:attributes EncodeWithType error,err:%v,attributes:%v", err, attributes)
			return
		}
		err = s.Propose(data)
		if err != nil {
			log.Log.Errorf("SyncAdminURL:Propose error,err:%v,attributes:%v", err, attributes)
			return
		}
		return
	}

	//wait for apply update leader url, then send request to leader admin url
	for len(leaderAdminURL) == 0 {
		leaderID := s.Leader()
		leader := s.cluster.Member(leaderID)
		if len(leader.AdminURLs) == 1 {
			leaderAdminURL = leader.AdminURLs[0]
			log.Log.Debugf("leader admin url is 1,leader:%v", *leader)
			break
		}
		log.Log.Debugf("SyncAdminURL:leader admin url is not 1,leader:%v", *leader)
	}

	leaderURL, err := url.Parse(leaderAdminURL)
	if err != nil {
		log.Log.Errorf("SyncAdminURL:Parse error,url:%s", leaderAdminURL)
		return
	}

	updateAdminURL := leaderURL.Scheme + "://" + leaderURL.Host + "/admin/url"
	req, err := json.Marshal(attributes)
	if err != nil {
		log.Log.Errorf("SyncAdminURL:json marshal error,err:%s,attributes:%v", err, attributes)
		return
	}
	resp, err := utils.SendRequest("PUT", updateAdminURL, req)
	if err != nil {
		log.Log.Errorf("SyncAdminURL:SendRequest error,err:%s,url:%s", err, updateAdminURL)
		return
	}
	if resp.Message != "success" {
		log.Log.Errorf("SyncAdminURL:resp.Message is not success,err:%s", resp.Message)
		return
	}
	log.Log.Infof("SyncAdminURL:sync admin url to leader success,leader admin url is:%s",
		attributes.AdminURLs[0])
}

//StartProposeBinlog start propose binlog event
func (s *KingbusServer) StartProposeBinlog(ctx context.Context) {
	go func() {
		for s.IsLeader() && s.IsSyncerStarted() {
			select {
			case <-ctx.Done():
				log.Log.Infof("StartProposeBinlog: receive cancel, stop propose binlog event")
				return
			case <-s.stopping:
				log.Log.Infof("kingbus server stopping")
				return
			case e := <-s.syncer.BinlogEventC():
				dataWithType, err := utils.EncodeBinlogEvent(e)
				if err != nil {
					select {
					case s.errorc <- err:
						log.Log.Errorf("EncodeBinlogEvent error,err:%s,event:%v", err, *e)
					default:
					}
					return
				}
				//get event after syncer ready, and propose the event to raft cluster.
				//if propose error,retry after 500ms
				//retry 1000 times
				s.ProposeWithRetry(ctx, dataWithType)
			case d := <-s.syncer.DataC():
				s.ProposeWithRetry(ctx, d)
			}
		}
	}()
}

// configure sends a configuration change through consensus and
// then waits for it to be applied to the server. It
// will block until the change is performed or there is an error.
func (s *KingbusServer) configure(ctx context.Context, cc raftpb.ConfChange) ([]*membership.Member, error) {
	cc.ID = s.reqIDGen.Next()
	ch := s.wait.Register(cc.ID)
	if err := s.raftNode.ProposeConfChange(ctx, cc); err != nil {
		s.wait.Trigger(cc.ID, nil)
		return nil, err
	}
	select {
	case x := <-ch:
		if x == nil {
			log.Log.Panicf("configure trigger value should never be nil")
		}
		resp := x.(*confChangeResponse)
		return resp.membs, resp.err
	case <-ctx.Done():
		s.wait.Trigger(cc.ID, nil) // GC wait
		return nil, ctx.Err()
	case <-s.stopping:
		return nil, errors.New("kingbus server stoping")
	}
}

//Propose data,only execute in lead node,follower node will be forbidden
func (s *KingbusServer) Propose(data []byte) error {
	if len(data) != 0 {
		return s.raftNode.Propose(context.Background(), data)
	}
	return nil
}

//ProposeWithRetry implements propose data with retry
func (s *KingbusServer) ProposeWithRetry(ctx context.Context, data []byte) {
	var retryCount int
	for retryCount < ProposeMaxRetryCount && s.IsLeader() {
		err := s.raftNode.Propose(ctx, data)
		if err != nil {
			log.Log.Errorf("Propose binlog event error,err:%s,data:%v,retryCount:%v", err, data, retryCount)
			retryCount++
			//send to kingbus server, this error will lead to this node stop
			if ProposeMaxRetryCount <= retryCount {
				select {
				case s.errorc <- err:
				default:
				}
				return
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return
	}
}

//AddMember member into raft cluster, only executed in lead node
func (s *KingbusServer) AddMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {
	if s.Cfg.RaftNodeCfg.StrictReconfigCheck {
		// by default StrictReconfigCheck is enabled; reject new members if unhealthy
		if !s.cluster.IsReadyToAddNewMember() {
			log.Log.Warnf("not enough started members, rejecting member add %+v", memb)
			return nil, ErrNotEnoughStartedMembers
		}
		if !isConnectedFullySince(s.raftNode.Transport, time.Now().Add(-HealthInterval), s.id, s.cluster.Members()) {
			log.Log.Warnf("not healthy for reconfigure, rejecting member add %+v", memb)
			return nil, ErrUnhealthy
		}
	}

	b, err := json.Marshal(memb)
	if err != nil {
		return nil, err
	}
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(memb.ID),
		Context: b,
	}
	return s.configure(ctx, cc)
}

//RemoveMember member from raft cluster, only executed in lead node
func (s *KingbusServer) RemoveMember(ctx context.Context, id uint64) ([]*membership.Member, error) {
	// by default StrictReconfigCheck is enabled; reject removal if leads to quorum loss
	if err := s.mayRemoveMember(types.ID(id)); err != nil {
		return nil, err
	}

	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	}
	return s.configure(ctx, cc)
}

func (s *KingbusServer) mayRemoveMember(id types.ID) error {
	if !s.Cfg.RaftNodeCfg.StrictReconfigCheck {
		return nil
	}

	if !s.cluster.IsReadyToRemoveMember(uint64(id)) {
		log.Log.Warnf("not enough started members, rejecting remove member %s", id)
		return ErrNotEnoughStartedMembers
	}

	// downed member is safe to remove since it's not part of the active quorum
	if t := s.raftNode.Transport.ActiveSince(id); id != s.id && t.IsZero() {
		return nil
	}

	// protect quorum if some members are down
	m := s.cluster.Members()
	active := numConnectedSince(s.raftNode.Transport, time.Now().Add(-HealthInterval), s.id, m)
	if (active - 1) < 1+((len(m)-1)/2) {
		log.Log.Warnf("reconfigure breaks active quorum, rejecting remove member %s", id)
		return ErrUnhealthy
	}

	return nil
}

//UpdateMember member in raft cluster, only executed in lead node
func (s *KingbusServer) UpdateMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error) {
	b, merr := json.Marshal(memb)
	if merr != nil {
		return nil, merr
	}

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeUpdateNode,
		NodeID:  uint64(memb.ID),
		Context: b,
	}
	return s.configure(ctx, cc)
}

//IsLeader return the node is lead
func (s *KingbusServer) IsLeader() bool {
	return s.ID() == s.Leader()
}

//ID get kingbus server id
func (s *KingbusServer) ID() types.ID { return s.id }

//Leader get raft cluster leader
func (s *KingbusServer) Leader() types.ID { return types.ID(s.getLead()) }

//CommittedIndex get committed index
func (s *KingbusServer) CommittedIndex() uint64 { return s.getCommittedIndex() }

//AppliedIndex get applied index
func (s *KingbusServer) AppliedIndex() uint64 { return s.getAppliedIndex() }

//Term get raft term
func (s *KingbusServer) Term() uint64 { return s.getTerm() }

func (s *KingbusServer) setCommittedIndex(v uint64) {
	s.raftNode.SetCommittedIndex(v)
}

func (s *KingbusServer) getCommittedIndex() uint64 {
	return s.raftNode.GetCommittedIndex()
}

func (s *KingbusServer) getAppliedIndex() uint64 {
	return s.raftNode.GetAppliedIndex()
}

func (s *KingbusServer) getTerm() uint64 {
	return s.raftNode.GetTerm()
}

func (s *KingbusServer) setLead(v uint64) {
	log.Log.Debugf("update Lead to %x", v)
	s.lead.Store(v)
}

func (s *KingbusServer) getLead() uint64 {
	return s.lead.Load()
}

//GetIP get ip of kingbus server
func (s *KingbusServer) GetIP() string {
	//s.adminSvr.adminAddr is ip:port
	strArray := strings.Split(s.adminSvr.AdminAddr, ":")
	return strArray[0]
}

//StartServer start sub servers:syncer server or binlog master server
func (s *KingbusServer) StartServer(svrType config.SubServerType, args interface{}) error {
	var err error
	switch svrType {
	case config.SyncerServerType:
		if s.IsSyncerStarted() {
			return ErrStarted
		}
		syncerArgs, ok := args.(*config.SyncerArgs)
		if !ok {
			log.Log.Errorf("StartServer args is illegal,args:%v", args)
			return ErrArgs
		}
		err = s.startSyncerServer(syncerArgs)
		if err != nil {
			log.Log.Errorf("startSyncerServer error,err:%s,args:%v", err, *syncerArgs)
			return ErrArgs
		}
		//start to propose binlog event to raft cluster
		s.StartProposeBinlog(s.syncer.ctx)
		log.Log.Debugf("start syncer,and propose!!!")
		return nil
	case config.BinlogServerType:
		if s.IsBinlogServerStarted() {
			return ErrStarted
		}
		masterArgs, ok := args.(*config.BinlogServerConfig)
		if !ok {
			log.Log.Errorf("StartServer args is illegal,args:%v", args)
			return ErrArgs
		}
		err = s.startMasterServer(masterArgs)
		if err != nil {
			log.Log.Errorf("startMasterServer error,err:%s,args:%v", err, *masterArgs)
			return ErrArgs
		}
		return nil
	default:
		log.Log.Fatalf("StartServer:server type not support,serverType:%v", svrType)
	}
	return nil
}

func (s *KingbusServer) startSyncerServer(args *config.SyncerArgs) error {
	//new syncer config
	syncerCfg, err := config.NewSyncerConfig(args)
	if err != nil {
		return err
	}
	syncer, err := NewSyncer(syncerCfg, s.store)
	if err != nil {
		log.Log.Errorf("NewSyncer error,err:%s,cfg:%v", err, *syncerCfg)
		return err
	}

	//wait for all binlog event applied, so the s.executedGtidSet is exact
	//If the appliedIndex is much smaller than committedIndex,
	//the time of apply cost large than election-timeout, other nodes may cause election again
	for s.getAppliedIndex() < s.getCommittedIndex() {
		log.Log.Debugf("slepp 100ms,appliedIndex:%d,committedIndex:%d", s.getAppliedIndex(), s.getCommittedIndex())
		time.Sleep(time.Millisecond * 100)
	}

	gtidPurged, err := syncer.getMasterGtidPurged()
	if err != nil {
		log.Log.Errorf("NewSyncer:getMasterGtidPurged error,err:%s", err)
		return err
	}
	//update executedGtidSet to lastest
	err = s.binlogProgress.reset(gtidPurged)
	if err != nil {
		log.Log.Errorf("NewSyncer:binlogProgress reset error,err:%s,gtidPurged:%s", err,gtidPurged)
		return err
	}

	err = syncer.proposeMasterGtidPurged(gtidPurged)
	if err != nil {
		log.Log.Errorf("NewSyncer:proposeMasterGtidPurged error,err:%s", err)
		return err
	}

	s.syncer = syncer

	//start syncer,must use ExecutedGtidSetClone,
	//since s.syncer will change gtidSet dynamically
	err = s.syncer.Start(s.binlogProgress.ExecutedGtidSetClone())
	if err != nil {
		log.Log.Errorf("syncer start error,err:%s", err)
		return err
	}
	log.Log.Infof("startSyncerServer success,args:%v", *args)
	return nil
}

func (s *KingbusServer) startMasterServer(args *config.BinlogServerConfig) error {
	master, err := NewBinlogServer(args, s, s.store, s.applyBroadcast)
	if err != nil {
		log.Log.Errorf("NewBinlogServer error,err:%s,args:%v", err, *args)
		return err
	}
	s.master = master
	s.master.Start()
	log.Log.Infof("startMasterServer success,args:%v", *args)
	return nil
}

//StopServer stop sub server
func (s *KingbusServer) StopServer(svrType config.SubServerType) {
	switch svrType {
	case config.SyncerServerType:
		if s.IsSyncerStarted() {
			s.syncer.Stop()
		}
	case config.BinlogServerType:
		if s.IsBinlogServerStarted() {
			s.master.Stop()
		}
	default:
		log.Log.Fatalf("StopServer:server type not support,serverType:%v", svrType)
	}
}

//GetServerStatus get the sub server status
func (s *KingbusServer) GetServerStatus(svrType config.SubServerType) interface{} {
	switch svrType {
	case config.SyncerServerType:
		var syncerStatus config.SyncerStatus
		if s.IsSyncerStarted() {
			cfg := s.syncer.cfg
			syncerStatus.MysqlAddr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
			syncerStatus.MysqlUser = cfg.User
			syncerStatus.MysqlPassword = cfg.Password
			syncerStatus.SemiSync = cfg.SemiSyncEnabled

			syncerStatus.Status = config.ServerRunningStatus
			syncerStatus.CurrentGtid = s.CurrentGtidStr()
			syncerStatus.LastBinlogFile = s.LastBinlogFile()
			syncerStatus.LastFilePosition = s.LastFilePosition()
			syncerStatus.ExecutedGtidSet = s.ExecutedGtidSetStr()

			purgedGtids, err := s.store.GetGtidSet("mysql", storage.GtidPurgedKey)
			if err != nil {
				log.Log.Fatalf("get PurgedGtidSet error,err:%s", err)
			}
			syncerStatus.PurgedGtidSet = purgedGtids.String()
		} else {
			syncerStatus.Status = config.ServerStoppedStatus
		}
		return &syncerStatus
	case config.BinlogServerType:
		var status config.BinlogServerStatus
		if s.IsBinlogServerStarted() {
			cfg := s.master.cfg
			status.Addr = cfg.Addr
			status.User = cfg.User
			status.Password = cfg.Password

			status.Slaves = make([]*mysql.Slave, 0, 2)
			slaves := s.master.GetSlaves()
			for _, s := range slaves {
				status.Slaves = append(status.Slaves, s)
			}
			status.CurrentGtid = s.CurrentGtidStr()
			status.LastBinlogFile = s.LastBinlogFile()
			status.LastFilePosition = s.LastFilePosition()
			status.ExecutedGtidSet = s.ExecutedGtidSetStr()

			purgedGtids, err := s.store.GetGtidSet("mysql", storage.GtidPurgedKey)
			if err != nil {
				log.Log.Fatalf("get PurgedGtidSet error,err:%s", err)
			}
			status.PurgedGtidSet = purgedGtids.String()
			status.Status = config.ServerRunningStatus
		} else {
			status.Status = config.ServerStoppedStatus
		}
		return &status
	default:
		log.Log.Fatalf("StopServer:server type not support,serverType:%v", svrType)
	}
	return nil
}

// Process takes a raft message and applies it to the server's raft state
// machine, respecting any timeout of the given context.
func (s *KingbusServer) Process(ctx context.Context, m raftpb.Message) error {
	if s.cluster.IsIDRemoved(types.ID(m.From)) {
		log.Log.Warnf("reject message from removed member %s", types.ID(m.From).String())
		return errors.New("cannot process message from removed member")
	}
	//todo metric
	//if m.Type == raftpb.MsgApp {
	//	s.stats.RecvAppendReq(types.ID(m.From).String(), m.Size())
	//}
	return s.raftNode.Step(ctx, m)
}

//IsIDRemoved return if the kingbus has been removed
func (s *KingbusServer) IsIDRemoved(id uint64) bool {
	return s.cluster.IsIDRemoved(types.ID(id))
}

//ReportUnreachable report unreachable
func (s *KingbusServer) ReportUnreachable(id uint64) {
	s.raftNode.ReportUnreachable(id)
}

//IsSyncerStarted return if syncer started
func (s *KingbusServer) IsSyncerStarted() bool {
	return s.syncer != nil && s.syncer.started.Load() == true
}

//IsBinlogServerStarted return if binlog server started
func (s *KingbusServer) IsBinlogServerStarted() bool {
	return s.master != nil && s.master.started.Load() == true
}

// ReportSnapshot reports snapshot sent status to the raft state machine,
// and clears the used snapshot from the snapshot store.
func (s *KingbusServer) ReportSnapshot(id uint64, status etcdraft.SnapshotStatus) {
	s.raftNode.ReportSnapshot(id, status)
}

func (s *KingbusServer) newBinlogProgress() error {
	p, err := newBinlogProgress(s.store)
	if err != nil {
		return err
	}
	s.binlogProgress = p
	return nil
}

//LastBinlogFile return last binlog file
func (s *KingbusServer) LastBinlogFile() string {
	return s.binlogProgress.LastBinlogFile()
}

//LastFilePosition return last binlog file position
func (s *KingbusServer) LastFilePosition() uint32 {
	return s.binlogProgress.LastFilePosition()
}

//ExecutedGtidSetStr return executed gtid
func (s *KingbusServer) ExecutedGtidSetStr() string {
	return s.binlogProgress.ExecutedGtidSetStr()
}

//CurrentGtidStr return current gtid
func (s *KingbusServer) CurrentGtidStr() string {
	return s.binlogProgress.CurrentGtidStr()
}
