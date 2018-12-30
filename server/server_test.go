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
	"bytes"
	"io/ioutil"
	"math/rand"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"go.uber.org/atomic"

	"github.com/flike/kingbus/mysql"

	"time"

	"fmt"
	"os"
	"path"

	"encoding/json"

	"github.com/coreos/etcd/pkg/types"
	"github.com/flike/kingbus/config"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/storage"
	"github.com/flike/kingbus/utils"
	"github.com/satori/go.uuid"
)

const letterArray = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

const (
	nodeNamePrefix = "node_"
	TestDataDir    = "/tmp/kingbus/data"
	TestSyncerDir  = "/tmp/kingbus/syncer"
	TempLogDir     = "/tmp/kingbus/log"
)

func TestSyncAdminURL(t *testing.T) {
	var httpResp utils.Resp
	cluster := StartClusterInTest()
	defer StopClusterInTest(cluster)

	require.Equal(t, 3, len(cluster))
	time.Sleep(time.Second)

	resp, err := http.Get("http://127.0.0.1:9591/members")
	require.Nil(t, err)

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	require.Nil(t, err)

	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	err = d.Decode(&httpResp)
	require.Nil(t, err)

	require.Equal(t, "success", httpResp.Message)

	members, ok := httpResp.Data.([]interface{})
	require.Equal(t, true, ok)
	for i := 0; i < len(members); i++ {
		member, ok := members[i].(map[string]interface{})
		require.Equal(t, true, ok)
		t.Logf("Id:%s,peerURLs:%s,name:%s,adminURLs:%s", member["Id"],
			member["peerURLs"], member["name"], member["adminURLs"])
	}
}

//start with no file and join cluster
func initEnv() {
	err := os.RemoveAll(TestDataDir)
	if err != nil {
		panic(err)
	}
}

func newRaftNodeConfigInTest(id int) config.RaftNodeConfig {
	peerURL, err := types.NewURLs([]string{fmt.Sprintf("http://127.0.0.1:1500%d", id)})
	if err != nil {
		panic(err)
	}
	initialPeerURLsMap, _ := types.NewURLsMap(
		"node_1=http://127.0.0.1:15001," +
			"node_2=http://127.0.0.1:15002," +
			"node_3=http://127.0.0.1:15003")
	nodeName := fmt.Sprintf("%s%d", nodeNamePrefix, id)

	cfg := config.RaftNodeConfig{
		Name:                nodeName,
		AppliedIndex:        0,
		PeerURLs:            peerURL,
		InitialPeerURLsMap:  initialPeerURLsMap,
		MaxRequestBytes:     1048576,
		HeartbeatMs:         time.Millisecond * 200,
		ElectionTimeoutMs:   time.Second * 2,
		NewCluster:          true,
		PreVote:             true,
		DataDir:             path.Join(TestDataDir, nodeName),
		ReserveDataSize:     4, //4GB
		StrictReconfigCheck: true,
	}
	return cfg
}

func StartClusterInTest() []*KingbusServer {
	initEnv()

	log.InitLoggers(TempLogDir, "DEBUG")
	cluster := make([]*KingbusServer, 0, 3)
	for i := 1; i <= 3; i++ {
		s := StartKingbusServerNodeInTest(i)
		cluster = append(cluster, s)
	}

	time.Sleep(time.Second * 10)

	leaderID := cluster[0].getLead()
	if leaderID == 0 {
		panic("no leader")
	}
	//get leader and follower
	for _, svr := range cluster {
		if uint64(svr.id) == leaderID {
			leader = svr
		} else {
			follower = svr
		}
	}

	return cluster
}

func StartKingbusServerNodeInTest(id int) *KingbusServer {
	s := new(KingbusServer)
	s.Cfg = new(config.KingbusServerConfig)
	s.lead = atomic.NewUint64(0)
	raftNodecfg := newRaftNodeConfigInTest(id)
	s.Cfg.RaftNodeCfg = raftNodecfg

	adminAddr := fmt.Sprintf("http://127.0.0.1:959%d", id)
	adminURLs, err := types.NewURLs([]string{adminAddr})
	if err != nil {
		panic(err)
	}
	s.Cfg.AdminURLs = adminURLs

	err = s.starRaft(raftNodecfg)
	if err != nil {
		panic(err)
	}
	err = s.startRaftPeer(raftNodecfg.PeerURLs)
	if err != nil {
		panic(err)
	}
	err = s.newBinlogProgress()
	if err != nil {
		panic(err)
	}
	s.startAdminServer(adminURLs)
	metricsAddr := fmt.Sprintf("127.0.0.1:272%d", id)
	err = s.startPrometheus(metricsAddr)
	if err != nil {
		panic(err)
	}

	go s.runRaft()
	go s.adminSvr.Run()
	go s.SyncAdminURL()
	//go s.prometheusSvr.Run()
	s.started = atomic.NewBool(true)

	err = saveMasterInfoForTest(s.store, id)
	if err != nil {
		panic(err)
	}

	return s
}

func saveMasterInfoForTest(store storage.Storage, id int) error {
	masterInfo := mysql.MasterInfo{
		ServerID:   int32(id),
		ServerUUID: uuid.NewV4().String(),
		Version:    "5.7.23-log",
	}

	buf, err := json.Marshal(masterInfo)
	if err != nil {
		return err
	}
	key := utils.StringToBytes(storage.MasterInfoKey)
	err = store.Set(key, buf)
	if err != nil {
		return err
	}
	return nil
}

func StopKingbusServerNodeInTest(s *KingbusServer) {
	if s.started.Load() == false {
		return
	}
	s.raftNode.Stop()
	s.adminSvr.Stop()
	s.StopServer(config.SyncerServerType)
	s.StopServer(config.BinlogServerType)
}

func StopClusterInTest(cluster []*KingbusServer) {
	for _, s := range cluster {
		StopKingbusServerNodeInTest(s)
	}
	log.UnInitLoggers()
}

func NewMessagesInTest(recordCount int) []string {
	msgs := make([]string, 0)

	for i := 0; i < recordCount; i++ {
		msgString := randStringBytesInTest(rand.Intn(1024))
		msgs = append(msgs, msgString)
	}
	return msgs
}

// new size = n random string
func randStringBytesInTest(n int) string {
	if n <= 0 {
		n = 1
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = letterArray[rand.Intn(len(letterArray))]
	}
	return string(b)
}
