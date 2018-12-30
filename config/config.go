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

package config

import (
	"context"
	"time"

	"errors"

	"fmt"

	"sort"
	"strings"

	"github.com/coreos/etcd/pkg/netutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/siddontang/go-mysql/replication"
	"github.com/spf13/viper"
)

const (
	//ClusterStateFlagNew used to indicate that the cluster is new
	ClusterStateFlagNew = "new"
	//ClusterStateFlagExisting used to indicate that the cluster is exist
	ClusterStateFlagExisting = "existing"
)

const (
	//MaxRequestBytes is max size of request transfer in raft cluster, 20MB
	MaxRequestBytes uint64 = 20 * 1024 * 1024
)

//KingbusServerConfig is the config of kingbus server
type KingbusServerConfig struct {
	RaftNodeCfg RaftNodeConfig
	AdminURLs   types.URLs
	MetricsAddr string
	LogDir      string
	LogLevel    string
}

//SyncerConfig is the config of syncer in kingbus
type SyncerConfig struct {
	replication.BinlogSyncerConfig
	MaxEventBytes uint64
}

//RaftNodeConfig is the config of raft node
type RaftNodeConfig struct {
	Name                string
	AppliedIndex        uint64
	PeerURLs            types.URLs
	InitialPeerURLsMap  types.URLsMap
	MaxRequestBytes     uint64
	HeartbeatMs         time.Duration
	ElectionTimeoutMs   time.Duration
	NewCluster          bool
	PreVote             bool
	DataDir             string
	ReserveDataSize     int
	StrictReconfigCheck bool
}

//NewKingbusServerConfig implements create a config of kingbus server
func NewKingbusServerConfig(configPath string) (*KingbusServerConfig, error) {
	if configPath == "" {
		return nil, errors.New("config path is nil")

	}
	viper.SetConfigFile(configPath)
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	//get raft node config
	raftNodeCfg, err := getRaftNodeConfig()
	if err != nil {
		return nil, err
	}

	adminURLs, err := types.NewURLs([]string{viper.GetString("admin-url")})
	if err != nil {
		return nil, err
	}
	if len(adminURLs) != 1 {
		return nil, fmt.Errorf("adminURLs muster be 1")
	}

	ksCfg := &KingbusServerConfig{
		RaftNodeCfg: *raftNodeCfg,
		AdminURLs:   adminURLs,
		MetricsAddr: viper.GetString("metrics-addr"),
		LogDir:      viper.GetString("log-dir"),
		LogLevel:    viper.GetString("log-level"),
	}

	return ksCfg, nil
}

func getRaftNodeConfig() (*RaftNodeConfig, error) {
	peerURLs, err := types.NewURLs([]string{viper.GetString("peer-url")})
	if err != nil {
		return nil, err
	}

	initialPeerURLsMap, err := types.NewURLsMap(viper.GetString("initial-cluster"))
	if err != nil {
		return nil, err
	}

	clusterStatus := viper.GetString("initial-cluster-state")

	raftNodeCfg := &RaftNodeConfig{
		Name:                viper.GetString("name"),
		PeerURLs:            peerURLs,
		InitialPeerURLsMap:  initialPeerURLsMap,
		MaxRequestBytes:     MaxRequestBytes,
		HeartbeatMs:         viper.GetDuration("heartbeat-interval"),
		ElectionTimeoutMs:   viper.GetDuration("election-timeout"),
		PreVote:             true,
		DataDir:             viper.GetString("data-dir"),
		ReserveDataSize:     viper.GetInt("reserve-data-size"),
		StrictReconfigCheck: true,
		NewCluster:          clusterStatus == ClusterStateFlagNew,
	}
	return raftNodeCfg, nil
}

// VerifyBootstrap sanity-checks the initial config for bootstrap case
// and returns an error for things that should never happen.
func (c *RaftNodeConfig) VerifyBootstrap() error {
	if err := c.hasLocalMember(); err != nil {
		return err
	}
	if err := c.advertiseMatchesCluster(); err != nil {
		return err
	}
	if checkDuplicateURL(c.InitialPeerURLsMap) {
		return fmt.Errorf("initial cluster %s has duplicate url", c.InitialPeerURLsMap)
	}
	if c.InitialPeerURLsMap.String() == "" {
		return fmt.Errorf("initial cluster unset and no discovery URL found")
	}
	if c.ReserveDataSize < 4 {
		return fmt.Errorf("reserve data size is too small")
	}
	return nil
}

// hasLocalMember checks that the cluster at least contains the local server.
func (c *RaftNodeConfig) hasLocalMember() error {
	if urls := c.InitialPeerURLsMap[c.Name]; urls == nil {
		return fmt.Errorf("couldn't find local name %q in the initial cluster configuration", c.Name)
	}
	return nil
}

// advertiseMatchesCluster confirms peer URLs match those in the cluster peer list.
func (c *RaftNodeConfig) advertiseMatchesCluster() error {
	urls, apurls := c.InitialPeerURLsMap[c.Name], c.PeerURLs.StringSlice()
	urls.Sort()
	sort.Strings(apurls)
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	ok, err := netutil.URLStringsEqual(ctx, apurls, urls.StringSlice())
	if ok {
		return nil
	}

	initMap, apMap := make(map[string]struct{}), make(map[string]struct{})
	for _, url := range c.PeerURLs {
		apMap[url.String()] = struct{}{}
	}
	for _, url := range c.InitialPeerURLsMap[c.Name] {
		initMap[url.String()] = struct{}{}
	}

	missing := []string{}
	for url := range initMap {
		if _, ok := apMap[url]; !ok {
			missing = append(missing, url)
		}
	}
	if len(missing) > 0 {
		for i := range missing {
			missing[i] = c.Name + "=" + missing[i]
		}
		mstr := strings.Join(missing, ",")
		apStr := strings.Join(apurls, ",")
		return fmt.Errorf("--initial-cluster has %s but missing from --peer-urls=%s (%v)", mstr, apStr, err)
	}

	for url := range apMap {
		if _, ok := initMap[url]; !ok {
			missing = append(missing, url)
		}
	}
	if len(missing) > 0 {
		mstr := strings.Join(missing, ",")
		umap := types.URLsMap(map[string]types.URLs{c.Name: c.PeerURLs})
		return fmt.Errorf("--peer-urls has %s but missing from --initial-cluster=%s", mstr, umap.String())
	}

	// resolved URLs from "--peer-urls" and "--initial-cluster" did not match or failed
	apStr := strings.Join(apurls, ",")
	umap := types.URLsMap(map[string]types.URLs{c.Name: c.PeerURLs})
	return fmt.Errorf("failed to resolve %s to match --initial-cluster=%s (%v)", apStr, umap.String(), err)
}

func checkDuplicateURL(urlsmap types.URLsMap) bool {
	um := make(map[string]bool)
	for _, urls := range urlsmap {
		for _, url := range urls {
			u := url.String()
			if um[u] {
				return true
			}
			um[u] = true
		}
	}
	return false
}

// VerifyJoinExisting sanity-checks the initial config for join existing cluster
// case and returns an error for things that should never happen.
func (c *RaftNodeConfig) VerifyJoinExisting() error {
	// The member has announced its peer urls to the cluster before starting; no need to
	// set the configuration again.
	if err := c.hasLocalMember(); err != nil {
		return err
	}
	if checkDuplicateURL(c.InitialPeerURLsMap) {
		return fmt.Errorf("initial cluster %s has duplicate url", c.InitialPeerURLsMap)
	}
	return nil
}
