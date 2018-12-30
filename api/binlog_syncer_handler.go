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

package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sync"

	"github.com/flike/kingbus/config"

	"github.com/flike/kingbus/utils"

	"github.com/flike/kingbus/log"

	"github.com/labstack/echo"
)

//BinlogSyncerHandler is used handling the api call of binlog syncer.
//handler with lock will keep calling api with no compete
type BinlogSyncerHandler struct {
	l       sync.Mutex
	svr     Server
	cluster Cluster
}

//StartBinlogSyncer implements start a binlog syncer
func (h *BinlogSyncerHandler) StartBinlogSyncer(echoCtx echo.Context) error {
	h.l.Lock()
	defer h.l.Unlock()

	var args config.SyncerArgs
	var err error
	var syncerID int

	defer func() {
		if err != nil {
			log.Log.Errorf("StartBinlogSyncer error,err: %s", err)
			echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
		}
	}()

	err = echoCtx.Bind(&args)
	if err != nil {
		return err
	}
	//check args
	err = args.Check()
	if err != nil {
		return err
	}

	//forward to leader
	if h.svr.IsLeader() == false {
		req, err := json.Marshal(args)
		if err != nil {
			return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
		}
		resp, err := h.sendToLeader("PUT", "/binlog/syncer/start", req)
		if err != nil {
			log.Log.Errorf("sendToLeader error,err:%s,args:%v", err, args)
			return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
		}
		if resp.Message != "success" {
			return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(resp.Message))
		}
		return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(resp.Data))
	}

	//start syncer server
	err = h.svr.StartServer(config.SyncerServerType, &args)
	if err != nil {
		log.Log.Errorf("StartServer error,err:%s,args:%v", err, args)
		return err
	}

	//propose start syncer info
	err = h.ProposeSyncerArgs(&args)
	if err != nil {
		log.Log.Errorf("ProposeSyncerArgs error,err:%s,args:%v", err, args)
		return err
	}

	return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(syncerID))
}

//sendToLeader implements forward request to leader in raft cluster
func (h *BinlogSyncerHandler) sendToLeader(method string, uri string, req []byte) (*utils.Resp, error) {
	leaderID := h.svr.Leader()
	leader := h.cluster.Member(leaderID)
	if leader == nil {
		return nil, ErrNoLeader
	}
	if len(leader.AdminURLs) != 1 {
		log.Log.Errorf("leader admin url is not 1,leader:%v", *leader)
		return nil, ErrNoLeader
	}
	leaderURL, err := url.Parse(leader.AdminURLs[0])
	if err != nil {
		return nil, err
	}
	reqURL := leaderURL.Scheme + "://" + leaderURL.Host + uri

	log.Log.Debugf("sendToLeader:reqURL is:%s", reqURL)

	resp, err := utils.SendRequest(method, reqURL, req)
	if err != nil {
		log.Log.Errorf("sendToLeader:SendRequest error,err:%s,url:%s", err, reqURL)
		return nil, err
	}

	return resp, nil
}

//StopBinlogSyncer implements stop binlog syncer
func (h *BinlogSyncerHandler) StopBinlogSyncer(echoCtx echo.Context) error {
	h.l.Lock()
	defer h.l.Unlock()

	//forward to leader
	if h.svr.IsLeader() == false {
		resp, err := h.sendToLeader("PUT", "/binlog/syncer/stop", nil)
		if err != nil {
			log.Log.Errorf("sendToLeader error,err:%s", err)
			return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
		}
		if resp.Message != "success" {
			return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(resp.Message))
		}
		return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(resp.Data))
	}

	h.svr.StopServer(config.SyncerServerType)
	return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(""))
}

//GetBinlogSyncerStatus implements get binlog syncer status in the runtime state
func (h *BinlogSyncerHandler) GetBinlogSyncerStatus(echoCtx echo.Context) error {
	h.l.Lock()
	defer h.l.Unlock()

	//forward to leader
	if h.svr.IsLeader() == false {
		resp, err := h.sendToLeader("GET", "/binlog/syncer/status", nil)
		if err != nil {
			log.Log.Errorf("sendToLeader error,err:%s", err)
			return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
		}
		if resp.Message != "success" {
			return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(resp.Message))
		}
		return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(resp.Data))
	}

	status := h.svr.GetServerStatus(config.SyncerServerType)
	if syncerStatus, ok := status.(*config.SyncerStatus); ok {
		return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(syncerStatus))
	}
	return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError("no resp"))
}

//ProposeSyncerArgs implements propose start syncer args to raft cluster
//when other node became to lead, it can use this information start syncer automatically
func (h *BinlogSyncerHandler) ProposeSyncerArgs(args *config.SyncerArgs) error {
	data, err := args.EncodeWithType()
	if err != nil {
		return err
	}
	err = h.svr.Propose(data)
	if err != nil {
		return err
	}
	return nil
}
