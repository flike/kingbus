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

package api

import (
	"context"

	"github.com/flike/kingbus/config"

	"github.com/flike/kingbus/utils"

	"time"

	"net/http"

	"fmt"

	"net/url"

	"encoding/json"

	"github.com/coreos/etcd/pkg/types"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/raft/membership"
	"github.com/labstack/echo"
)

//MembershipHandler is used for handling the api call of membership in raft cluster
type MembershipHandler struct {
	svr     Server
	cluster Cluster
	timeout time.Duration
}

// Cluster is responsible for a collection of members in one etcd cluster.
type Cluster interface {
	// GetID returns the cluster id
	GetID() types.ID
	// Members returns a slice of members sorted by their id
	Members() []*membership.Member
	// Member retrieves a particular member based on id, or nil if the
	// member does not exist in the cluster
	Member(id types.ID) *membership.Member

	MemberByName(name string) *membership.Member
}

//Role represents a node in cluster
type Role struct {
	ID string `json:"Id"`
	membership.RaftAttributes
	membership.Attributes
	IsLeader bool `json:"isLeader"`
}

//GetCluster implements get information of raft cluster
func (h *MembershipHandler) GetCluster(echoCtx echo.Context) error {
	members := h.cluster.Members()
	roles := make([]*Role, 0, len(members))
	for _, m := range members {
		r := new(Role)
		r.ID = fmt.Sprintf("%x", uint64(m.ID))
		r.RaftAttributes = m.RaftAttributes
		r.Attributes = m.Attributes
		if uint64(m.ID) == uint64(h.svr.Leader()) {
			r.IsLeader = true
		} else {
			r.IsLeader = false
		}
		roles = append(roles, r)
	}
	return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(roles))
}

//GetMembers implements get information of membership, not include lead information
func (h *MembershipHandler) GetMembers(echoCtx echo.Context) error {
	members := h.cluster.Members()
	return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(members))
}

//UpdateAdminURL implements update raft node admin url in raft cluster
func (h *MembershipHandler) UpdateAdminURL(echoCtx echo.Context) error {
	var attributes config.Attributes
	if h.svr.IsLeader() == false {
		return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(ErrNotLeader.Error()))
	}
	err := echoCtx.Bind(&attributes)
	if err != nil {
		return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
	}
	data, err := attributes.EncodeWithType()
	if err != nil {
		log.Log.Errorf("attributes EncodeWithType error,err:%v,attributes:%v", err, attributes)
		return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
	}

	err = h.svr.Propose(data)
	if err != nil {
		log.Log.Errorf("Propose attributes error,err:%s,attributes:%v", err, attributes)
		return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
	}
	return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(""))
}

//AddMember implements add a member into raft cluster
func (h *MembershipHandler) AddMember(echoCtx echo.Context) error {
	args := struct {
		NodeName string `json:"name"`
		PeerURL  string `json:"peer_url"`
		AdminURL string `json:"admin_url"`
	}{}
	err := echoCtx.Bind(&args)
	if err != nil {
		return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
	}

	isLeader := h.svr.IsLeader()
	if isLeader == false {
		req, err := json.Marshal(args)
		if err != nil {
			return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
		}
		resp, err := h.sendToLeader("POST", req)
		if err != nil {
			return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
		}
		return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(resp.Data))
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	peerURLs, err := types.NewURLs([]string{args.PeerURL})
	if err != nil {
		return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
	}
	adminURLs, err := types.NewURLs([]string{args.AdminURL})
	if err != nil {
		return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
	}
	now := time.Now()

	member := membership.NewMember(args.NodeName, peerURLs, adminURLs, &(now))
	members, err := h.svr.AddMember(ctx, *member)
	switch {
	case err == membership.ErrIDExists || err == membership.ErrPeerURLexists:
		return echoCtx.JSON(http.StatusConflict, utils.NewResp().SetError(err.Error()))
	case err != nil:
		log.Log.Errorf("error adding member %s (%v)", member.ID, err)
		return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
	}

	return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(members))
}

//UpdateMember implements update member information
func (h *MembershipHandler) UpdateMember(echoCtx echo.Context) error {
	args := struct {
		NodeName   string `json:"name"`
		NewPeerURL string `json:"new_peer_url"`
	}{}
	err := echoCtx.Bind(&args)
	if err != nil {
		return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
	}

	isLeader := h.svr.IsLeader()
	if isLeader == false {
		req, err := json.Marshal(args)
		if err != nil {
			return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
		}
		resp, err := h.sendToLeader("PUT", req)
		if err != nil {
			return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
		}
		return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(resp.Data))
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	m := h.cluster.MemberByName(args.NodeName)
	if m == nil {
		err = fmt.Errorf("no such member: %s", args.NodeName)
		return echoCtx.JSON(http.StatusNotFound, utils.NewResp().SetError(err.Error()))
	}

	newMember := membership.Member{
		ID:             m.ID,
		RaftAttributes: membership.RaftAttributes{PeerURLs: []string{args.NewPeerURL}},
	}
	members, err := h.svr.UpdateMember(ctx, newMember)
	switch {
	case err == membership.ErrPeerURLexists:
		return echoCtx.JSON(http.StatusConflict, utils.NewResp().SetError(err.Error()))
	case err == membership.ErrIDNotFound:
		err = fmt.Errorf("no such member: %s", args.NodeName)
		return echoCtx.JSON(http.StatusNotFound, utils.NewResp().SetError(err.Error()))
	case err != nil:
		log.Log.Errorf("error updating member %s (%v)", m.ID, err)
		echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
	default:
		return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(members))
	}
	return nil
}

//DeleteMember implements remove a member from raft cluster
func (h *MembershipHandler) DeleteMember(echoCtx echo.Context) error {
	args := struct {
		NodeName string `json:"name"`
		PeerURL  string `json:"peer_url"`
	}{}
	err := echoCtx.Bind(&args)
	if err != nil {
		return echoCtx.JSON(http.StatusForbidden, err.Error())
	}

	isLeader := h.svr.IsLeader()
	if isLeader == false {
		req, err := json.Marshal(args)
		if err != nil {
			return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
		}
		resp, err := h.sendToLeader("DELETE", req)
		if err != nil {
			return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
		}
		return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(resp.Data))
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	m := h.cluster.MemberByName(args.NodeName)
	if m == nil {
		msg := fmt.Sprintf("No such member: %s", args.NodeName)
		return echoCtx.JSON(http.StatusNotFound, utils.NewResp().SetError(msg))
	}

	log.Log.Debugf("DeleteMember:remove member id is %s", m.ID.String())

	members, err := h.svr.RemoveMember(ctx, uint64(m.ID))
	switch {
	case err == membership.ErrIDRemoved:
		msg := fmt.Sprintf("Member permanently removed: %s", args.NodeName)
		return echoCtx.JSON(http.StatusGone, utils.NewResp().SetError(msg))
	case err == membership.ErrIDNotFound:
		msg := fmt.Sprintf("No such member: %s", args.NodeName)
		return echoCtx.JSON(http.StatusNotFound, utils.NewResp().SetError(msg))
	case err != nil:
		log.Log.Errorf("error removing member %s (%v)", args.NodeName, err)
		return echoCtx.JSON(http.StatusInternalServerError, utils.NewResp().SetError(err.Error()))
	}
	return echoCtx.JSON(http.StatusOK, utils.NewResp().SetData(members))
}

func (h *MembershipHandler) sendToLeader(method string, req []byte) (*utils.Resp, error) {
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
	url := leaderURL.Scheme + "://" + leaderURL.Host + "/members"

	resp, err := utils.SendRequest(method, url, req)
	if err != nil {
		log.Log.Errorf("sendToLeader:SendRequest error,err:%s,url:%s", err, url)
		return nil, err
	}

	return resp, nil
}
