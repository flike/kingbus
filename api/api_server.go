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
	"time"

	"github.com/coreos/etcd/pkg/types"
	"github.com/flike/kingbus/config"
	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/raft/membership"
	"github.com/labstack/echo"
	mw "github.com/labstack/echo/middleware"
)

const (
	adminAPITimeout = 10 * time.Second
)

//AdminServer is a server for handling api call
type AdminServer struct {
	AdminAddr string
	web       *echo.Echo
	mh        *MembershipHandler
	bs        *BinlogSyncerHandler
	bm        *BinlogServerHandler
}

//Server is a kingbus server interface
type Server interface {
	// AddMember attempts to add a member into the cluster. It will return
	// ErrIDRemoved if member id is removed from the cluster, or return
	// ErrIDExists if member id exists in the cluster.
	AddMember(ctx context.Context, memb membership.Member) ([]*membership.Member, error)
	// RemoveMember attempts to remove a member from the cluster. It will
	// return ErrIDRemoved if member id is removed from the cluster, or return
	// ErrIDNotFound if member id is not in the cluster.
	RemoveMember(ctx context.Context, id uint64) ([]*membership.Member, error)
	// UpdateMember attempts to update an existing member in the cluster. It will
	// return ErrIDNotFound if the member id does not exist.
	UpdateMember(ctx context.Context, updateMemb membership.Member) ([]*membership.Member, error)
	GetIP() string
	IsLeader() bool
	Leader() types.ID

	Propose(data []byte) error
	StartServer(svrType config.SubServerType, args interface{}) error
	StopServer(svrType config.SubServerType)
	GetServerStatus(svrType config.SubServerType) interface{}
}

//NewAdminServer creates a admin server
func NewAdminServer(addr string, svr Server, cluster Cluster) *AdminServer {
	adminServer := new(AdminServer)

	adminServer.AdminAddr = addr
	adminServer.web = echo.New()
	adminServer.web.HideBanner = true
	adminServer.web.HidePort = true

	adminServer.mh = &MembershipHandler{
		svr:     svr,
		cluster: cluster,
		timeout: adminAPITimeout,
	}
	adminServer.bs = &BinlogSyncerHandler{
		svr:     svr,
		cluster: cluster,
	}
	adminServer.bm = &BinlogServerHandler{
		svr: svr,
	}
	return adminServer
}

//Run a AdminServer
func (s *AdminServer) Run() {
	s.RegisterMiddleware()
	s.RegisterURL()
	err := s.web.Start(s.AdminAddr)
	if err != nil {
		log.Log.Infof("admin server start error,err:%s", err)
	}
}

//RegisterMiddleware implements register middleware in web
func (s *AdminServer) RegisterMiddleware() {
	loggerConfig := mw.LoggerConfig{
		Skipper: mw.DefaultSkipper,
		Format: `{"time":"${time_rfc3339_nano}","id":"${id}","remote_ip":"${remote_ip}","host":"${host}",` +
			`"method":"${method}","uri":"${uri}","status":${status}, "latency":${latency},` +
			`"latency_human":"${latency_human}","bytes_in":${bytes_in},` +
			`"bytes_out":${bytes_out}}` + "\n",
		CustomTimeFormat: "2006-01-02 15:04:05.00000",
		Output:           log.NewWriter(),
	}
	s.web.Use(mw.LoggerWithConfig(loggerConfig))
	s.web.Use(mw.Recover())
}

//RegisterURL implements url binding
func (s *AdminServer) RegisterURL() {
	//member handler
	s.web.GET("/members", s.mh.GetMembers)
	s.web.POST("/members", s.mh.AddMember)
	s.web.PUT("/members", s.mh.UpdateMember)
	s.web.DELETE("/members", s.mh.DeleteMember)
	s.web.GET("/cluster", s.mh.GetCluster)
	s.web.PUT("/admin/url", s.mh.UpdateAdminURL)

	//binlog syncer handler
	s.web.PUT("/binlog/syncer/start", s.bs.StartBinlogSyncer)
	s.web.PUT("/binlog/syncer/stop", s.bs.StopBinlogSyncer)
	s.web.GET("/binlog/syncer/status", s.bs.GetBinlogSyncerStatus)

	//binlog server handler
	s.web.PUT("/binlog/server/start", s.bm.StartBinlogServer)
	s.web.PUT("/binlog/server/stop", s.bm.StopBinlogServer)
	s.web.GET("/binlog/server/status", s.bm.GetBinlogServerStatus)
}

//Stop AdminServer
func (s *AdminServer) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.web.Shutdown(ctx); err != nil {
		log.Log.Fatalf("adminServer Shutdown error:%s", err.Error())
	}
}
