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
	"context"
	"io/ioutil"
	stdlog "log"
	"net"
	"net/http"
	"time"

	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/rafthttp"
	"github.com/flike/kingbus/log"
)

type peerListener struct {
	net.Listener
	serve func() error
	close func(context.Context) error
}

//SetPeerHandler set the handler of raft transport
func (r *Node) SetPeerHandler() {
	mux := http.NewServeMux()
	// configure peer handlers after rafthttp.Transport started
	mux.HandleFunc("/", http.NotFound)
	mux.Handle(rafthttp.RaftPrefix, r.Transport.Handler())
	mux.Handle(rafthttp.RaftPrefix+"/", r.Transport.Handler())

	for i := range r.PeerListener {
		srv := &http.Server{
			Handler:     mux,
			ReadTimeout: 5 * time.Minute,
			ErrorLog:    stdlog.New(ioutil.Discard, "", 0),
		}
		r.PeerListener[i].serve = func() error {
			return srv.Serve(r.PeerListener[i].Listener)
		}
		r.PeerListener[i].close = func(ctx context.Context) error {
			return srv.Shutdown(ctx)
		}
	}
}

//NewPeerListener create listener of peer
func (r *Node) NewPeerListener(peerURLs types.URLs) (err error) {
	peers := make([]*peerListener, len(peerURLs))
	defer func() {
		if err == nil {
			return
		}
		for i := range peers {
			if peers[i] != nil && peers[i].close != nil {
				log.Log.Info("stopping listening for peers on ", peerURLs.String())
				peers[i].close(context.Background())
			}
		}
	}()

	for i, u := range peerURLs {
		peers[i] = &peerListener{close: func(context.Context) error { return nil }}
		peers[i].Listener, err = rafthttp.NewListener(u, nil)
		if err != nil {
			return err
		}
		peers[i].close = func(context.Context) error {
			return peers[i].Listener.Close()
		}
		log.Log.Info("listening for peers on ", u.String())
	}
	r.PeerListener = peers
	return nil
}

//PeerHandlerServe serve
func (r *Node) PeerHandlerServe() {
	for _, peer := range r.PeerListener {
		go peer.serve()
	}
}

//StopPeerListener stop the listener of peer
func (r *Node) StopPeerListener() error {
	for _, peerListener := range r.PeerListener {
		err := peerListener.close(context.Background())
		if err != nil {
			log.Log.Errorf("close peer listener [%s] error",
				peerListener.Addr().String())
			return err
		}
	}
	return nil
}
