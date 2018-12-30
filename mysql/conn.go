// Copyright 2014 The go-mysql Authors. All rights reserved.
// Use of this source code is governed by a MIT License
// that can be found in the LICENSES/go-mysql-LICENSE file.

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

package mysql

import (
	"context"
	"net"

	"runtime"

	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/storage/storagepb"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"go.uber.org/atomic"
)

var baseConnID = atomic.NewUint32(10000)

//Conn acts like a MySQL server connection,
//you can use MySQL client to communicate with it.
type Conn struct {
	*BaseConn
	capability   uint32
	connectionID uint32
	status       uint16
	user         string
	salt         []byte
	closed       *atomic.Bool
	ctx          context.Context
	cancel       context.CancelFunc
	//user variable
	userVariables map[string]interface{}
	binlogServer  BinlogServer
}

//BinlogServer implements binlog master functions used in replication
type BinlogServer interface {
	RegisterSlave(slave *Slave) error
	UnregisterSlave(uuid string)
	GetSlaves() map[string]*Slave

	DumpBinlogAt(ctx context.Context,
		startRaftIndex uint64, slaveGtids *gomysql.MysqlGTIDSet,
		eventC chan<- *storagepb.BinlogEvent, errorC chan<- error) error

	CheckGtidSet(flavor string, slaveExecutedGtidSet gomysql.GTIDSet) error
	GetMySQLDumpAt(slaveExecutedGtids *gomysql.MysqlGTIDSet) (uint64, error)
	GetFde(raftIndex uint64) ([]byte, error)
	GetNextBinlogFile(startRaftIndex uint64) (string, error)
	GetMasterInfo() (*MasterInfo, error)
	GetGtidSet(flavor string, key string) (gomysql.GTIDSet, error)

	LastBinlogFile() string
	LastFilePosition() uint32
}

//NewConn create a Conn
func NewConn(conn net.Conn, s BinlogServer, user string, password string) (*Conn, error) {
	c := new(Conn)

	c.user = user
	c.BaseConn = NewBaseConn(conn)
	c.connectionID = baseConnID.Add(1)
	c.salt, _ = gomysql.RandomBuf(20)
	c.closed = atomic.NewBool(false)
	masterInfo, err := s.GetMasterInfo()
	if err != nil {
		c.BaseConn.Close()
		log.Log.Errorf("NewConn:GetMasterInfo error,err:%s", err)
		return nil, err
	}

	err = c.handshake(masterInfo.Version, password)
	if err != nil {
		c.BaseConn.Close()
		log.Log.Errorf("NewConn:handshake error,err:%s", err)
		return nil, err
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.userVariables = make(map[string]interface{})
	c.binlogServer = s

	return c, nil
}

//handshake implements the handshake protocol in mysql
func (c *Conn) handshake(serverVersion, password string) error {
	if err := c.writeInitialHandshake(serverVersion); err != nil {
		return err
	}

	if err := c.readHandshakeResponse(password); err != nil {
		c.writeError(err)
		return err
	}

	if err := c.writeOK(nil); err != nil {
		return err
	}

	c.ResetSequence()

	return nil
}

//Run implements handle client request in Conn
func (c *Conn) Run() {
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]

			log.Log.Errorf("Conn Run error,err:%s,stack:%s", err, string(buf))
		}
		c.Close()
		log.Log.Debugf("close client connection")
	}()

	for {
		select {
		case <-c.ctx.Done():
			log.Log.Debugf("BinlogServer closed, close connection")
			return
		default:
			data, err := c.ReadPacket()
			if err != nil {
				log.Log.Errorf("ReadPacket error,err:%s", err)
				return
			}

			if err := c.dispatch(c.ctx, data); err != nil {
				log.Log.Errorf("dispatch error,err:%s,data:%v", err.Error(), data)
				//if the error is canceled, means the connection was killed by cmd
				//don't need send error message
				if err != context.Canceled && c.closed.Load() == false {
					c.writeError(err)
				}
				return
			}

			//if the connection is closed, return from loop
			if c.closed.Load() == true {
				log.Log.Infof("connection status is closed,need return")
				return
			}
			c.Sequence = 0
		}
	}
}

//Close the Conn
func (c *Conn) Close() {
	if c.closed.Load() == true {
		return
	}
	c.closed.Store(true)
	c.Conn.Close()
	c.cancel()
	c.Conn = nil
}
