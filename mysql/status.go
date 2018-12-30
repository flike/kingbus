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
	"encoding/json"
	"time"

	"github.com/flike/kingbus/utils"
)

//ReplicationState is the slave state
type ReplicationState int8

const (
	//UNREGISTERED represents slave has not been register
	UNREGISTERED ReplicationState = iota
	//REGISTERED represents slave has been register
	REGISTERED
)

//Stats is slave stats
type Stats struct {
}

//Slave replicating binlogs from kingbus
type Slave struct {
	State ReplicationState `json:"replication_state"`

	ServerID int32  `json:"server_id"`
	UUID     string `json:"server_uuid"`
	HostName string `json:"host_name"`
	Port     int16  `json:"port"`
	Rank     uint32 `json:"-"`
	MasterID uint32 `json:"-"`

	User     string `json:"user"`
	Password string `json:"password"`

	ConnectTime time.Time     `json:"connect_time"`
	HeartBeat   time.Duration `json:"heartbeat"`

	Conn  *Conn `json:"-"`
	Stats `json:"stats"`
}

//Close slave
func (s *Slave) Close() {
	s.Conn.Close()
}

//MasterInfo is the master information of syncer connected
type MasterInfo struct {
	ServerID   int32  `json:"server_id"`
	ServerUUID string `json:"server_uuid"`
	Version    string `json:"version"`
}

//EncodeMasterInfo implements encode MasterInfo into byte
func (m *MasterInfo) EncodeMasterInfo() ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	dataWithType := make([]byte, 1, len(data)+1)
	dataWithType[0] = utils.MasterInfoType
	dataWithType = append(dataWithType, data...)
	return dataWithType, nil
}
