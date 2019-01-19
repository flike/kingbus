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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/pkg/types"
	"github.com/flike/kingbus/mysql"
	"github.com/flike/kingbus/utils"
	"github.com/siddontang/go-mysql/replication"
)

const (
	//ServerRunningStatus means server is running
	ServerRunningStatus = "running"
	//ServerStoppedStatus means server has stopped
	ServerStoppedStatus = "stopped"
)

//SubServerType is the type of sub server
type SubServerType int8

const (
	//SyncerServerType represent a binlog syncer
	SyncerServerType SubServerType = iota + 1
	//BinlogServerType represent a binlog master
	BinlogServerType
)

//SyncerArgs is the argument to start a syncer
type SyncerArgs struct {
	SyncerID      int    `json:"syncer_id"`
	SynerUUID     string `json:"syncer_uuid"`
	MysqlAddr     string `json:"mysql_addr"`
	MysqlUser     string `json:"mysql_user"`
	MysqlPassword string `json:"mysql_password"`
	SemiSync      bool   `json:"semi_sync"`
}

//Check the correctness of SyncerArgs
func (s *SyncerArgs) Check() error {
	if s.SyncerID < 0 || len(s.MysqlAddr) == 0 ||
		len(s.MysqlUser) == 0 || len(s.SynerUUID) == 0 {
		return errors.New("args illegal")
	}
	return nil
}

//EncodeWithType implements encode SyncerArgs to byte,the first byte is type
//apply function will Identify the type using the byte.
func (s *SyncerArgs) EncodeWithType() ([]byte, error) {
	buf, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 1, len(buf)+1)
	data[0] = utils.SyncerArgsType
	data = append(data, buf...)
	return data, nil
}

//Attributes is raft node attributes, including admin url
type Attributes struct {
	ID        types.ID `json:"id"`
	Name      string   `json:"name"`
	AdminURLs []string `json:"admin_urls"`
}

//EncodeWithType implements encode Attributes to byte,the first byte is type
//apply function will Identify the type using the byte.
func (a *Attributes) EncodeWithType() ([]byte, error) {
	buf, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 1, len(buf)+1)
	data[0] = utils.NodeAttributesType
	data = append(data, buf...)
	return data, nil
}

//MasterGtidPurged is the value of of master gtid_purged.
type MasterGtidPurged struct {
	GtidPurged string `json:"gtid_purged"`
}

//EncodeWithType implements encode Attributes to byte,the first byte is type
//apply function will Identify the type using the byte.
func (s *MasterGtidPurged) EncodeWithType() ([]byte, error) {
	buf, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 1, len(buf)+1)
	data[0] = utils.MasterGtidPurgedType
	data = append(data, buf...)
	return data, nil
}

//NewSyncerConfig create a syncer config
func NewSyncerConfig(args *SyncerArgs) (*SyncerConfig, error) {
	mysqlAddr := strings.Split(args.MysqlAddr, ":")
	port, err := strconv.ParseInt(mysqlAddr[1], 10, 32)
	if err != nil {
		return nil, err
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("port is illegal")
	}

	syncerCfg := replication.BinlogSyncerConfig{
		// ServerID is the unique id in cluster.
		ServerID: uint32(args.SyncerID),

		ServerUuid: args.SynerUUID,
		// Flavor is "mysql" or "mariadb", if not set, use "mysql" default.
		Flavor: "mysql",

		// Host is for MySQL server host.
		Host: mysqlAddr[0],
		// Port is for MySQL server port.
		Port: uint16(port),
		// User is for MySQL user.
		User: args.MysqlUser,
		// Password is for MySQL password.
		Password: args.MysqlPassword,

		// Charset is for MySQL client character set
		Charset: "utf8",

		// SemiSyncEnabled enables semi-sync or not.
		SemiSyncEnabled: args.SemiSync,

		// RawModeEnabled is for not parsing binlog event.
		RawModeEnabled: false,

		// If not nil, use the provided tls.Config to connect to the database using TLS/SSL.
		TLSConfig: nil,

		// Use replication.Time structure for timestamp and datetime.
		// We will use Local location for timestamp and UTC location for datatime.
		ParseTime: false,

		// Use decimal.Decimal structure for decimals.
		UseDecimal: true,

		// RecvBufferSize sets the size in bytes of the operating system's receive buffer associated with the connection.
		RecvBufferSize: 0,

		// master heartbeat period
		//The default value for interval is equal to the value of slave_net_timeout divided by 2.
		//slave_net_timeout default value is 60s
		//kingbus set to 10s
		HeartbeatPeriod: time.Second * 10,

		// syncer read timeout, if timeout has expired,syncer will reconnect
		ReadTimeout: time.Second * 20,

		// maximum number of attempts to re-establish a broken connection
		MaxReconnectAttempts: 3,

		VerifyChecksum: true,
	}
	return &SyncerConfig{
		BinlogSyncerConfig: syncerCfg,
		MaxEventBytes:      MaxRequestBytes,
	}, nil
}

//BinlogServerConfig is the argument to start a binlog master
type BinlogServerConfig struct {
	Addr string `json:"addr"`
	// User is for MySQL user.
	User string `json:"user"`
	// Password is for MySQL password.
	Password string `json:"password"`
}

//Check the correctness of BinlogServerConfig
func (s *BinlogServerConfig) Check(kingbusIP string) error {
	if len(s.Addr) == 0 || len(s.User) == 0 || len(s.Password) == 0 {
		return errors.New("args illegal")
	}
	if len(s.Addr) != 0 {
		strArray := strings.Split(s.Addr, ":")
		if strArray[0] != kingbusIP {
			return errors.New("ip not equal kingbusIP")
		}
	}
	return nil
}

//SyncerStatus is the status of syncer dump for user
type SyncerStatus struct {
	SyncerArgs
	CurrentGtid      string `json:"current_gtid"`
	LastBinlogFile   string `json:"last_binlog_file"`
	LastFilePosition uint32 `json:"last_file_position"`
	ExecutedGtidSet  string `json:"executed_gtid_set"`
	PurgedGtidSet    string `json:"purged_gtid_set"`
	Status           string `json:"status"`
}

//BinlogServerStatus is the status of binlog master dump for user
type BinlogServerStatus struct {
	BinlogServerConfig
	Slaves           []*mysql.Slave `json:"slaves"`
	CurrentGtid      string         `json:"current_gtid"`
	LastBinlogFile   string         `json:"last_binlog_file"`
	LastFilePosition uint32         `json:"last_file_position"`
	ExecutedGtidSet  string         `json:"executed_gtid_set"`
	PurgedGtidSet    string         `json:"purged_gtid_set"`
	Status           string         `json:"status"`
}
