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
	"hash/crc32"

	"github.com/flike/kingbus/storage/storagepb"

	"runtime"

	"encoding/binary"

	"time"

	"strings"

	"fmt"

	"github.com/flike/kingbus/log"
	"github.com/flike/kingbus/storage"
	"github.com/flike/kingbus/utils"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver" //need by parser
	metrics "github.com/rcrowley/go-metrics"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

//MaxHeartbeatPeriod is ten years
var MaxHeartbeatPeriod = time.Hour * 24 * 30 * 12 * 10

const (
	//ServerID is the variable name of server id in mysql
	ServerID = "server_id"
	//ServerUUID is the variable name of server uuid in mysql
	ServerUUID = "server_uuid"
	//SlaveUUID is the variable name of slave uuid in mysql
	SlaveUUID = "slave_uuid"
	//MasterHeartbeatPeriod is the variable name of master_heartbeat_period in mysql
	MasterHeartbeatPeriod = "master_heartbeat_period"
	//GtidMode is the variable name of gtid_mode in mysql
	GtidMode = "gtid_mode"
	//GtidPurged is the variable name of gtid_purged in mysql
	GtidPurged = "gtid_purged"
	//VersionComment is the variable name of version_comment in mysql
	VersionComment = "version_comment"
	//BinlogFormat is the variable name of binlog_format in mysql
	BinlogFormat = "binlog_format"
	//BinlogChecksum is the variable name of binlog_checksum in mysql
	BinlogChecksum = "binlog_checksum"
	//MasterBinlogChecksum is the variable name of master_binlog_checksum in mysql
	MasterBinlogChecksum = "master_binlog_checksum"
	//SetNames using in the replication interaction process
	SetNames = "setnames"
	//UnixTimestamp is the variable name of unix_timestamp in mysql
	UnixTimestamp = "unix_timestamp"
	//Version is the variable name of version in mysql
	Version = "version"
)

func (c *Conn) dispatch(ctx context.Context, data []byte) error {
	cmd := data[0]
	data = data[1:]

	switch cmd {
	case gomysql.COM_QUIT:
		c.Close()
		log.Log.Debugf("close client connection")
		return nil
	case gomysql.COM_QUERY:
		return c.handleQuery(utils.BytesToString(data))
	case gomysql.COM_PING:
		return c.writeOK(nil)
	case gomysql.COM_BINLOG_DUMP_GTID:
		return c.handleBinlogDumpGtid(ctx, data)
	case gomysql.COM_REGISTER_SLAVE:
		return c.handleRegisterSlave(data)
	default:
		log.Log.Errorf("master not support this cmd:%v", data)
		return c.writeError(ErrSQLNotSupport)
	}

	return nil
}

func (c *Conn) handleQuery(sql string) (err error) {
	defer func() {
		if e := recover(); e != nil {
			if myerr, ok := e.(error); ok {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				log.Log.Errorf("Conn handleQuery error,err:%s,sql:%s,stack:%s", myerr, sql, string(buf))
				err = myerr
			}
			return
		}
	}()

	log.Log.Infof("handleQuery sql:%s", sql)
	sqlParser := parser.New()
	stmt, err := sqlParser.ParseOneStmt(sql, "", "")
	if err != nil {
		return err
	}
	switch v := stmt.(type) {
	case *ast.ShowStmt:
		return c.handleShow(v)
	case *ast.SelectStmt:
		return c.handleSelect(v)
	case *ast.SetStmt:
		return c.handleSet(v)
	case *ast.KillStmt:
		return c.handleKill(v)
	//todo get metrics of slave
	default:
		return ErrSQLNotSupport
	}
	return
}

//SELECT @@GLOBAL.SERVER_UUID
//SELECT @@GLOBAL.SERVER_ID
//SELECT @master_binlog_checksum
//SELECT UNIX_TIMESTAMP()
//SELECT @@gtid_mode
//SELECT @@gtid_purged
func (c *Conn) handleSelect(stmt *ast.SelectStmt) error {
	if len(stmt.Fields.Fields) == 1 {
		columnName := stmt.Fields.Fields[0].Text()

		switch v := stmt.Fields.Fields[0].Expr.(type) {
		case *ast.VariableExpr:
			return c.handleSelectVariableExpr(v, columnName)
		case *ast.FuncCallExpr:
			return c.handleSelectFuncCallExpr(v, columnName)
		}
	}

	return ErrSQLNotSupport
}

func (c *Conn) handleSelectVariableExpr(v *ast.VariableExpr, columnName string) error {
	var result *gomysql.Resultset
	var err error

	switch strings.ToLower(v.Name) {
	case ServerID, ServerUUID:
		//SELECT @@GLOBAL.SERVER_UUID
		//SELECT @@GLOBAL.SERVER_ID
		masterInfo, err := c.binlogServer.GetMasterInfo()
		if err != nil {
			return err
		}
		if v.Name == ServerUUID {
			result, err = gomysql.BuildSimpleResultset(
				[]string{columnName},
				[][]interface{}{
					[]interface{}{masterInfo.ServerUUID},
				},
				false)
		} else {
			result, err = gomysql.BuildSimpleResultset(
				[]string{columnName},
				[][]interface{}{
					[]interface{}{masterInfo.ServerID},
				},
				false)
		}
	case GtidMode:
		result, err = gomysql.BuildSimpleResultset(
			[]string{columnName},
			[][]interface{}{
				[]interface{}{"ON"},
			},
			false)
	case GtidPurged:
		gtidSet, err := c.binlogServer.GetGtidSet(gomysql.MySQLFlavor, storage.GtidPurgedKey)
		if err != nil {
			return err
		}
		result, err = gomysql.BuildSimpleResultset(
			[]string{columnName},
			[][]interface{}{
				[]interface{}{gtidSet.String()},
			},
			false)
	case VersionComment:
		result, err = gomysql.BuildSimpleResultset(
			[]string{columnName},
			[][]interface{}{
				[]interface{}{"MySQL Community Server (GPL)"},
			},
			false)
	case BinlogFormat:
		result, err = gomysql.BuildSimpleResultset(
			[]string{columnName},
			[][]interface{}{
				[]interface{}{"ROW"},
			},
			false)
	case BinlogChecksum:
		result, err = gomysql.BuildSimpleResultset(
			[]string{columnName},
			[][]interface{}{
				[]interface{}{"CRC32"},
			},
			false)
	case MasterBinlogChecksum:
		result, err = gomysql.BuildSimpleResultset(
			[]string{columnName},
			[][]interface{}{
				[]interface{}{"CRC32"},
			},
			false)
	}
	if err != nil {
		return err
	}

	err = c.writeResultset(result)
	if err != nil {
		return err
	}

	return nil
}

//SELECT UNIX_TIMESTAMP()
//SELECT VERSION()
func (c *Conn) handleSelectFuncCallExpr(v *ast.FuncCallExpr, columnName string) error {
	var result *gomysql.Resultset
	var err error
	switch v.FnName.L {
	case UnixTimestamp:
		result, err = gomysql.BuildSimpleResultset(
			[]string{columnName},
			[][]interface{}{
				[]interface{}{time.Now().Unix()},
			},
			false)
	case Version:
		masterInfo, err := c.binlogServer.GetMasterInfo()
		if err != nil {
			return err
		}
		result, err = gomysql.BuildSimpleResultset(
			[]string{columnName},
			[][]interface{}{
				[]interface{}{masterInfo.Version},
			},
			false)
	}

	err = c.writeResultset(result)
	if err != nil {
		return err
	}
	return nil
}

func (c *Conn) handleShow(stmt *ast.ShowStmt) error {
	showVariable := stmt.Pattern.Pattern.Text()
	log.Log.Debugf("show query showVariable:%s", showVariable)
	switch strings.ToLower(showVariable) {
	case ServerID:
		masterInfo, err := c.binlogServer.GetMasterInfo()
		if err != nil {
			return err
		}
		result, err := gomysql.BuildSimpleResultset(
			[]string{showVariable},
			[][]interface{}{
				[]interface{}{masterInfo.ServerID},
			},
			false)
		err = c.writeResultset(result)
		if err != nil {
			return err
		}
	}

	return ErrSQLNotSupport
}

func (c *Conn) handleSet(stmt *ast.SetStmt) error {
	if len(stmt.Variables) != 1 {
		return ErrSQLNotSupport
	}

	name := strings.ToLower(stmt.Variables[0].Name)

	switch name {
	case MasterHeartbeatPeriod:
		valueExpr := stmt.Variables[0].Value.(ast.ValueExpr)
		//the period is an nano-secs
		period := valueExpr.GetValue().(int64)
		c.userVariables[MasterHeartbeatPeriod] = period
		//set heartbeat in slave
		if slaveUUID, ok := c.userVariables[SlaveUUID]; ok {
			slaves := c.binlogServer.GetSlaves()
			for _, s := range slaves {
				if s.UUID == slaveUUID {
					s.HeartBeat = time.Duration(period)
					break
				}
			}
		}

		err := c.writeOK(nil)
		if err != nil {
			log.Log.Errorf("writeOK error,err:%s", err)
			return err
		}
		return nil
	case SlaveUUID:
		valueExpr := stmt.Variables[0].Value.(ast.ValueExpr)
		slaveUUID := valueExpr.GetString()
		if len(slaveUUID) != 0 {
			slaves := c.binlogServer.GetSlaves()
			for _, s := range slaves {
				if s.UUID == slaveUUID && s.Conn == c {
					log.Log.Warningf("slave has been registered, slaveUUID:%s", slaveUUID)
				}
			}
		}
		c.userVariables[SlaveUUID] = slaveUUID
		err := c.writeOK(nil)
		if err != nil {
			log.Log.Errorf("writeOK error,err:%s", err)
			return err
		}
		return nil
	case MasterBinlogChecksum:
		// @@global.binlog_checksum or crc32
		switch v := stmt.Variables[0].Value.(type) {
		case *ast.VariableExpr:
			if v.IsSystem == false || v.IsGlobal == false ||
				strings.ToLower(v.Name) != "binlog_checksum" {
				return ErrChecksum
			}
		case ast.ValueExpr:
			if strings.ToLower(v.GetString()) != "crc32" {
				return ErrChecksum
			}
		}
		c.userVariables[MasterBinlogChecksum] = "crc32"
		err := c.writeOK(nil)
		if err != nil {
			log.Log.Errorf("writeOK error,err:%s", err)
			return err
		}
		return nil
	case SetNames:
		valueExpr := stmt.Variables[0].Value.(ast.ValueExpr)
		c.userVariables[SetNames] = valueExpr.GetString()
		err := c.writeOK(nil)
		if err != nil {
			log.Log.Errorf("writeOK error,err:%s", err)
			return err
		}
		return nil
	}
	return ErrSQLNotSupport
}

//ignore this command, because kingbus will close connection automatically
func (c *Conn) handleKill(stmt *ast.KillStmt) error {
	slaves := c.binlogServer.GetSlaves()
	for _, s := range slaves {
		if s.Conn.connectionID == uint32(stmt.ConnectionID) {
			c.binlogServer.UnregisterSlave(s.UUID)
			break
		}
	}
	return c.writeOK(nil)
}

func (c *Conn) handleRegisterSlave(data []byte) error {
	var s Slave
	pos := 0

	s.ServerID = int32(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	hostNameLen := int(data[pos])
	pos++

	s.HostName = string(data[pos : pos+hostNameLen])
	pos += hostNameLen

	userLen := int(data[pos])
	pos++

	s.User = string(data[pos : pos+userLen])
	pos += userLen

	passwordLen := int(data[pos])
	pos++

	s.Password = string(data[pos : pos+passwordLen])
	pos += passwordLen

	s.Port = int16(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	s.Rank = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	s.MasterID = binary.LittleEndian.Uint32(data[pos:])
	s.State = REGISTERED

	//kill the zombie dump thread with same uuid
	c.killZombieDumpThreads()

	if uuid, ok := c.userVariables[SlaveUUID]; ok {
		s.UUID = uuid.(string)
	} else {
		s.UUID = ""
	}
	s.ConnectTime = time.Now()
	s.Conn = c
	err := c.binlogServer.RegisterSlave(&s)
	if err != nil {
		return err
	}
	log.Log.Infof("handleRegisterSlave:slave info:%v", s)
	return c.writeOK(nil)
}

//todo kill the slave with same uuid
func (c *Conn) handleBinlogDumpGtid(ctx context.Context, data []byte) error {
	var (
		err             error
		heartbeatPeriod time.Duration
	)

	slaveGtidExecuted, slaveServerID, err := c.parseMysqlGtidDumpPacket(data)
	if err != nil {
		return err
	}

	//UnregisterSlave
	slaveUUID := c.userVariables[SlaveUUID].(string)
	defer c.binlogServer.UnregisterSlave(slaveUUID)

	err = c.binlogServer.CheckGtidSet(gomysql.MySQLFlavor, slaveGtidExecuted)
	if err != nil {
		log.Log.Errorf("CheckGtidSet error,err:%s,slaveGtids:%v", err, slaveGtidExecuted)
		return err
	}

	//get the previousGtidEvent raft index
	preGtidEventIndex, err := c.binlogServer.GetMySQLDumpAt(slaveGtidExecuted)
	if err != nil {
		log.Log.Errorf("GetMySQLDumpAt error,err:%s,slaveGtids:%v", err, slaveGtidExecuted)
		return err
	}

	fde, err := c.binlogServer.GetFde(preGtidEventIndex)
	if err != nil {
		log.Log.Errorf("handleBinlogDumpGtid:GetFde error,err:%s, gtidSet: %s,flavor:%s",
			err, slaveGtidExecuted.String(), gomysql.MySQLFlavor)
		return err
	}

	//1.send fake rotate event
	masterServerID := binary.LittleEndian.Uint32(fde[5:])
	fileName, err := c.binlogServer.GetNextBinlogFile(preGtidEventIndex)
	if err != nil {
		log.Log.Errorf("handleBinlogDumpGtid:GetNextBinlogFile error,err:%s, gtidSet: %s,flavor:%s",
			err, slaveGtidExecuted.String(), gomysql.MySQLFlavor)
		return err
	}
	err = c.sendFakeRotateEvent(masterServerID, fileName)
	if err != nil {
		log.Log.Errorf("handleBinlogDumpGtid:sendFakeRotateEvent error,err:%s, serverId: %d,fileName:%s",
			err, masterServerID, fileName)
		return err
	}

	//2.send fde
	err = c.sendFormatDescriptionEvent(fde)
	if err != nil {
		log.Log.Errorf("handleBinlogDumpGtid:sendFormatDescriptionEvent error,err:%s, fde:%v",
			err, fde)
		return err
	}

	//3.send event
	eventC := make(chan *storagepb.BinlogEvent, 2000)
	errorC := make(chan error, 1)
	err = c.binlogServer.DumpBinlogAt(ctx, preGtidEventIndex, slaveGtidExecuted, eventC, errorC)
	if err != nil {
		log.Log.Errorf("DumpBinlogAt error,err:%s,preGtidEventIndex:%d,slaveGtidExecuted:%v",
			err, preGtidEventIndex, slaveGtidExecuted)
		return err
	}

	//4.new metrics
	slaveEps := metrics.NewMeter()
	slaveThroughput := metrics.NewMeter()
	metrics.Register(fmt.Sprintf("slave_eps_%d", slaveServerID), slaveEps)
	metrics.Register(fmt.Sprintf("slave_thoughput_%d", slaveServerID), slaveThroughput)

	if period, ok := c.userVariables[MasterHeartbeatPeriod]; ok {
		heartbeatPeriod = time.Duration(period.(int64))
	} else {
		heartbeatPeriod = MaxHeartbeatPeriod
	}
	timer := time.NewTimer(heartbeatPeriod)
	for {
		select {
		case event := <-eventC:
			//event is not divided or the first divided event
			//WriteEvent need write a ok_header(one byte),after the header size
			if event.DividedCount == 0 || (0 < event.DividedCount && event.DividedSeqNum == 0) {
				err = c.WriteEvent(event.Data, true)
				if err != nil {
					log.Log.Errorf("WriteEvent error,err:%s,event:%v", err, *event)
					return err
				}
			} else {
				err = c.WriteEvent(event.Data, false)
				if err != nil {
					log.Log.Errorf("WriteEvent error,err:%s,event:%v", err, *event)
					return err
				}
				//event is divided,and the last packet size is MaxPayloadLen
				//need send a empty packet
				//https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
				if event.DividedSeqNum == event.DividedCount-1 && len(event.Data) == MaxPayloadLen {
					err = c.WriteEvent(nil, false)
					if err != nil {
						log.Log.Errorf("WriteEvent error,err:%s,event:%v", err, *event)
						return err
					}
				}
			}
			slaveEps.Mark(1)
			slaveThroughput.Mark(int64(len(event.Data)))
			//reset heartbeat period
			resetTime(timer, heartbeatPeriod)
		case err = <-errorC:
			log.Log.Errorf("binlog server DumpBinlogAt error,err:%s", err)
			return err
		case <-ctx.Done():
			log.Log.Errorf("handleBinlogDumpGtid:ctx done,quit")
			return ctx.Err()
		case <-timer.C:
			//kingbus send the heartbeat log event which received by syncer to slave
			log.Log.Debugf("send a heartbeat log event to slave")
			err = c.sendHeartbeatEvent(masterServerID)
			if err != nil {
				return err
			}
			//reset heartbeat period
			resetTime(timer, heartbeatPeriod)
		}
	}
	return nil
}

func (c *Conn) killZombieDumpThreads() {
	slaveUUID, ok := c.userVariables[SlaveUUID]
	if !ok {
		return
	}
	if len(slaveUUID.(string)) == 0 {
		return
	}

	slaves := c.binlogServer.GetSlaves()
	for uuid, slave := range slaves {
		//different connection use the same uuid, kill the connection registered
		if uuid == slaveUUID && slave.Conn != c {
			log.Log.Debugf("killZombieDumpThreads:kill slave,uuid is:%s", slaveUUID)
			c.binlogServer.UnregisterSlave(uuid)
			return
		}
	}
}

func (c *Conn) parseMysqlGtidDumpPacket(data []byte) (*gomysql.MysqlGTIDSet, uint32, error) {
	var flags uint16
	var dataSize uint32
	var slaveServerID uint32
	var binlogFileNameSize uint32
	var binlogFileName string
	var binlogPos uint64

	var pos uint32
	flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	slaveServerID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	binlogFileNameSize = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	binlogFileName = string(data[pos : pos+binlogFileNameSize])
	pos += binlogFileNameSize

	binlogPos = binary.LittleEndian.Uint64(data[pos:])
	pos += 8

	dataSize = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	//only support mysql gtid
	slaveGtidExecuted, err := gomysql.DecodeMysqlGTIDSet(data[pos:])
	if err != nil {
		log.Log.Errorf("DecodeMysqlGTIDSet error,err:%s,data:%v", err, data[pos:])
		return nil, 0, err
	}

	log.Log.Debugf("handleBinlogDumpGtid:flag:%d,slaveServerID:%d,binlogFileNameSize:%d,"+
		"binlogFileName:%s,binlogPos:%d,dataSize:%d,slaveGtidExecuted:%v", flags, slaveServerID, binlogFileNameSize,
		binlogFileName, binlogPos, dataSize, *slaveGtidExecuted)
	return slaveGtidExecuted, slaveServerID, nil
}

//When starting to dump a binlog file, Format_description_log_event
//is read and sent first. If the requested position is after
//Format_description_log_event, log_pos field in the first
//Format_description_log_event has to be set to 0. So the slave
//will not increment its master's binlog position
func (c *Conn) sendFormatDescriptionEvent(data []byte) error {
	//next_position
	var LogPosOffset = 13
	binary.LittleEndian.PutUint32(data[LogPosOffset:LogPosOffset+4], 0)

	//remove crc32
	data = data[:len(data)-4]

	//recalculate the CRC
	checksum := crc32.ChecksumIEEE(data)
	computed := make([]byte, replication.BinlogChecksumLength)
	binary.LittleEndian.PutUint32(computed, checksum)

	data = append(data, computed...)
	return c.WriteEvent(data, true)
}

func (c *Conn) sendHeartbeatEvent(serverID uint32) error {
	binlogFile := c.binlogServer.LastBinlogFile()
	filePosition := c.binlogServer.LastFilePosition()
	data := buildHeartbeatEvent(binlogFile, serverID, filePosition)
	return c.WriteEvent(data, true)
}

func buildHeartbeatEvent(fileName string, serverID uint32, logPos uint32) []byte {
	eventHeader := &replication.EventHeader{
		Timestamp: 0,
		EventType: replication.HEARTBEAT_EVENT,
		ServerID:  serverID,
		//enable crc32
		EventSize: uint32(replication.EventHeaderSize + len(fileName) + replication.BinlogChecksumLength),
		LogPos:    logPos,
		//LOG_ARTIFICIAL_F
		Flags: 0x20,
	}

	eventBuf := encodeEventHeader(eventHeader)
	fileNameBuf := utils.StringToBytes(fileName)
	eventBuf = append(eventBuf, fileNameBuf...)

	// mysql use zlib's CRC32 implementation, which uses polynomial 0xedb88320UL.
	// reference: https://github.com/madler/zlib/blob/master/crc32.c
	// https://github.com/madler/zlib/blob/master/doc/rfc1952.txt#L419
	checksum := crc32.ChecksumIEEE(eventBuf)
	computed := make([]byte, replication.BinlogChecksumLength)
	binary.LittleEndian.PutUint32(computed, checksum)
	eventBuf = append(eventBuf, computed...)
	return eventBuf
}

//send a fake rotate event
//server_id comes from fde event
func (c *Conn) sendFakeRotateEvent(serverID uint32, fileName string) error {
	var needCrc32 bool
	if value, ok := c.userVariables[MasterBinlogChecksum]; ok {
		if value == "crc32" {
			needCrc32 = true
		}
	}
	data := buildFakeRotateEvent(serverID, fileName, needCrc32)
	return c.WriteEvent(data, true)
}

func buildFakeRotateEvent(serverID uint32, fileName string, needCrc32 bool) []byte {
	eventHeader := &replication.EventHeader{
		Timestamp: 0,
		EventType: replication.ROTATE_EVENT,
		ServerID:  serverID,
		//enable crc32
		EventSize: uint32(replication.EventHeaderSize + 8 + len(fileName) + replication.BinlogChecksumLength),
		LogPos:    0,
		//LOG_ARTIFICIAL_F
		Flags: 0x20,
	}
	eventBuf := encodeEventHeader(eventHeader)

	//the requested pos from slave, usually 4
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf[:], 4)

	fileNameBuf := utils.StringToBytes(fileName)
	eventBuf = append(eventBuf, buf...)
	eventBuf = append(eventBuf, fileNameBuf...)

	if needCrc32 {
		// mysql use zlib's CRC32 implementation, which uses polynomial 0xedb88320UL.
		// reference: https://github.com/madler/zlib/blob/master/crc32.c
		// https://github.com/madler/zlib/blob/master/doc/rfc1952.txt#L419
		checksum := crc32.ChecksumIEEE(eventBuf)
		computed := make([]byte, replication.BinlogChecksumLength)
		binary.LittleEndian.PutUint32(computed, checksum)
		eventBuf = append(eventBuf, computed...)
	}

	return eventBuf
}

func encodeEventHeader(header *replication.EventHeader) []byte {
	buf := make([]byte, replication.EventHeaderSize)
	pos := 0

	binary.LittleEndian.PutUint32(buf[pos:], header.Timestamp)
	pos += 4

	buf[pos] = byte(header.EventType)
	pos++

	binary.LittleEndian.PutUint32(buf[pos:], header.ServerID)
	pos += 4

	binary.LittleEndian.PutUint32(buf[pos:], header.EventSize)
	pos += 4

	binary.LittleEndian.PutUint32(buf[pos:], header.LogPos)
	pos += 4

	binary.LittleEndian.PutUint16(buf[pos:], header.Flags)

	return buf
}

func resetTime(t *time.Timer, period time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C: //try to drain from the channel
		default:
		}
	}
	t.Reset(period)
}
