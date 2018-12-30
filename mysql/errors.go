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

import "errors"

var (
	//ErrSQLNotSupport returns for sql is not support
	ErrSQLNotSupport = errors.New("mysql:sql not support")
	//ErrBadConn returns for connection was bad
	ErrBadConn = errors.New("mysql:connection was bad")
	//ErrLargePacket returns for the the packet is large than 16MB
	ErrLargePacket = errors.New("mysql:the packet is large than 16MB")
	//ErrChecksum returns for master_binlog_checksum is not crc32
	ErrChecksum = errors.New("mysql:master_binlog_checksum must be crc32")
	//ErrUpdateState returns for transaction boundary parser update state error
	ErrUpdateState = errors.New("mysql:transaction boundary parser update state error")
)
