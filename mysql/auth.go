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
	"bytes"
	"encoding/binary"

	gomysql "github.com/siddontang/go-mysql/mysql"
)

func (c *Conn) writeInitialHandshake(serverVersion string) error {
	capability := gomysql.CLIENT_LONG_PASSWORD | gomysql.CLIENT_LONG_FLAG |
		gomysql.CLIENT_CONNECT_WITH_DB | gomysql.CLIENT_PROTOCOL_41 |
		gomysql.CLIENT_TRANSACTIONS | gomysql.CLIENT_SECURE_CONNECTION

	data := make([]byte, 4, 128)

	//min version 10
	data = append(data, 10)

	//server version[00]
	data = append(data, serverVersion...)
	data = append(data, 0)

	//connection id
	data = append(data, byte(c.connectionID), byte(c.connectionID>>8), byte(c.connectionID>>16), byte(c.connectionID>>24))

	//auth-plugin-data-part-1
	data = append(data, c.salt[0:8]...)

	//filter [00]
	data = append(data, 0)

	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(capability), byte(capability>>8))

	//charset, utf-8 default
	data = append(data, uint8(gomysql.DEFAULT_COLLATION_ID))

	//status
	data = append(data, byte(c.status), byte(c.status>>8))

	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(capability>>16), byte(capability>>24))

	//filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)

	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	//auth-plugin-data-part-2
	data = append(data, c.salt[8:]...)

	//filter [00]
	data = append(data, 0)

	return c.WritePacket(data)
}

func (c *Conn) readHandshakeResponse(password string) error {
	data, err := c.ReadPacket()

	if err != nil {
		return err
	}

	pos := 0

	//capability
	c.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4

	//skip max packet size
	pos += 4

	//charset, skip, if you want to use another charset, use set names
	//c.collation = CollationId(data[pos])
	pos++

	//skip reserved 23[00]
	pos += 23

	//user name
	user := string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
	pos += len(user) + 1

	if c.user != user {
		return gomysql.NewDefaultError(gomysql.ER_NO_SUCH_USER, user, c.RemoteAddr().String())
	}

	//auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]

	checkAuth := gomysql.CalcPassword(c.salt, []byte(password))

	if !bytes.Equal(auth, checkAuth) {
		return gomysql.NewDefaultError(gomysql.ER_ACCESS_DENIED_ERROR, c.RemoteAddr().String(), c.user, "Yes")
	}

	pos += authLen

	if c.capability|gomysql.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		db := string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		pos += len(db) + 1
		//todo
		//if err = c.h.UseDB(db); err != nil {
		//	return err
		//}
	}

	return nil
}
