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
	"fmt"

	gomysql "github.com/siddontang/go-mysql/mysql"
)

func (c *Conn) writeOK(r *gomysql.Result) error {
	if r == nil {
		r = &gomysql.Result{}
	}

	r.Status |= c.status

	data := make([]byte, 4, 32)

	data = append(data, gomysql.OK_HEADER)

	data = append(data, gomysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, gomysql.PutLengthEncodedInt(r.InsertId)...)

	if c.capability&gomysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, 0, 0)
	}

	return c.WritePacket(data)
}

func (c *Conn) writeError(e error) error {
	var m *gomysql.MyError
	var ok bool
	if m, ok = e.(*gomysql.MyError); !ok {
		m = gomysql.NewError(gomysql.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, gomysql.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if c.capability&gomysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return c.WritePacket(data)
}

func (c *Conn) writeEOF() error {
	data := make([]byte, 4, 9)

	data = append(data, gomysql.EOF_HEADER)
	if c.capability&gomysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(c.status), byte(c.status>>8))
	}

	return c.WritePacket(data)
}

func (c *Conn) writeResultset(r *gomysql.Resultset) error {
	columnLen := gomysql.PutLengthEncodedInt(uint64(len(r.Fields)))

	data := make([]byte, 4, 1024)

	data = append(data, columnLen...)
	if err := c.WritePacket(data); err != nil {
		return err
	}

	for _, v := range r.Fields {
		data = data[0:4]
		data = append(data, v.Dump()...)
		if err := c.WritePacket(data); err != nil {
			return err
		}
	}

	if err := c.writeEOF(); err != nil {
		return err
	}

	for _, v := range r.RowDatas {
		data = data[0:4]
		data = append(data, v...)
		if err := c.WritePacket(data); err != nil {
			return err
		}
	}

	if err := c.writeEOF(); err != nil {
		return err
	}

	return nil
}

func (c *Conn) writeFieldList(fs []*gomysql.Field) error {
	data := make([]byte, 4, 1024)

	for _, v := range fs {
		data = data[0:4]
		data = append(data, v.Dump()...)
		if err := c.WritePacket(data); err != nil {
			return err
		}
	}

	if err := c.writeEOF(); err != nil {
		return err
	}
	return nil
}

type noResponse struct{}

func (c *Conn) writeValue(value interface{}) error {
	switch v := value.(type) {
	case noResponse:
		return nil
	case error:
		return c.writeError(v)
	case nil:
		return c.writeOK(nil)
	case *gomysql.Result:
		if v != nil && v.Resultset != nil {
			return c.writeResultset(v.Resultset)
		}
		return c.writeOK(v)
	case []*gomysql.Field:
		return c.writeFieldList(v)
	default:
		return fmt.Errorf("invalid response type %T", value)
	}
}
