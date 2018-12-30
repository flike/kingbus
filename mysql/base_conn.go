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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"

	"github.com/flike/kingbus/log"
)

const (
	//MaxPayloadLen is the max size of one packet in mysql
	MaxPayloadLen int = 1<<24 - 1
	//OKHeaderByte is is the flag for OK packet header
	OKHeaderByte = 1
)

//BaseConn is the base class to handle MySQL protocol.
type BaseConn struct {
	net.Conn
	br       *bufio.Reader
	bw       *bufio.Writer
	Sequence uint8
}

//NewBaseConn implements create a BaseConn
func NewBaseConn(conn net.Conn) *BaseConn {
	c := new(BaseConn)

	c.br = bufio.NewReaderSize(conn, 1<<20)
	c.bw = bufio.NewWriterSize(conn, 1<<20)
	c.Conn = conn

	return c
}

//ReadPacket implements read data from connection into buffer
func (c *BaseConn) ReadPacket() ([]byte, error) {
	var buf bytes.Buffer

	err := c.ReadPacketTo(&buf)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

//ReadPacketTo implements read data from connection into writer
func (c *BaseConn) ReadPacketTo(w io.Writer) error {
	header := []byte{0, 0, 0, 0}

	if _, err := io.ReadFull(c.br, header); err != nil {
		return err
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length < 1 {
		return fmt.Errorf("invalid payload length %d", length)
	}

	sequence := uint8(header[3])

	if sequence != c.Sequence {
		return fmt.Errorf("invalid sequence %d != %d", sequence, c.Sequence)
	}

	c.Sequence++

	if n, err := io.CopyN(w, c.br, int64(length)); err != nil {
		return err
	} else if n != int64(length) {
		return fmt.Errorf("read length not expect")
	} else {
		if length < MaxPayloadLen {
			return nil
		}

		if err := c.ReadPacketTo(w); err != nil {
			return err
		}
	}

	return nil
}

//WritePacket implements encode data into a mysql packet,
//then write the packet by connection
func (c *BaseConn) WritePacket(data []byte) error {
	length := len(data) - 4

	for length >= MaxPayloadLen {
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = c.Sequence

		if n, err := c.Write(data[:4+MaxPayloadLen]); err != nil {
			return err
		} else if n != (4 + MaxPayloadLen) {
			return fmt.Errorf("write length not expect")
		} else {
			c.Sequence++
			length -= MaxPayloadLen
			data = data[MaxPayloadLen:]
		}
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = c.Sequence

	if n, err := c.Write(data); err != nil {
		return err
	} else if n != len(data) {
		return fmt.Errorf("write length not expect")
	} else {
		c.Sequence++
		return nil
	}
}

//WriteEvent implements encode data into a binlog event,then write by the connection.
//the syncer will divide the size large than 16MB packet
//WriteEvent don't need dividing packet
func (c *BaseConn) WriteEvent(data []byte, first bool) error {
	var packetLen int

	header := make([]byte, 4)
	if first {
		packetLen = len(data) + OKHeaderByte
	} else {
		packetLen = len(data)
	}

	if MaxPayloadLen < packetLen {
		log.Log.Errorf("data is large than 0xffffff,packetLen:%d", packetLen)
		return ErrLargePacket
	}

	header[0] = byte(packetLen)
	header[1] = byte(packetLen >> 8)
	header[2] = byte(packetLen >> 16)
	header[3] = c.Sequence

	_, err := c.bw.Write(header)
	if err != nil {
		return err
	}
	//todo need add semi sync flag?
	//ok_header
	if first {
		err = c.bw.WriteByte(0)
		if err != nil {
			return err
		}
	}

	if 0 < len(data) {
		_, err = c.bw.Write(data)
		if err != nil {
			return err
		}
	}

	err = c.bw.Flush()
	if err != nil {
		return err
	}
	c.Sequence++

	return nil
}

//ResetSequence implements reset the sequence of BaseConn
func (c *BaseConn) ResetSequence() {
	c.Sequence = 0
}

//Close the BaseConn
func (c *BaseConn) Close() error {
	c.Sequence = 0
	if c.Conn != nil {
		return c.Conn.Close()
	}
	return nil
}
