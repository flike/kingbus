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

package storage

import (
	"testing"

	"github.com/flike/kingbus/utils"
	gomysql "github.com/siddontang/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
)

//case2: new meta store with file
func TestNewMetaStore2(t *testing.T) {
	clearAndInitDir()
	s, err := NewMetaStore(tempDataDir)
	assert.Nil(t, err)

	key := utils.StringToBytes("abc")
	value := utils.StringToBytes("def")
	err = s.Set(key, value)
	assert.Nil(t, err)

	s.Close2()

	s, err = NewMetaStore(tempDataDir)
	assert.Nil(t, err)

	v, err := s.Get(utils.StringToBytes("abc"))
	assert.Nil(t, err)
	assert.Equal(t, utils.BytesToString(v), "def")
	s.Close2()
}

func TestMetaStoreGet(t *testing.T) {
	clearAndInitDir()
	s, err := NewMetaStore(tempDataDir)
	assert.Nil(t, err)
	defer s.Close2()

	v, err := s.Get(utils.StringToBytes("abc"))
	assert.Nil(t, err)
	assert.Nil(t, v)
}

func TestMetaStore_Set(t *testing.T) {
	clearAndInitDir()
	s, err := NewMetaStore(tempDataDir)
	assert.Nil(t, err)
	defer s.Close2()

	key := utils.StringToBytes("abc")
	value := utils.StringToBytes("def")
	err = s.Set(key, value)
	assert.Nil(t, err)

	v, err := s.Get(utils.StringToBytes("abc"))
	assert.Nil(t, err)
	assert.Equal(t, utils.BytesToString(v), "def")
}

func TestMetaStore_Delete(t *testing.T) {
	clearAndInitDir()
	s, err := NewMetaStore(tempDataDir)
	assert.Nil(t, err)
	defer s.Close2()

	key := utils.StringToBytes("abc")
	value := utils.StringToBytes("def")
	err = s.Set(key, value)
	assert.Nil(t, err)

	v, err := s.Get(utils.StringToBytes("abc"))
	assert.Nil(t, err)
	assert.Equal(t, utils.BytesToString(v), "def")

	//delete
	err = s.Delete(utils.StringToBytes("abc"))
	assert.Nil(t, err)
	v, err = s.Get(utils.StringToBytes("abc"))
	assert.Nil(t, err)
	assert.Nil(t, v)
}

func TestMetaStore_GetFde(t *testing.T) {

}

func TestMetaStore_GetNextBinlogFile(t *testing.T) {

}

func TestMetaStore_GetPreviousGtidSetRaftIndex(t *testing.T) {

}

func TestMetaStore_InitialState(t *testing.T) {

}

func TestMetaStore_SaveHardState(t *testing.T) {

}

func TestMetaStore_UpdatePugedGtidset(t *testing.T) {

}

func TestGeneratePreviousLogName(t *testing.T) {
	s := "mysql_bin.000012"
	a := generatePreviousLogName(s)
	assert.Equal(t, a, "mysql_bin.000011")
}

func TestMetaStore_SetGtidSet(t *testing.T) {
	clearAndInitDir()
	s, err := NewMetaStore(tempDataDir)
	assert.Nil(t, err)
	defer s.Close2()

	gtidSet := "b8aab019-d062-11e8-bfab-c88d834bfdaf:20-1000"
	mysqlGtidSet, err := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, gtidSet)
	assert.Nil(t, err)

	err = s.SetGtidSet(gomysql.MySQLFlavor, "purge_key", mysqlGtidSet)
	assert.Nil(t, err)

	//get key
	v, err := s.GetGtidSet(gomysql.MySQLFlavor, "purge_key")
	assert.Nil(t, err)
	assert.Equal(t, v, mysqlGtidSet)
}

func TestDeletePreviousGtidSet(t *testing.T) {

}
