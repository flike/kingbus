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

package utils

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const (
	// PrivateFileMode grants owner to read/write a file.
	PrivateFileMode = 0600
	// PrivateDirMode grants owner to make/remove files inside the directory.
	PrivateDirMode = 0700
)

var (
	//LogSuffix is the log file name suffix
	LogSuffix = ".log"
	//IndexSuffix is the index file name suffix
	IndexSuffix = ".index"
)

//DirExist check the dir if exist
func DirExist(dir string) bool {
	_, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		panic(fmt.Errorf("DirExist error,err:%s", err))
	}
	return true
}

//ExistLog check log file if exist
func ExistLog(dirpath string) bool {
	dir, err := os.Open(dirpath)
	if err != nil {
		return false
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return false
	}
	sort.Strings(names)
	if err != nil {
		return false
	}
	for _, fileName := range names {
		if strings.HasSuffix(fileName, LogSuffix) {
			return true
		}
	}
	return false
}

//IsDirWriteable check dir if writeable
func IsDirWriteable(dir string) error {
	f := filepath.Join(dir, ".touch")
	if err := ioutil.WriteFile(f, []byte(""), PrivateFileMode); err != nil {
		return err
	}
	return os.Remove(f)
}

// ReadDir reads the directory named by dirname and returns
// a list of directory entries sorted by filename.
func ReadDir(dirname string) ([]os.FileInfo, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	list, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name() < list[j].Name() })
	return list, nil
}

//GetIndexPath get index path by log path
func GetIndexPath(logPath string) string {
	dir := path.Dir(logPath)
	baseName := path.Base(logPath)
	indexName := strings.TrimSuffix(baseName, LogSuffix) + IndexSuffix
	return path.Join(dir, indexName)
}

//GetIndexName get the index file name by segment name
func GetIndexName(segmentName string) string {
	return strings.TrimSuffix(segmentName, LogSuffix) + IndexSuffix
}
