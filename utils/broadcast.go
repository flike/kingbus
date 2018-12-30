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

import "sync"

// Broadcast allows to send a signal to all listeners
type Broadcast struct {
	lock sync.RWMutex
	ch   chan struct{}
}

// NewBroadcast creates a new broadcast
func NewBroadcast() *Broadcast {
	return &Broadcast{
		lock: sync.RWMutex{},
		ch:   make(chan struct{}),
	}
}

// Receive a channel on which the next (close) signal will be sent
func (b *Broadcast) Receive() <-chan struct{} {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return b.ch
}

// Send a signal to all listeners
func (b *Broadcast) Send() {
	b.lock.Lock()
	defer b.lock.Unlock()
	close(b.ch)
	b.ch = make(chan struct{})
}
