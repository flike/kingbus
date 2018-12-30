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

package server

import "errors"

var (
	//ErrStopped return for kingbus server stopped
	ErrStopped = errors.New("kingbus: server stopped")
	//ErrStarted return for kingbus server started
	ErrStarted = errors.New("kingbus: server already started")
	//ErrCanceled return for request be cancelled
	ErrCanceled = errors.New("kingbus: request cancelled")
	//ErrTimeout return for request timed out
	ErrTimeout = errors.New("kingbus: request timed out")
	//ErrTimeoutDueToLeaderFail return for request timed out,possibly due to previous leader failure
	ErrTimeoutDueToLeaderFail = errors.New("kingbus: request timed out, possibly due to previous leader failure")
	//ErrTimeoutDueToConnectionLost return for request timed out,possibly due to connection lost
	ErrTimeoutDueToConnectionLost = errors.New("kingbus: request timed out, possibly due to connection lost")
	//ErrNotEnoughStartedMembers return for re-configuration failed due to not enough started members
	ErrNotEnoughStartedMembers = errors.New("kingbus: re-configuration failed due to not enough started members")
	//ErrRequestTooLarge return for request is too large
	ErrRequestTooLarge = errors.New("kingbus: request is too large")
	//ErrNoSpace return for no space
	ErrNoSpace = errors.New("kingbus: no space")
	//ErrTooManyRequests return for too many requests
	ErrTooManyRequests = errors.New("kingbus: too many requests")
	//ErrUnhealthy return for unhealthy cluster
	ErrUnhealthy = errors.New("kingbus: unhealthy cluster")
	//ErrKeyNotFound return for key not found
	ErrKeyNotFound = errors.New("kingbus: key not found")
	//ErrCorrupt return for corrupt cluster
	ErrCorrupt = errors.New("kingbus: corrupt cluster")
	//ErrUnsupport return for apply unsupport event type
	ErrUnsupport = errors.New("apply unsupport event type")
	//ErrArgs return for args are not available
	ErrArgs = errors.New("binlog_server:args are not available")
	//ErrNotContain return for slave gtidset not contain in master
	ErrNotContain = errors.New("binlog_server:slave gtidset not contain in master")
	//ErrUUIDIsNull return for the uuid of server is null
	ErrUUIDIsNull = errors.New("binlog_server:the uuid of server is null")
)
