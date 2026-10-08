/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package transport

import (
	"fmt"
	"net"
	"syscall"
)

func init() {
	flowReceiveBuffer = func(conn net.Conn) (int, error) {
		sc, ok := conn.(syscall.Conn)
		if !ok {
			return 0, fmt.Errorf("%T does not expose SyscallConn", conn)
		}
		raw, err := sc.SyscallConn()
		if err != nil {
			return 0, err
		}
		var size int
		var sockErr error
		if err := raw.Control(func(fd uintptr) {
			size, sockErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
		}); err != nil {
			return 0, err
		}
		return size, sockErr
	}
}
