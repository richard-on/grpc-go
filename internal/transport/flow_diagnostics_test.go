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
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Linux installs a read-only getsockopt implementation before tests start.
var flowReceiveBuffer = func(net.Conn) (int, error) {
	return 0, fmt.Errorf("SO_RCVBUF unavailable on %s", runtime.GOOS)
}

func configureFlowDiagnostics(t *testing.T, server *ServerConfig, client *ConnectOptions) string {
	if os.Getenv("GRPC_FLOW_DIAGNOSTICS") != "1" {
		return ""
	}
	mode := os.Getenv("GRPC_FLOW_MODE")
	if mode == "" {
		mode = "baseline"
	}
	switch mode {
	case "baseline", "socket-buffer":
	case "read-buffer":
		server.ReadBufferSize, client.ReadBufferSize = 32<<10, 32<<10
	case "read-write-buffer":
		server.ReadBufferSize, client.ReadBufferSize = 32<<10, 32<<10
		server.WriteBufferSize, client.WriteBufferSize = 32<<10, 32<<10
	default:
		t.Fatalf("unknown GRPC_FLOW_MODE %q", mode)
	}
	fmt.Fprintf(os.Stderr, "FLOW_CONTROL_START test=%s mode=%s gomaxprocs=%d server_read_buffer=%d client_read_buffer=%d server_write_buffer=%d client_write_buffer=%d\n", t.Name(), mode, runtime.GOMAXPROCS(0), server.ReadBufferSize, client.ReadBufferSize, server.WriteBufferSize, client.WriteBufferSize)
	return mode
}

func prepareFlowSockets(t *testing.T, mode string, client *http2Client, server *http2Server) {
	if mode == "" {
		return
	}
	for _, endpoint := range []struct {
		side   string
		conn   net.Conn
		framer *framer
	}{{"client", client.conn, client.framer}, {"server", server.conn, server.framer}} {
		before, beforeErr := flowReceiveBuffer(endpoint.conn)
		requested := 0
		if mode == "socket-buffer" {
			conn, ok := endpoint.conn.(*net.TCPConn)
			if !ok {
				t.Fatalf("socket-buffer control requires TCP, got %T", endpoint.conn)
			}
			requested = 1 << 20
			if err := conn.SetReadBuffer(requested); err != nil {
				t.Fatalf("%s SetReadBuffer(%d): %v", endpoint.side, requested, err)
			}
		}
		after, afterErr := flowReceiveBuffer(endpoint.conn)
		fmt.Fprintf(os.Stderr, "FLOW_CONTROL_SOCKET test=%s mode=%s side=%s local=%s remote=%s requested_rcvbuf=%d before_rcvbuf=%d before_error=%v so_rcvbuf=%d error=%v reader=%T writer_batch=%d\n", t.Name(), mode, endpoint.side, endpoint.conn.LocalAddr(), endpoint.conn.RemoteAddr(), requested, before, beforeErr, after, afterErr, endpoint.framer.reader, endpoint.framer.writer.batchSize)
		if runtime.GOOS == "linux" && afterErr != nil {
			t.Fatalf("%s SO_RCVBUF readback: %v", endpoint.side, afterErr)
		}
	}
}

// Local investigation only. Snapshots are individually synchronized, not atomic
// across the transport. Never read loopy-owned fields from the watchdog.
func startFlowDiagnostics(name string, client *http2Client, server *http2Server, streams []*ClientStream) (func(uint32, int, string), func()) {
	if os.Getenv("GRPC_FLOW_DIAGNOSTICS") != "1" {
		return func(uint32, int, string) {}, func() {}
	}
	type progress struct {
		message int
		phase   string
		at      time.Time
	}
	started := time.Now()
	var mu sync.Mutex
	states := make(map[uint32]progress)
	for _, stream := range streams {
		states[stream.id] = progress{phase: "created", at: started}
	}
	update := func(id uint32, message int, phase string) {
		mu.Lock()
		states[id] = progress{message, phase, time.Now()}
		mu.Unlock()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			stacks := make([]byte, 2<<20)
			n := runtime.Stack(stacks, true)
			fmt.Fprintf(os.Stderr, "FLOW_CONTROL_DIAGNOSTIC test=%s elapsed=%s stack_bytes=%d stack_capacity=%d\n%s\n", name, time.Since(started), n, len(stacks), stacks[:n])
			fmt.Fprintf(os.Stderr, "client done=%v reader_done=%v writer_done=%v inflow=%d\n", flowDone(client.ctxDone), flowDone(client.readerDone), flowDone(client.writerDone), client.fc.getSize())
			if client.mu.TryLock() {
				state, active := client.state, len(client.activeStreams)
				client.mu.Unlock()
				fmt.Fprintf(os.Stderr, "client state=%v active=%d\n", state, active)
			}
			fmt.Fprintf(os.Stderr, "server done=%v reader_done=%v writer_done=%v inflow=%d last_read_ago=%s\n", flowDone(server.done), flowDone(server.readerDone), flowDone(server.loopyWriterDone), server.fc.getSize(), time.Since(time.Unix(0, atomic.LoadInt64(&server.lastRead))))
			var serverStreams []*ServerStream
			if server.mu.TryLock() {
				state, active := server.state, len(server.activeStreams)
				for _, stream := range server.activeStreams {
					serverStreams = append(serverStreams, stream)
				}
				server.mu.Unlock()
				fmt.Fprintf(os.Stderr, "server state=%v active=%d\n", state, active)
			}
			flowControlSnapshot("client", client.controlBuf)
			flowControlSnapshot("server", server.controlBuf)
			for _, endpoint := range []struct {
				side string
				conn net.Conn
			}{{"client", client.conn}, {"server", server.conn}} {
				rcvbuf, err := flowReceiveBuffer(endpoint.conn)
				fmt.Fprintf(os.Stderr, "%s so_rcvbuf=%d error=%v\n", endpoint.side, rcvbuf, err)
			}
			for _, stream := range streams {
				mu.Lock()
				p := states[stream.id]
				mu.Unlock()
				fmt.Fprintf(os.Stderr, "client stream=%d message=%d phase=%s idle=%s done=%v %s\n", stream.id, p.message, p.phase, time.Since(p.at), flowDone(stream.done), flowStreamSnapshot(&stream.Stream))
			}
			for _, stream := range serverStreams {
				// activeStreams insertion precedes wq initialization. Sending the
				// response headers publishes initialized fields through this atomic.
				if stream.headerSent.Load() {
					fmt.Fprintf(os.Stderr, "server stream=%d header_sent=true %s\n", stream.id, flowStreamSnapshot(&stream.Stream))
				} else {
					fmt.Fprintf(os.Stderr, "server stream=%d header_sent=false\n", stream.id)
				}
			}
			flowSocketSnapshot(ctx, client.conn)
		}
	}()
	return update, func() { cancel(); <-done }
}

func flowDone(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func flowControlSnapshot(side string, buffer *controlBuffer) {
	if buffer.mu.TryLock() {
		queued, responses, waiting, closed := buffer.list.count, buffer.transportResponseFrames, buffer.consumerWaiting, buffer.closed
		buffer.mu.Unlock()
		fmt.Fprintf(os.Stderr, "%s control queued=%d responses=%d waiting=%v wakeup=%d closed=%v throttled=%v\n", side, queued, responses, waiting, len(buffer.wakeupCh), closed, buffer.trfChan.Load() != nil)
	}
}

func flowStreamSnapshot(stream *Stream) string {
	buf := "recv_mu=busy"
	if stream.buf.mu.TryLock() {
		buf = fmt.Sprintf("recv_channel=%d backlog=%d recv_error=%v", len(stream.buf.c), len(stream.buf.backlog), stream.buf.err)
		stream.buf.mu.Unlock()
	}
	fc := "inflow_mu=busy"
	if stream.fc.mu.TryLock() {
		fc = fmt.Sprintf("limit=%d pending_data=%d pending_update=%d delta=%d", stream.fc.limit, stream.fc.pendingData, stream.fc.pendingUpdate, stream.fc.delta)
		stream.fc.mu.Unlock()
	}
	return fmt.Sprintf("state=%v context=%v quota=%d quota_signal=%d quota_done=%v %s %s", stream.getState(), stream.ctx.Err(), atomic.LoadInt32(&stream.wq.quota), len(stream.wq.ch), flowDone(stream.wq.done), buf, fc)
}

func flowSocketSnapshot(ctx context.Context, conn net.Conn) {
	local, lok := conn.LocalAddr().(*net.TCPAddr)
	remote, rok := conn.RemoteAddr().(*net.TCPAddr)
	if !lok || !rok || !local.IP.IsLoopback() || !remote.IP.IsLoopback() {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	filter := fmt.Sprintf("( src %s and dst %s ) or ( src %s and dst %s )", local, remote, remote, local)
	out, err := exec.CommandContext(ctx, "ss", "-tinpm", filter).CombinedOutput()
	if len(out) > 16<<10 {
		out = out[:16<<10]
	}
	fmt.Fprintf(os.Stderr, "tcp_info error=%v\n%s\n", err, out)
}
