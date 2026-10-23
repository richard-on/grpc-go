//go:build ignore

// Linux-only diagnostic for grpc-go issue 9371. Uses the standard library only.
// Five logical streams share one TCP connection. Each makes five request/reply
// exchanges. Frames use separate 9-byte header and <=16KiB payload writes, and
// matching ReadFull calls, without HTTP/2 flow control or gRPC implementation.
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const streams = 5
const exchanges = 5
const frameSize = 16384
const messageSize = 1048576 + 5

type endpoint struct {
	conn      *net.TCPConn
	send      chan int
	received  [streams]chan struct{}
	bytesRead atomic.Int64
}

type sample struct {
	Client int `json:"client"`
	Server int `json:"server"`
}

type result struct {
	Iteration int           `json:"iteration"`
	Elapsed   time.Duration `json:"elapsed_ns"`
	Procs     int           `json:"procs"`
	Start     sample        `json:"rcvbuf_start"`
	Minimum   sample        `json:"rcvbuf_min"`
	Final     sample        `json:"rcvbuf_final"`
	ReadBytes [2]int64      `json:"read_bytes_client_server"`
	Error     string        `json:"error,omitempty"`
}

func rcvbuf(c *net.TCPConn) (int, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var n int
	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		n, sockErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	}); err != nil {
		return 0, err
	}
	return n, sockErr
}

func snapshot(client, server *net.TCPConn) (sample, error) {
	c, err := rcvbuf(client)
	if err != nil {
		return sample{}, err
	}
	s, err := rcvbuf(server)
	return sample{Client: c, Server: s}, err
}

func (e *endpoint) read(readBuffer int) error {
	var r io.Reader = e.conn
	if readBuffer > 0 {
		r = bufio.NewReaderSize(r, readBuffer)
	}
	var header [9]byte
	var payload [frameSize]byte
	var received [streams]int
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return err
		}
		n := int(header[0])<<16 | int(header[1])<<8 | int(header[2])
		id := binary.BigEndian.Uint32(header[5:])
		if n < 1 || n > frameSize || id < 1 || id > streams*2-1 || id%2 != 1 || header[3] != 0 || header[4] != 0 {
			return fmt.Errorf("invalid frame: %x", header)
		}
		if _, err := io.ReadFull(r, payload[:n]); err != nil {
			return err
		}
		e.bytesRead.Add(int64(n + len(header)))
		i := int(id / 2)
		received[i] += n
		if received[i] > messageSize {
			return fmt.Errorf("stream %d: received %d bytes in a message", id, received[i])
		}
		if received[i] == messageSize {
			received[i] = 0
			e.received[i] <- struct{}{}
		}
	}
}

func (e *endpoint) write(ctx context.Context, writeBuffer int) error {
	var w io.Writer = e.conn
	var buffered *bufio.Writer
	if writeBuffer > 0 {
		buffered = bufio.NewWriterSize(w, writeBuffer)
		w = buffered
	}
	payload := make([]byte, messageSize)
	binary.BigEndian.PutUint32(payload[1:5], messageSize-5)
	type pending struct{ id, offset int }
	var active []pending
	var header [9]byte
	for {
		if len(active) == 0 {
			if buffered != nil {
				if err := buffered.Flush(); err != nil {
					return err
				}
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case id := <-e.send:
				active = append(active, pending{id: id})
			}
		}
	collect:
		for {
			select {
			case id := <-e.send:
				active = append(active, pending{id: id})
			default:
				break collect
			}
		}
		p := active[0]
		active = active[1:]
		n := min(frameSize, messageSize-p.offset)
		header[0], header[1], header[2] = byte(n>>16), byte(n>>8), byte(n)
		binary.BigEndian.PutUint32(header[5:], uint32(p.id*2+1))
		if _, err := w.Write(header[:]); err != nil {
			return err
		}
		if _, err := w.Write(payload[p.offset : p.offset+n]); err != nil {
			return err
		}
		p.offset += n
		if p.offset != messageSize {
			active = append(active, p)
		}
	}
}

func run(iteration, readBuffer, writeBuffer, receiveBuffer int, timeout, interval time.Duration) (out result) {
	out.Iteration, out.Procs = iteration, runtime.GOMAXPROCS(0)
	started := time.Now()
	defer func() { out.Elapsed = time.Since(started) }()
	fail := func(err error) { out.Error = err.Error() }
	l, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		fail(err)
		return
	}
	defer l.Close()
	c, err := net.DialTCP("tcp4", nil, l.Addr().(*net.TCPAddr))
	if err != nil {
		fail(err)
		return
	}
	defer c.Close()
	s, err := l.AcceptTCP()
	if err != nil {
		fail(err)
		return
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for _, conn := range []*net.TCPConn{c, s} {
		if err := conn.SetDeadline(started.Add(timeout)); err != nil {
			fail(err)
			return
		}
		if receiveBuffer > 0 {
			if err := conn.SetReadBuffer(receiveBuffer); err != nil {
				fail(err)
				return
			}
		}
	}
	if out.Start, err = snapshot(c, s); err != nil {
		fail(err)
		return
	}
	out.Minimum, out.Final = out.Start, out.Start
	var endpoints [2]*endpoint
	var workers sync.WaitGroup
	errors := make(chan error, 16)
	start := func(f func() error) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := f(); err != nil {
				errors <- err
			}
		}()
	}
	for i, conn := range []*net.TCPConn{c, s} {
		e := &endpoint{conn: conn, send: make(chan int, streams)}
		for j := range e.received {
			e.received[j] = make(chan struct{}, 1)
		}
		endpoints[i] = e
		start(func() error { return e.read(readBuffer) })
		start(func() error { return e.write(ctx, writeBuffer) })
	}
	client, server := endpoints[0], endpoints[1]
	var clients sync.WaitGroup
	for id := 0; id < streams; id++ {
		clients.Add(1)
		start(func() error {
			defer clients.Done()
			for range exchanges {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case client.send <- id:
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-client.received[id]:
				}
			}
			return nil
		})
		start(func() error {
			for range exchanges {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-server.received[id]:
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case server.send <- id:
				}
			}
			return nil
		})
	}
	completed := make(chan struct{})
	go func() { clients.Wait(); close(completed) }()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	update := func() {
		state, err := snapshot(c, s)
		if err != nil {
			fail(err)
			return
		}
		out.Minimum.Client = min(out.Minimum.Client, state.Client)
		out.Minimum.Server = min(out.Minimum.Server, state.Server)
		out.Final = state
	}
loop:
	for {
		select {
		case <-completed:
			break loop
		case err := <-errors:
			fail(err)
			break loop
		case <-ctx.Done():
			fail(ctx.Err())
			break loop
		case <-ticker.C:
			update()
		}
	}
	update()
	cancel()
	c.Close()
	s.Close()
	workers.Wait()
	out.ReadBytes = [2]int64{client.bytesRead.Load(), server.bytesRead.Load()}
	const expected = streams * exchanges * (messageSize + 9*((messageSize+frameSize-1)/frameSize))
	if out.Error == "" && (out.ReadBytes[0] != expected || out.ReadBytes[1] != expected) {
		fail(fmt.Errorf("read bytes %v, expected %d in each direction", out.ReadBytes, expected))
	}
	return
}

func main() {
	count := flag.Int("count", 1, "number of fresh TCP connections")
	procs := flag.Int("procs", runtime.GOMAXPROCS(0), "GOMAXPROCS")
	readBuffer := flag.Int("readbuf", 0, "bufio read buffer at both endpoints; zero means raw TCP")
	writeBuffer := flag.Int("writebuf", 0, "bufio write buffer at both endpoints; zero means separate raw frame writes")
	receiveBuffer := flag.Int("rcvbuf", 0, "SO_RCVBUF request at both endpoints; zero preserves autotuning")
	timeout := flag.Duration("timeout", 30*time.Second, "per-connection timeout")
	interval := flag.Duration("sample", 10*time.Millisecond, "SO_RCVBUF sampling interval")
	flag.Parse()
	if *count < 1 || *procs < 1 || *interval <= 0 || *timeout <= 0 || *readBuffer < 0 || *writeBuffer < 0 || *receiveBuffer < 0 {
		fmt.Fprintln(os.Stderr, "invalid flag value")
		os.Exit(2)
	}
	runtime.GOMAXPROCS(*procs)
	encoder := json.NewEncoder(os.Stdout)
	for i := 0; i < *count; i++ {
		r := run(i, *readBuffer, *writeBuffer, *receiveBuffer, *timeout, *interval)
		if err := encoder.Encode(r); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if r.Error != "" {
			os.Exit(1)
		}
	}
}
