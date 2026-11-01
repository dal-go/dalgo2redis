package dalgo2redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This loopback peer deliberately withholds a dispatched GET response. It proves
// interruption of a real socket read, rather than just cancellation before I/O.
func transportPeer(t *testing.T, read func([]string) string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var workers sync.WaitGroup
	connections := make(map[net.Conn]bool)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections[conn] = true
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() {
					_ = conn.Close()
					mu.Lock()
					delete(connections, conn)
					mu.Unlock()
				}()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				reader := bufio.NewReader(conn)
				for {
					args, err := peerCommand(reader)
					if err != nil {
						return
					}
					reply := "+OK\r\n"
					switch strings.ToUpper(args[0]) {
					case "HELLO":
						reply = "-ERR unknown command HELLO\r\n"
					case "PING":
						reply = "+PONG\r\n"
					case "GET":
						reply = read(args)
					}
					if _, err := io.WriteString(conn, reply); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		mu.Lock()
		for conn := range connections {
			_ = conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return "redis://" + listener.Addr().String() + "/0?pool_size=1"
}

func peerCommand(reader *bufio.Reader) ([]string, error) {
	header, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(header, "*")))
	if err != nil || count < 1 || count > 130 {
		return nil, fmt.Errorf("invalid fixture command")
	}
	args := make([]string, count)
	for i := range args {
		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(header, "$")))
		if err != nil || size < 0 || size > 65536 {
			return nil, fmt.Errorf("invalid fixture argument")
		}
		value := make([]byte, size+2)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		args[i] = string(value[:size])
	}
	return args, nil
}

func assertReadsReleased(t *testing.T, client *Client) {
	t.Helper()
	client.reads.mu.Lock()
	active := len(client.reads.active)
	client.reads.mu.Unlock()
	if active != 0 || len(client.reads.slots) != 0 {
		t.Fatalf("read resources retained: active=%d slots=%d", active, len(client.reads.slots))
	}
}

func TestReadTransportAfterDispatchCancellationAndReopen(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%v", deadline), func(t *testing.T) {
			dispatched := make(chan struct{})
			var first atomic.Bool
			first.Store(true)
			raw := transportPeer(t, func([]string) string {
				if first.Swap(false) {
					close(dispatched)
					return "" // Keep the connection open without a response.
				}
				return "$2\r\nok\r\n"
			})
			client, err := Open(context.Background(), raw)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithCancel(context.Background())
			want := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
				want = context.DeadlineExceeded
			}
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := client.QueryNative(ctx, "GET", "key")
				result <- err
			}()
			select {
			case <-dispatched:
			case <-time.After(2 * time.Second):
				t.Fatal("query never dispatched")
			}
			if !deadline {
				cancel()
			}
			select {
			case err := <-result:
				if !errors.Is(err, want) {
					t.Fatalf("wrong cancellation: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("in-flight read did not promptly cancel")
			}
			assertReadsReleased(t, client)
			value, err := client.QueryNative(context.Background(), "GET", "key")
			if err != nil {
				t.Fatal(err)
			}
			if bytes, ok := value.(NativeBytes); !ok || string(bytes) != "ok" {
				t.Fatalf("read after cancellation: %#v", value)
			}
			assertReadsReleased(t, client)
		})
	}
}

func TestReadTransportAdmissionAndClientClose(t *testing.T) {
	dispatched := make(chan struct{})
	var commands atomic.Int64
	raw := transportPeer(t, func([]string) string {
		if commands.Add(1) == 1 {
			close(dispatched)
		}
		return ""
	})
	client, err := Open(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	first := make(chan error, 1)
	go func() {
		_, err := client.Query(context.Background(), "GET", "first")
		first <- err
	}()
	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("first read did not dispatch")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := client.Query(ctx, "GET", "queued"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued read did not obey caller deadline: %v", err)
	}
	if commands.Load() != 1 {
		t.Fatal("pool_size=1 admitted concurrent drivers")
	}
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Client.Close left active read alive")
	}
	select {
	case err := <-first:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("closed read returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("closed read did not finish")
	}
	assertReadsReleased(t, client)
	if _, err := client.QueryNative(context.Background(), "GET", "after-close"); err == nil {
		t.Fatal("closed client accepted a read")
	}
	if commands.Load() != 1 {
		t.Fatal("closed client sent another command")
	}
}
