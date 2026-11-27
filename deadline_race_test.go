package dalgo2redis

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// laggingDeadline reports a deadline but never closes Done, standing in for the
// window after a socket deadline fires and before the context's own timer marks
// it done. A child of it inherits the deadline and relies on it for cancellation.
type laggingDeadline struct {
	context.Context
	at time.Time
}

func (c laggingDeadline) Deadline() (time.Time, bool) { return c.at, true }

func newLaggingDeadline(after time.Duration) laggingDeadline {
	return laggingDeadline{Context: context.Background(), at: time.Now().Add(after)}
}

func sleepUntil(at time.Time) { time.Sleep(time.Until(at) + time.Millisecond) }

func TestQueryReportsADeadlineThatOutrunsTheContextTimer(t *testing.T) {
	ctx := newLaggingDeadline(20 * time.Millisecond)
	driver := redis.NewClient(&redis.Options{})
	t.Cleanup(func() { _ = driver.Close() })
	driver.AddHook(nativeResultHook{process: func(context.Context, redis.Cmder) error {
		sleepUntil(ctx.at)
		return &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	}})
	client := &Client{client: driver}
	for _, native := range []bool{false, true} {
		var err error
		if native {
			_, err = client.QueryNative(ctx, "GET", "key")
		} else {
			_, err = client.Query(ctx, "GET", "key")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("native=%v: socket timeout at the deadline reported as %v", native, err)
		}
	}
}

func TestQueryRejectsConversionFinishingAfterTheDeadline(t *testing.T) {
	ctx := newLaggingDeadline(20 * time.Millisecond)
	client := resultClient(t, "reply", nil)
	_, err := client.query(ctx, []string{"GET", "key"}, func(_ string, v any) (any, error) {
		sleepUntil(ctx.at)
		return v, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("conversion past the deadline delivered: %v", err)
	}
}

func TestOpenReportsADeadlineThatOutrunsTheContextTimer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	ctx := newLaggingDeadline(100 * time.Millisecond)
	client, err := Open(ctx, "redis://"+listener.Addr().String()+"/0")
	if client != nil {
		_ = client.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ping timing out at the deadline reported as %v", err)
	}
}
