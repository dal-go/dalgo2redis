package dalgo2redis

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type nativeResultHook struct {
	process func(context.Context, redis.Cmder) error
}

func (h nativeResultHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h nativeResultHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h nativeResultHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return h.process
}

func resultClient(t *testing.T, result any, onResult func(context.Context)) *Client {
	t.Helper()
	driver := redis.NewClient(&redis.Options{})
	t.Cleanup(func() { _ = driver.Close() })
	driver.AddHook(nativeResultHook{process: func(ctx context.Context, command redis.Cmder) error {
		if onResult != nil {
			onResult(ctx)
		}
		command.(*redis.Cmd).SetVal(result)
		return nil
	}})
	return &Client{client: driver, options: Options{ScanPrefix: "tenant:"}}
}

func TestNativeLosslessJSON(t *testing.T) {
	for _, tc := range []struct {
		command string
		value   any
		want    string
	}{
		{"GET", "\x00\xff\x80\r\n", `{"kind":"bytes","base64":"AP+ADQo="}`},
		{"GET", "", `{"kind":"bytes","base64":""}`},
		{"GET", nil, "null"},
		{"TYPE", "string", `"string"`},
		{"PTTL", int64(9223372036854775807), `{"kind":"integer","value":"9223372036854775807"}`},
		{"TTL", int64(-9223372036854775808), `{"kind":"integer","value":"-9223372036854775808"}`},
		{"MGET", []any{"", nil, "\xff"}, `{"kind":"array","values":[{"kind":"bytes","base64":""},null,{"kind":"bytes","base64":"/w=="}]}`},
		{"SCAN", []any{"17", []any{}}, `{"kind":"array","values":[{"kind":"bytes","base64":"MTc="},{"kind":"array","values":[]}]}`},
		{"SCAN", []any{"0", []any{"tenant:a", "tenant:a"}}, `{"kind":"array","values":[{"kind":"bytes","base64":"MA=="},{"kind":"array","values":[{"kind":"bytes","base64":"dGVuYW50OmE="},{"kind":"bytes","base64":"dGVuYW50OmE="}]}]}`},
	} {
		t.Run(tc.command+"/"+tc.want, func(t *testing.T) {
			value, err := nativeReply(tc.command, tc.value)
			if err != nil {
				t.Fatal(err)
			}
			got, err := json.Marshal(value)
			if err != nil || string(got) != tc.want {
				t.Fatalf("JSON %s error %v; want %s", got, err, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		command string
		value   any
	}{
		{"GET", int64(1)}, {"TYPE", "\xff"}, {"TTL", "123"},
		{"SCAN", []any{"0", []any{nil}}}, {"MGET", []any{int64(1)}},
	} {
		if _, err := nativeReply(tc.command, tc.value); err == nil {
			t.Fatalf("wrong reply shape accepted for %s", tc.command)
		}
	}
}

func TestQueryNativeAndLegacyKeepSeparateContracts(t *testing.T) {
	client := resultClient(t, "\xff", nil)
	legacy, err := client.Query(context.Background(), "GET", "key")
	if err != nil || legacy != "\xff" {
		t.Fatalf("legacy result changed: %#v, %v", legacy, err)
	}
	native, err := client.QueryNative(context.Background(), "GET", "key")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := native.(NativeBytes); !ok || string(got) != "\xff" {
		t.Fatalf("native bytes: %#v", native)
	}
}

func TestQueryRejectsLateSuccessAfterCallerCancellation(t *testing.T) {
	for _, native := range []bool{false, true} {
		for _, result := range []any{nil, "late"} {
			ctx, cancel := context.WithCancel(context.Background())
			client := resultClient(t, result, func(context.Context) { cancel() })
			var err error
			if native {
				_, err = client.QueryNative(ctx, "GET", "key")
			} else {
				_, err = client.Query(ctx, "GET", "key")
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("late success delivered (native=%v): %v", native, err)
			}
		}
	}
}

func TestQueryRejectsLateSuccessAfterDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	client := resultClient(t, "late", func(ctx context.Context) { <-ctx.Done() })
	if _, err := client.QueryNative(ctx, "GET", "key"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late deadline success: %v", err)
	}
}

func TestQueryChecksCancellationAfterConversion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := resultClient(t, "reply", nil)
	result, err := client.query(ctx, []string{"GET", "key"}, func(string, any) (any, error) {
		cancel()
		return "converted", nil
	})
	if result != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("converted result escaped final cancellation guard: %#v, %v", result, err)
	}
}

func TestQueryRejectsForeignScanKeysInBothModes(t *testing.T) {
	for _, native := range []bool{false, true} {
		client := resultClient(t, []any{"0", []any{"tenant:a", "foreign-secret"}}, nil)
		var result any
		var err error
		if native {
			result, err = client.QueryNative(context.Background(), "SCAN", "0")
		} else {
			result, err = client.Query(context.Background(), "SCAN", "0")
		}
		if err == nil || result != nil {
			t.Fatalf("foreign SCAN page delivered: %#v, %v", result, err)
		}
	}
}
