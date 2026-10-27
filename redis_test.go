package dalgo2redis

import (
	"context"
	"errors"
	"github.com/dal-go/record"
	"github.com/redis/go-redis/v9"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestValidateCommand(t *testing.T) {
	for _, args := range [][]string{nil, {"SET", "x", "y"}, {"EVAL", "return 1", "0"}, {"KEYS", "*"}, {"LRANGE", "x", "0", "-1"}, {"GET", "x", "y"}, {"HMGET", "x"}, {"HGET", "x"}, {"GET", strings.Repeat("x", 65536)}} {
		if ValidateCommand(args) == nil {
			t.Fatalf("accepted %q", args)
		}
	}
	for _, args := range [][]string{{"GET", "x"}, {"mget", "x", "y"}, {"HGET", "x", "f"}, {"HMGET", "x", "a", "b"}, {"EXISTS", "x"}, {"TTL", "x"}} {
		if e := ValidateCommand(args); e != nil {
			t.Fatal(e)
		}
	}
}
func TestRedisIntegration(t *testing.T) {
	raw := os.Getenv("REDIS_TEST_URL")
	if raw == "" {
		t.Skip("set REDIS_TEST_URL to run against a real Redis server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	opts, e := redis.ParseURL(raw)
	if e != nil {
		t.Fatal(e)
	}
	seed := redis.NewClient(opts)
	defer seed.Close()
	prefix := "dalgo-test-" + time.Now().Format("150405.000000000")
	key, hash := prefix+":str", prefix+":hash"
	defer seed.Del(context.Background(), key, hash)
	if e = seed.Set(ctx, key, "hello", time.Minute).Err(); e != nil {
		t.Fatal(e)
	}
	if e = seed.HSet(ctx, hash, "name", "Ada", "role", "reader").Err(); e != nil {
		t.Fatal(e)
	}
	c, e := Open(ctx, raw)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	for _, tc := range []struct {
		args []string
		want any
	}{
		{[]string{"GET", key}, "hello"}, {[]string{"GET", prefix + ":absent"}, nil},
		{[]string{"MGET", key, prefix + ":absent"}, []any{"hello", nil}},
		{[]string{"HGET", hash, "name"}, "Ada"}, {[]string{"HMGET", hash, "name", "missing"}, []any{"Ada", nil}},
		{[]string{"TYPE", hash}, "hash"}, {[]string{"EXISTS", key}, int64(1)},
	} {
		got, e := c.Query(ctx, tc.args...)
		if e != nil || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%v got %#v %v want %#v", tc.args[0], got, e, tc.want)
		}
	}
	if _, e = c.Query(ctx, "SET", key, "changed"); e == nil {
		t.Fatal("write allowed")
	}
	if value := seed.Get(ctx, key).Val(); value != "hello" {
		t.Fatal("write changed data")
	}
	data := map[string]any{}
	r := record.NewRecordWithData(record.NewKeyWithID("strings", key), &data)
	if e = c.DB().Get(ctx, r); e != nil || data["value"] != "hello" {
		t.Fatalf("DALgo get: %v %v", data, e)
	}
	if _, e = c.Query(ctx, "GET", hash); e == nil {
		t.Fatal("wrong type accepted")
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if _, e = c.Query(cancelled, "GET", key); e == nil {
		t.Fatal("cancellation ignored")
	}
	// Oversized real replies must fail before driver parsing and leave the pool usable.
	if e = seed.Set(ctx, key, strings.Repeat("x", MaxResponseBytes), time.Minute).Err(); e != nil {
		t.Fatal(e)
	}
	if _, e = c.Query(ctx, "GET", key); !errors.Is(e, errResponseBudget) {
		t.Fatalf("oversize response: %v", e)
	}
	if e = seed.Set(ctx, key, "hello", time.Minute).Err(); e != nil {
		t.Fatal(e)
	}
	if v, e := c.Query(ctx, "GET", key); e != nil || v != "hello" {
		t.Fatalf("pool recovery: %v %v", v, e)
	}
	// Verify real Redis ACL authentication and denial, beyond client-side guards.
	username := prefix + "-reader"
	password := "test-only-password"
	if e = seed.Do(ctx, "ACL", "SETUSER", username, "reset", "on", ">"+password, "~"+prefix+":*", "+@connection", "+get", "+mget").Err(); e != nil {
		t.Fatal(e)
	}
	defer seed.Do(context.Background(), "ACL", "DELUSER", username)
	endpoint, e := url.Parse(raw)
	if e != nil {
		t.Fatal(e)
	}
	endpoint.User = url.UserPassword(username, password)
	reader, e := Open(ctx, endpoint.String())
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	if v, e := reader.Query(ctx, "GET", key); e != nil || v != "hello" {
		t.Fatalf("ACL read: %v %v", v, e)
	}
	if _, e = reader.Query(ctx, "GET", "outside-allowed-prefix"); e == nil {
		t.Fatal("ACL key restriction ignored")
	}
	if _, e = reader.Query(ctx, "HGET", hash, "name"); e == nil {
		t.Fatal("ACL command restriction ignored")
	}
	endpoint.User = url.UserPassword(username, "wrong-password-secret")
	if _, e = Open(ctx, endpoint.String()); e == nil || strings.Contains(e.Error(), "wrong-password-secret") {
		t.Fatalf("unsafe auth error: %v", e)
	}

}
