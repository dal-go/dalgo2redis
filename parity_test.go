package dalgo2redis

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Uses only an explicitly supplied disposable fixture. Production endpoints and
// credentials are never inferred. Existing integration tests retain their own ACL
// and oversize/pool-recovery coverage.
func TestRedisNativeParityIntegration(t *testing.T) {
	raw := os.Getenv("REDIS_TEST_URL")
	if raw == "" {
		t.Skip("set REDIS_TEST_URL to run against a disposable Redis fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	opts, err := redis.ParseURL(raw)
	if err != nil {
		t.Fatal("invalid fixture URL")
	}
	admin := redis.NewClient(opts)
	defer admin.Close()
	prefix := fmt.Sprintf("dalgo-parity-%d:", time.Now().UnixNano())
	binary := []byte{0, 255, 128, 13, 10}
	keys := []string{prefix + "binary", prefix + "integer", prefix + "zset"}
	for i := 0; i < 31; i++ {
		keys = append(keys, fmt.Sprintf("%sitem:%d", prefix, i))
	}
	foreign := "foreign-" + prefix
	defer admin.Del(context.Background(), append(append([]string(nil), keys...), foreign)...)
	for _, key := range append(append([]string(nil), keys...), foreign) {
		if err := admin.Set(ctx, key, "owned", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := admin.Set(ctx, keys[0], binary, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := admin.Set(ctx, keys[1], "9223372036854775000", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := admin.Do(ctx, "PEXPIREAT", keys[1], "9223372036854775000").Err(); err != nil {
		t.Fatal(err)
	}
	if err := admin.Del(ctx, keys[2]).Err(); err != nil {
		t.Fatal(err)
	}
	if err := admin.ZAdd(ctx, keys[2], redis.Z{Score: 1.5, Member: "member"}).Err(); err != nil {
		t.Fatal(err)
	}
	username, password := prefix+"reader", "fixture-only-password"
	if err := admin.Do(ctx, "ACL", "SETUSER", username, "reset", "on", ">"+password, "~"+prefix+"*",
		"+@connection", "+get", "+mget", "+pttl", "+scan", "+zscore").Err(); err != nil {
		t.Fatal(err)
	}
	defer admin.Do(context.Background(), "ACL", "DELUSER", username)
	endpoint, err := url.Parse(raw)
	if err != nil {
		t.Fatal("invalid fixture URL")
	}
	endpoint.User = url.UserPassword(username, password)
	reader, err := OpenWithOptions(ctx, endpoint.String(), Options{ScanPrefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	value, err := reader.QueryNative(ctx, "GET", keys[0])
	if err != nil {
		t.Fatal(err)
	}
	bulk, ok := value.(NativeBytes)
	if !ok || !bytes.Equal(bulk, binary) {
		t.Fatalf("native binary lost: %#v", value)
	}
	value, err = reader.QueryNative(ctx, "PTTL", keys[1])
	if err != nil {
		t.Fatal(err)
	}
	integer, ok := value.(NativeInteger)
	if !ok || int64(integer) <= 9007199254740991 {
		t.Fatalf("native int64 lost: %#v", value)
	}
	value, err = reader.QueryNative(ctx, "ZSCORE", keys[2], "member")
	if err != nil {
		t.Fatal(err)
	}
	score, ok := value.(NativeBytes)
	if !ok || string(score) != "1.5" {
		t.Fatalf("ZSCORE: %#v", value)
	}
	cursor, pages := "0", 0
	seen := make(map[string]bool)
	for {
		pages++
		if pages > 256 {
			t.Fatal("fixture whole-scan work budget exceeded")
		}
		value, err := reader.QueryNative(ctx, "SCAN", cursor, "COUNT", "1")
		if err != nil {
			t.Fatal(err)
		}
		page, ok := value.(NativeArray)
		if !ok || len(page) != 2 {
			t.Fatalf("scan page: %#v", value)
		}
		cursorBytes, ok := page[0].(NativeBytes)
		if !ok {
			t.Fatal("cursor is not bulk bytes")
		}
		cursor = string(cursorBytes)
		if _, err := strconv.ParseUint(cursor, 10, 64); err != nil {
			t.Fatal("invalid native cursor")
		}
		scanned, ok := page[1].(NativeArray)
		if !ok {
			t.Fatal("keys are not an array")
		}
		for _, key := range scanned {
			data, ok := key.(NativeBytes)
			if !ok {
				t.Fatal("key is not bulk bytes")
			}
			seen[string(data)] = true
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != len(keys) || seen[foreign] {
		t.Fatalf("scoped scan got %d keys, want %d", len(seen), len(keys))
	}
	for _, key := range keys {
		if !seen[key] {
			t.Fatal("owned key absent from complete fixture scan")
		}
	}
	for _, command := range [][]string{
		{"GET", foreign}, {"SET", keys[0], "mutated"}, {"KEYS", "*"},
		{"SCAN", "0", "MATCH", "*"},
	} {
		if _, err := reader.QueryNative(ctx, command...); err == nil {
			t.Fatalf("unsafe read or policy bypass: %s", command[0])
		}
	}
	if got := admin.Get(ctx, foreign).Val(); got != "owned" {
		t.Fatal("foreign fixture key changed")
	}
}
