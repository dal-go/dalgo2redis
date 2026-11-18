package dalgo2redis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"unicode/utf8"
)

// NativeValue is a closed lossless RESP read value. A nil value is JSON null.
// The JSON contract matches the independent TypeScript RESP read adapter.
type NativeValue interface {
	redisNativeValue()
}

// NativeString is a RESP simple status (TYPE). Bulk data uses NativeBytes.
type NativeString string

func (NativeString) redisNativeValue() {}

// NativeInteger retains signed 64-bit precision in a JSON decimal string.
type NativeInteger int64

func (NativeInteger) redisNativeValue() {}
func (v NativeInteger) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Kind  string `json:"kind"`
		Value string `json:"value"`
	}{"integer", strconv.FormatInt(int64(v), 10)})
}

// NativeBytes retains bulk bytes, including empty and non-UTF8 data.
type NativeBytes []byte

func (NativeBytes) redisNativeValue() {}
func (v NativeBytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Kind   string `json:"kind"`
		Base64 string `json:"base64"`
	}{"bytes", base64.StdEncoding.EncodeToString(v)})
}

// NativeArray retains reply ordering, nil slots and nested array boundaries.
type NativeArray []NativeValue

func (NativeArray) redisNativeValue() {}
func (v NativeArray) MarshalJSON() ([]byte, error) {
	if v == nil {
		v = NativeArray{}
	}
	return json.Marshal(struct {
		Kind   string        `json:"kind"`
		Values []NativeValue `json:"values"`
	}{"array", v})
}

// QueryNative executes the same admitted read and budgets as Query, but its
// result is safe to serialize for a JavaScript consumer without byte/int64 loss.
// Query and DB retain their legacy Go string/int64 results.
func (c *Client) QueryNative(ctx context.Context, args ...string) (NativeValue, error) {
	var native NativeValue
	_, err := c.query(ctx, args, func(command string, value any) (any, error) {
		var conversionErr error
		native, conversionErr = nativeReply(command, value)
		return nil, conversionErr
	})
	if err != nil {
		return nil, err
	}
	return native, nil
}

func nativeReply(command string, value any) (NativeValue, error) {
	bad := errors.New("invalid Redis read response")
	bulk := func(value any) (NativeValue, error) {
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, bad
		}
		return NativeBytes([]byte(text)), nil
	}
	switch command {
	case "TYPE":
		text, ok := value.(string)
		if !ok || !utf8.ValidString(text) {
			return nil, bad
		}
		return NativeString(text), nil
	case "GET", "HGET", "ZSCORE":
		return bulk(value)
	case "MGET", "HMGET":
		array, ok := value.([]any)
		if !ok {
			return nil, bad
		}
		result := make(NativeArray, len(array))
		for i, child := range array {
			converted, err := bulk(child)
			if err != nil {
				return nil, err
			}
			result[i] = converted
		}
		return result, nil
	case "SCAN":
		array, ok := value.([]any)
		if !ok || len(array) != 2 {
			return nil, bad
		}
		cursor, ok := array[0].(string)
		if !ok || !decimalUint64(cursor) {
			return nil, bad
		}
		keys, ok := array[1].([]any)
		if !ok {
			return nil, bad
		}
		result := make(NativeArray, len(keys))
		for i, child := range keys {
			text, ok := child.(string)
			if !ok {
				return nil, bad
			}
			result[i] = NativeBytes([]byte(text))
		}
		return NativeArray{NativeBytes([]byte(cursor)), result}, nil
	default:
		// This branch is reachable only for the closed integer-read allowlist
		// admitted before I/O, never arbitrary commands or server reply tags.
		n, ok := value.(int64)
		if !ok {
			return nil, bad
		}
		return NativeInteger(n), nil
	}
}
