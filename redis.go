// Package dalgo2redis provides read-only native Redis commands and DALgo key reads.
package dalgo2redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
	"github.com/redis/go-redis/v9"
)

// Client owns the Redis connection pool. Close it when finished.
// Redis ACLs remain the authority: use a dedicated read-only account.
type Client struct {
	client *redis.Client
	id     string
}

// Open accepts redis:// or rediss:// URLs, including ACL credentials and /DB.
// Errors deliberately omit URLs, credentials and server-provided messages.
func Open(ctx context.Context, rawURL string) (*Client, error) {
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, errors.New("invalid Redis connection URL")
	}
	opts.DialTimeout = 5 * time.Second
	opts.ReadTimeout = 5 * time.Second
	opts.WriteTimeout = 5 * time.Second
	opts.ContextTimeoutEnabled = true
	opts.MaxRetries = -1
	c := &Client{client: redis.NewClient(opts), id: fmt.Sprintf("redis:%d", opts.DB)}
	if err := c.client.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return nil, errors.New("Redis connection failed; check endpoint, credentials and ACL")
	}
	return c, nil
}
func (c *Client) Close() error { return c.client.Close() }

// ValidateCommand rejects writes, scripts, administrative commands, key scans,
// transactions, blocking reads and unbounded collection reads before network I/O.
// At most 128 key/field arguments and 64 KiB of command text are accepted.
func ValidateCommand(args []string) error {
	if len(args) < 2 || len(args) > 129 {
		return errors.New("Redis read requires a command and 1 to 128 arguments")
	}
	size := 0
	for _, a := range args {
		size += len(a)
	}
	if size > 65536 {
		return errors.New("Redis command exceeds 64 KiB")
	}
	command := strings.ToUpper(args[0])
	switch command {
	case "GET", "TYPE", "TTL", "PTTL", "STRLEN", "HLEN", "LLEN", "SCARD", "ZCARD":
		if len(args) != 2 {
			return errors.New("Redis command requires exactly one key")
		}
	case "HGET", "HEXISTS":
		if len(args) != 3 {
			return errors.New("Redis hash command requires one key and one field")
		}
	case "HMGET":
		if len(args) < 3 {
			return errors.New("Redis HMGET requires a key and fields")
		}
	case "MGET", "EXISTS":
	default:
		return errors.New("unsupported Redis read command; allowed: GET, MGET, HGET, HMGET, HEXISTS, EXISTS, TYPE, TTL, PTTL, STRLEN, HLEN, LLEN, SCARD, ZCARD")
	}
	return nil
}

// Query executes exactly one validated read. Missing string/hash values are nil.
// Redis values are returned as strings; arrays preserve order and missing values.
// Reads across keys are not a transaction or snapshot.
func (c *Client) Query(ctx context.Context, args ...string) (any, error) {
	if err := ValidateCommand(args); err != nil {
		return nil, err
	}
	timeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	values := make([]any, len(args))
	for i, v := range args {
		values[i] = v
	}
	values[0] = strings.ToUpper(args[0])
	result, err := c.client.Do(timeout, values...).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		if timeout.Err() != nil {
			return nil, timeout.Err()
		}
		return nil, errors.New("Redis read failed; check command, key type and ACL")
	}
	return result, nil
}

// DB exposes Redis strings through DALgo record keys. Collections are ignored:
// Redis keys share the selected numeric database's flat namespace. Data is a map
// with key and value fields. Structured queries and all writes are unsupported.
func (c *Client) DB() dal.DB { return dal.NewDB(&backend{Client: c}) }

type backend struct {
	dal.NoConcurrency
	*Client
}

var _ dal.Backend = (*backend)(nil)

func (b *backend) ID() string           { return b.id }
func (b *backend) Adapter() dal.Adapter { return dal.NewAdapter("dalgo2redis", "0.1.0") }
func (b *backend) Schema() dal.Schema   { return nil }
func (b *backend) Get(ctx context.Context, r record.Record) error {
	value, err := b.Query(ctx, "GET", fmt.Sprint(r.Key().ID))
	if err == nil && value == nil {
		err = dal.NewErrNotFoundByKey(r.Key(), nil)
	}
	if err == nil {
		err = record.MapToData(r.Data(), map[string]any{"key": fmt.Sprint(r.Key().ID), "value": value})
	}
	r.SetError(err)
	return err
}
func (b *backend) Exists(ctx context.Context, k *record.Key) (bool, error) {
	v, e := b.Query(ctx, "EXISTS", fmt.Sprint(k.ID))
	if e != nil {
		return false, e
	}
	return v == int64(1), nil
}
func (b *backend) GetMulti(ctx context.Context, rs []record.Record) error {
	for _, r := range rs {
		if e := b.Get(ctx, r); e != nil {
			return e
		}
	}
	return nil
}
func (b *backend) RunReadonlyTransaction(context.Context, dal.ROTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (b *backend) RunReadwriteTransaction(context.Context, dal.RWTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (b *backend) ExecuteQueryToRecordsReader(context.Context, dal.Query) (dal.RecordsReader, error) {
	return nil, dal.ErrNotSupported
}
func (b *backend) ExecuteQueryToRecordsetReader(context.Context, dal.Query, ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, dal.ErrNotSupported
}
