// Package dalgo2redis provides read-only native Redis commands and DALgo key reads.
package dalgo2redis

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
	"github.com/redis/go-redis/v9"
)

// Client owns bounded operation-specific read connections. Close it when finished.
// Redis ACLs remain the authority: use a dedicated read-only account.
type Client struct {
	client  *redis.Client
	id      string
	options Options
	reads   *readOwner
}

// Open accepts redis:// or rediss:// URLs, including ACL credentials and /DB.
// Errors deliberately omit URLs, credentials and server-provided messages.
func Open(ctx context.Context, rawURL string) (*Client, error) {
	return OpenWithOptions(ctx, rawURL, Options{})
}

// OpenWithOptions adds an explicit, trusted enumeration scope. The caller owns
// this configuration; do not take ScanPrefix from an untrusted HTTP request.
func OpenWithOptions(ctx context.Context, rawURL string, options Options) (*Client, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, errors.New("invalid Redis connection URL")
	}
	opts.DialTimeout = 5 * time.Second
	opts.ReadTimeout = 5 * time.Second
	opts.WriteTimeout = 5 * time.Second
	opts.ContextTimeoutEnabled = true
	opts.MaxRetries = -1
	opts.Protocol = 2
	opts.DisableIdentity = true
	opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{Timeout: opts.DialTimeout}
		var conn net.Conn
		var err error
		if opts.TLSConfig != nil {
			conn, err = (&tls.Dialer{NetDialer: dialer, Config: opts.TLSConfig}).DialContext(ctx, network, addr)
		} else {
			conn, err = dialer.DialContext(ctx, network, addr)
		}
		if err != nil {
			return nil, err
		}
		return &boundedConn{Conn: conn, reader: bufio.NewReaderSize(conn, 4096)}, nil
	}
	c := &Client{client: redis.NewClient(opts), id: fmt.Sprintf("redis:%d", opts.DB), options: options}
	c.reads = newReadOwner(c.client.Options())
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = c.client.Close()
		close(closed)
	})
	pingErr := c.client.Ping(ctx).Err()
	if !stop() {
		<-closed
	}
	if err := ctx.Err(); err != nil {
		_ = c.Close()
		return nil, err
	}
	if pingErr != nil {
		_ = c.Close()
		return nil, errors.New("Redis connection failed; check endpoint, credentials and ACL")
	}
	// The verification connection is not retained in addition to the admitted
	// read drivers: total live reads stay within the original pool limit.
	_ = c.client.Close()
	if err := ctx.Err(); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}
func (c *Client) Close() error {
	if c.reads != nil {
		c.reads.close()
	}
	err := c.client.Close()
	if errors.Is(err, redis.ErrClosed) {
		return nil
	}
	return err
}

// ValidateCommand rejects writes, scripts, administrative commands, key scans,
// transactions, blocking reads and unbounded collection reads before network I/O.
// At most 128 key/field arguments and 64 KiB of command text are accepted.
func ValidateCommand(args []string) error {
	return ValidateCommandWithOptions(args, Options{})
}

func validateRead(args []string) error {
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
	case "HGET", "HEXISTS", "ZSCORE":
		if len(args) != 3 {
			return errors.New("Redis command requires one key and one field or member")
		}
	case "HMGET":
		if len(args) < 3 {
			return errors.New("Redis HMGET requires a key and fields")
		}
	case "MGET", "EXISTS":
	case "SCAN":
		return validateScan(args)
	default:
		return errors.New("unsupported Redis read command; allowed: GET, MGET, HGET, HMGET, HEXISTS, EXISTS, TYPE, TTL, PTTL, STRLEN, HLEN, LLEN, SCARD, ZCARD, ZSCORE; SCAN requires a configured prefix")
	}
	return nil
}

// Query executes exactly one validated read. Missing string/hash values are nil.
// Redis bulk values are Go strings and integers are int64; arrays preserve order
// and missing values. Use QueryNative for lossless JavaScript-facing JSON.
// Reads across keys are not a transaction or snapshot.
func (c *Client) Query(ctx context.Context, args ...string) (any, error) {
	return c.query(ctx, args, nil)
}

// query retains one deadline through command execution, optional lossless
// conversion and the last delivery check. The conversion cannot expand authority.
func (c *Client) query(ctx context.Context, args []string, convert func(string, any) (any, error)) (any, error) {
	admitted, err := prepareCommand(args, c.options)
	if err != nil {
		return nil, err
	}
	timeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	driver := c.client
	if c.reads != nil {
		var release func()
		driver, release, err = c.reads.acquire(timeout, cancel)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	values := make([]any, len(admitted))
	for i, v := range admitted {
		values[i] = v
	}
	result, err := driver.Do(timeout, values...).Result()
	if deadlineErr := timeout.Err(); deadlineErr != nil {
		return nil, deadlineErr
	}
	if errors.Is(err, redis.Nil) {
		result, err = nil, nil
	}
	if err != nil {
		if errors.Is(err, errResponseBudget) {
			return nil, errResponseBudget
		}
		return nil, errors.New("Redis read failed; check command, key type and ACL")
	}
	if admitted[0] == "SCAN" {
		if err := validateScanReply(result, c.options.ScanPrefix); err != nil {
			return nil, err
		}
	}
	if convert != nil {
		result, err = convert(admitted[0], result)
		if err != nil {
			return nil, err
		}
	}
	if deadlineErr := timeout.Err(); deadlineErr != nil {
		return nil, deadlineErr
	}
	return result, nil
}

// DB exposes Redis strings through DALgo record keys. Collections are ignored:
// Redis keys share the selected numeric database's flat namespace. Data is a map
// with key and value fields. Structured queries and all writes are unsupported.
func (c *Client) DB() dal.DB { return flatKeyDB(c, c.id) }

type readClient interface {
	Query(context.Context, ...string) (any, error)
	QueryNative(context.Context, ...string) (NativeValue, error)
	Close() error
	DB() dal.DB
}

func flatKeyDB(client readClient, id string) dal.DB {
	return dal.NewDB(&backend{readClient: client, id: id})
}

type backend struct {
	dal.NoConcurrency
	readClient
	id string
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
