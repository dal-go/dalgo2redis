package dalgo2redis

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
)

// RESTTokenProvider obtains a scoped token within the original request deadline.
// It should honor ctx. A noncooperative provider cannot block caller cancellation;
// at most four provider calls can remain outstanding per RESTClient.
type RESTTokenProvider func(context.Context) (string, error)

type RESTOptions struct {
	Options       Options
	TokenProvider RESTTokenProvider
	// Timeout covers admission, token acquisition, HTTP, body and conversion.
	// Zero selects ten seconds; nonzero values must be 1ms..2min.
	Timeout time.Duration
	// Transport is cloned and owned. Its custom dialing/TLS callbacks are
	// trusted configuration. Proxies and TLS verification bypasses are refused.
	Transport *http.Transport
	// InsecureLoopbackForTesting permits only HTTP to an IP loopback address.
	InsecureLoopbackForTesting bool
}

const MaxRESTRequestBytes = 65536
const restConcurrency = 4

var (
	ErrRESTTransport = errors.New("Upstash REST request failed")
	ErrRESTProtocol  = errors.New("invalid Upstash REST response")
	ErrRESTDenied    = errors.New("Upstash REST read denied; check command, key type and token scope")
	ErrRESTAuth      = errors.New("Upstash REST token could not be acquired")
	ErrRESTClosed    = errors.New("Upstash REST client is closed")
	ErrRESTLimit     = errors.New("Upstash REST request or response exceeds safety budget")
)

// RESTHTTPError deliberately exposes only the HTTP status, never response text.
type RESTHTTPError struct{ Status int }

func (e *RESTHTTPError) Error() string { return fmt.Sprintf("Upstash REST HTTP %d", e.Status) }

type restOperation struct{ cancel context.CancelFunc }

// RESTClient executes the documented Upstash POST-array/RESP2 HTTP contract.
// NewRESTClient makes no network request and does not probe with broader tokens.
type RESTClient struct {
	endpoint  string
	options   Options
	token     RESTTokenProvider
	timeout   time.Duration
	transport *http.Transport
	http      *http.Client
	mu        sync.Mutex
	closed    bool
	done      chan struct{}
	slots     chan struct{}
	authSlots chan struct{}
	active    map[*restOperation]struct{}
	wg        sync.WaitGroup
}

func NewRESTClient(endpoint string, options RESTOptions) (*RESTClient, error) {
	u, err := url.Parse(endpoint)
	bad := errors.New("Upstash REST endpoint must be an HTTPS origin without credentials, path, query or fragment")
	if err != nil || u.Host == "" || u.Hostname() == "" || u.Opaque != "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawPath != "" || strings.ContainsAny(endpoint, "?#") {
		return nil, bad
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || !options.InsecureLoopbackForTesting || ip == nil || !ip.IsLoopback() {
			return nil, bad
		}
	}
	if err := validateOptions(options.Options); err != nil {
		return nil, err
	}
	if !utf8.ValidString(options.Options.ScanPrefix) {
		return nil, errors.New("Upstash REST scan prefix must be valid UTF-8")
	}
	if options.TokenProvider == nil {
		return nil, ErrRESTAuth
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	if timeout < time.Millisecond || timeout > 2*time.Minute {
		return nil, errors.New("Upstash REST timeout must be from 1ms to 2min")
	}
	var transport *http.Transport
	if options.Transport == nil {
		transport = &http.Transport{DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: timeout, IdleConnTimeout: 30 * time.Second, ForceAttemptHTTP2: true}
	} else {
		if options.Transport.Proxy != nil {
			return nil, errors.New("Upstash REST proxy transports are not supported")
		}
		transport = options.Transport.Clone()
	}
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if transport.TLSClientConfig.InsecureSkipVerify || (transport.TLSClientConfig.MaxVersion != 0 && transport.TLSClientConfig.MaxVersion < tls.VersionTLS12) {
		return nil, errors.New("Upstash REST requires verified TLS1.2 or newer")
	}
	if transport.TLSClientConfig.MinVersion < tls.VersionTLS12 {
		transport.TLSClientConfig.MinVersion = tls.VersionTLS12
	}
	if transport.TLSClientConfig.RootCAs != nil {
		transport.TLSClientConfig.RootCAs = transport.TLSClientConfig.RootCAs.Clone()
	}
	transport.MaxConnsPerHost = restConcurrency
	transport.MaxIdleConnsPerHost = restConcurrency
	transport.MaxIdleConns = restConcurrency
	transport.MaxResponseHeaderBytes = 64 << 10
	u.Path = "/"
	c := &RESTClient{endpoint: u.String(), options: options.Options, token: options.TokenProvider, timeout: timeout, transport: transport, done: make(chan struct{}), slots: make(chan struct{}, restConcurrency), authSlots: make(chan struct{}, restConcurrency), active: make(map[*restOperation]struct{})}
	c.http = &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return c, nil
}

func (c *RESTClient) begin(ctx context.Context, cancel context.CancelFunc) (func(), error) {
	select {
	case c.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, ErrRESTClosed
	}
	c.mu.Lock()
	if c.closed || ctx.Err() != nil {
		c.mu.Unlock()
		<-c.slots
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, ErrRESTClosed
	}
	op := &restOperation{cancel: cancel}
	c.active[op] = struct{}{}
	c.wg.Add(1)
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		delete(c.active, op)
		c.mu.Unlock()
		<-c.slots
		c.wg.Done()
	}, nil
}

func (c *RESTClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.wg.Wait()
		return nil
	}
	c.closed = true
	close(c.done)
	for op := range c.active {
		op.cancel()
	}
	c.mu.Unlock()
	c.transport.CloseIdleConnections()
	c.wg.Wait()
	c.transport.CloseIdleConnections()
	return nil
}

type restTokenResult struct {
	token string
	err   error
}

func (c *RESTClient) acquireToken(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	select {
	case c.authSlots <- struct{}{}:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	result := make(chan restTokenResult, 1)
	go func() {
		defer func() { <-c.authSlots }()
		var value restTokenResult
		func() {
			defer func() {
				if recover() != nil {
					value.err = ErrRESTAuth
				}
			}()
			value.token, value.err = c.token(ctx)
		}()
		result <- value
	}()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case value := <-result:
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if value.err != nil || value.token == "" || len(value.token) > 8192 {
			return "", ErrRESTAuth
		}
		for _, b := range []byte(value.token) {
			if b <= 32 || b >= 127 {
				return "", ErrRESTAuth
			}
		}
		return value.token, nil
	}
}

func (c *RESTClient) Query(ctx context.Context, args ...string) (any, error) {
	return c.queryREST(ctx, args, nil)
}

func (c *RESTClient) QueryNative(ctx context.Context, args ...string) (NativeValue, error) {
	value, err := c.queryREST(ctx, args, func(command string, raw any) (any, error) { return nativeReply(command, raw) })
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	return value.(NativeValue), nil
}

func (c *RESTClient) DB() dal.DB { return flatKeyDB(c, "upstash-rest") }

func (c *RESTClient) queryREST(parent context.Context, args []string, convert func(string, any) (any, error)) (value any, err error) {
	admitted, err := prepareCommand(args, c.options)
	if err != nil {
		return nil, err
	}
	for _, arg := range admitted {
		if !utf8.ValidString(arg) {
			return nil, errors.New("Upstash REST JSON commands require valid UTF-8 arguments")
		}
	}
	encoded, err := json.Marshal(admitted)
	if err != nil {
		return nil, ErrRESTProtocol
	}
	if len(encoded) > MaxRESTRequestBytes {
		return nil, ErrRESTLimit
	}
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	var release func()
	var response *http.Response
	defer func() {
		var closeErr error
		if response != nil && response.Body != nil {
			closeErr = response.Body.Close()
		}
		if release != nil {
			release()
		}
		if contextErr := ctx.Err(); contextErr != nil {
			value, err = nil, contextErr
		} else if err == nil && closeErr != nil {
			value, err = nil, ErrRESTTransport
		}
		cancel()
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	release, err = c.begin(ctx, cancel)
	if err != nil {
		return nil, err
	}
	token, err := c.acquireToken(ctx)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(encoded))
	if err != nil {
		return nil, ErrRESTTransport
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/octet-stream")
	request.Header.Set("Upstash-Response-Format", "resp2")
	response, err = c.http.Do(request)
	if err != nil {
		return nil, ErrRESTTransport
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode == 400 || response.StatusCode == 401 || response.StatusCode == 403 {
			return nil, ErrRESTDenied
		}
		return nil, &RESTHTTPError{Status: response.StatusCode}
	}
	media, _, mediaErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if mediaErr != nil || media != "application/octet-stream" {
		return nil, ErrRESTProtocol
	}
	if response.ContentLength > MaxResponseBytes {
		return nil, ErrRESTLimit
	}
	reader := bufio.NewReaderSize(io.LimitReader(response.Body, MaxResponseBytes+1), 4096)
	frame := make([]byte, 0, 4096)
	if readErr := readFrame(reader, &frame, 0); readErr != nil {
		if errors.Is(readErr, errResponseBudget) {
			return nil, ErrRESTLimit
		}
		return nil, ErrRESTProtocol
	}
	if _, extraErr := reader.ReadByte(); !errors.Is(extraErr, io.EOF) {
		return nil, ErrRESTProtocol
	}
	value, err = decodeRESTFrame(frame)
	if err != nil {
		return nil, err
	}
	// Validate command-specific reply shape in both public modes, not only
	// when the caller opts into the lossless JSON representation.
	if admitted[0] == "SCAN" {
		if err = validateScanReply(value, c.options.ScanPrefix); err != nil {
			return nil, ErrRESTProtocol
		}
	}
	if admitted[0] == "MGET" || admitted[0] == "HMGET" {
		count := len(admitted) - 1
		if admitted[0] == "HMGET" {
			count--
		}
		array, ok := value.([]any)
		if !ok || len(array) != count {
			return nil, ErrRESTProtocol
		}
	}
	if _, err = nativeReply(admitted[0], value); err != nil {
		return nil, ErrRESTProtocol
	}
	if convert != nil {
		return convert(admitted[0], value)
	}
	return value, nil
}
