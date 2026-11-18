package dalgo2redis

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

func restFixture(t *testing.T, handler http.HandlerFunc, options RESTOptions) (*RESTClient, *httptest.Server) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	options.Transport = server.Client().Transport.(*http.Transport)
	if options.TokenProvider == nil {
		options.TokenProvider = func(context.Context) (string, error) { return "synthetic-scoped-token", nil }
	}
	client, err := NewRESTClient(server.URL, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return client, server
}

func restReply(w http.ResponseWriter, frame string) {
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = io.WriteString(w, frame)
}

func drainRESTFixtureRequest(w http.ResponseWriter, r *http.Request) bool {
	// HTTP/1's server-side disconnect observer starts after body EOF. The
	// request is small, but drain it with the same explicit wire byte bound.
	n, err := io.Copy(io.Discard, io.LimitReader(r.Body, MaxRESTRequestBytes+1))
	if err != nil || n > MaxRESTRequestBytes {
		w.WriteHeader(http.StatusBadRequest)
		return false
	}
	return true
}

func mustRESTValue(t *testing.T, client *RESTClient, args ...string) NativeValue {
	t.Helper()
	value, err := client.QueryNative(context.Background(), args...)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestRESTRequestAndNativeFidelity(t *testing.T) {
	type observation struct {
		method, path, auth, encoding, format, contentType string
		command                                           []string
	}
	captured := make(chan observation, 8)
	var tokens atomic.Int32
	client, _ := restFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var command []string
		if err := json.NewDecoder(io.LimitReader(r.Body, MaxRESTRequestBytes+1)).Decode(&command); err != nil {
			w.WriteHeader(400)
			return
		}
		captured <- observation{r.Method, r.URL.RequestURI(), r.Header.Get("Authorization"), r.Header.Get("Upstash-Encoding"), r.Header.Get("Upstash-Response-Format"), r.Header.Get("Content-Type"), command}
		switch command[0] {
		case "GET":
			restReply(w, "$5\r\n\x00\xff\x80\r\n\r\n")
		case "PTTL":
			restReply(w, ":9223372036854775807\r\n")
		case "TYPE":
			restReply(w, "+string\r\n")
		case "MGET":
			restReply(w, "*3\r\n$0\r\n\r\n$-1\r\n$1\r\nx\r\n")
		}
	}, RESTOptions{TokenProvider: func(context.Context) (string, error) { return fmt.Sprintf("synthetic-%d", tokens.Add(1)), nil }})
	for _, test := range []struct {
		args []string
		json string
	}{
		{[]string{"get", "space / key?"}, `{"kind":"bytes","base64":"AP+ADQo="}`},
		{[]string{"PTTL", "key"}, `{"kind":"integer","value":"9223372036854775807"}`},
		{[]string{"TYPE", "key"}, `"string"`},
		{[]string{"MGET", "empty", "missing", "text"}, `{"kind":"array","values":[{"kind":"bytes","base64":""},null,{"kind":"bytes","base64":"eA=="}]}`},
	} {
		value := mustRESTValue(t, client, test.args...)
		encoded, err := json.Marshal(value)
		if err != nil || string(encoded) != test.json {
			t.Fatalf("lossless REST value mismatch: %s (%v)", encoded, err)
		}
		got := <-captured
		if got.method != "POST" || got.path != "/" || got.format != "resp2" || got.encoding != "" || got.contentType != "application/json" || got.auth != fmt.Sprintf("Bearer synthetic-%d", tokens.Load()) {
			t.Fatal("incorrect provider request/auth contract")
		}
		if len(got.command) != len(test.args) || got.command[0] != strings.ToUpper(test.args[0]) {
			t.Fatal("command changed")
		}
		for i := 1; i < len(test.args); i++ {
			if got.command[i] != test.args[i] {
				t.Fatal("key/argument changed")
			}
		}
	}
}

func TestRESTPolicyAndJSONRequestBudgetsBeforeAuthentication(t *testing.T) {
	var auth, calls atomic.Int32
	captured := make(chan []string, 1)
	client, _ := restFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var command []string
		if err := json.NewDecoder(r.Body).Decode(&command); err != nil {
			t.Error("fixture received invalid command JSON")
			w.WriteHeader(400)
			return
		}
		captured <- command
		restReply(w, "$-1\r\n")
	}, RESTOptions{TokenProvider: func(context.Context) (string, error) { auth.Add(1); return "token", nil }})
	for index, args := range [][]string{{"SET", "k", "x"}, {"KEYS", "*"}, {"MULTI", "k"}, {"EVAL", "x", "0"}, {"SCAN", "0"}, {"GET", string([]byte{255})}, {"GET", strings.Repeat("x", 65536)}, {"GET", strings.Repeat("\n", 40000)}} {
		if value, err := client.Query(context.Background(), args...); err == nil || value != nil {
			t.Fatalf("refusal case %d accepted (%d arguments, %d raw value bytes)", index, len(args), len(args[len(args)-1]))
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("synthetic-secret-context-cause"))
	if _, err := client.Query(ctx, "GET", "k"); !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "synthetic-secret") {
		t.Fatal("pre-abort did not retain redacted context sentinel")
	}
	if auth.Load() != 0 || calls.Load() != 0 {
		t.Fatal("refused/pre-aborted request invoked auth or I/O")
	}
	// JSON escapes a newline as two bytes. This request fits the encoded
	// cap and must preserve its literal key rather than reject all controls.
	key := strings.Repeat("\n", 12000)
	if value, err := client.Query(context.Background(), "GET", key); err != nil || value != nil {
		t.Fatal("under-cap escaped key was refused")
	}
	command := <-captured
	if len(command) != 2 || command[0] != "GET" || command[1] != key || auth.Load() != 1 || calls.Load() != 1 {
		t.Fatal("under-cap escaped key changed or unexpected I/O occurred")
	}
}

func TestRESTScopedScanValidatesWholeReply(t *testing.T) {
	prefix := "owned:*?[]\\:"
	commands := make(chan []string, 8)
	var frame atomic.Value
	frame.Store("*2\r\n$1\r\n0\r\n*0\r\n")
	client, _ := restFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var command []string
		if err := json.NewDecoder(r.Body).Decode(&command); err != nil {
			w.WriteHeader(400)
			return
		}
		commands <- command
		restReply(w, frame.Load().(string))
	}, RESTOptions{Options: Options{ScanPrefix: prefix}})
	key := prefix + string([]byte{255})
	frame.Store(fmt.Sprintf("*2\r\n$20\r\n18446744073709551615\r\n*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(key), key))
	value := mustRESTValue(t, client, "SCAN", "0", "COUNT", "1")
	page, ok := value.(NativeArray)
	if !ok || string(page[0].(NativeBytes)) != "18446744073709551615" || len(page[1].(NativeArray)) != 2 {
		t.Fatal("scan cursor/duplicates lost")
	}
	if string(page[1].(NativeArray)[0].(NativeBytes)) != key {
		t.Fatal("scan key bytes lost")
	}
	command := <-commands
	if len(command) != 6 || command[4] != "MATCH" || command[5] != "owned:\\*\\?\\[\\]\\\\:*" {
		t.Fatal("literal scan prefix was not escaped")
	}
	for _, reply := range []string{"*2\r\n$1\r\n0\r\n*1\r\n$7\r\nforeign\r\n", "*2\r\n$1\r\nx\r\n*0\r\n", "*1\r\n$1\r\n0\r\n"} {
		frame.Store(reply)
		if v, err := client.Query(context.Background(), "SCAN", "0"); !errors.Is(err, ErrRESTProtocol) || v != nil {
			t.Fatal("malformed/foreign scan page was accepted")
		}
	}
	frame.Store("-NOPERM synthetic-secret denial\r\n")
	if v, err := client.QueryNative(context.Background(), "SCAN", "0"); !errors.Is(err, ErrRESTDenied) || v != nil || strings.Contains(err.Error(), "synthetic-secret") {
		t.Fatal("read-only token denial was masked or leaked")
	}
}

func TestRESTReplyBoundsShapeAndRecovery(t *testing.T) {
	var payload atomic.Value
	payload.Store("$-1\r\n")
	client, _ := restFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		frame := payload.Load().(string)
		if frame == "wrong-content-type" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{ "result": "fake" }`)
			return
		}
		if frame == "declared-overflow" {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", fmt.Sprint(MaxResponseBytes+1))
			return
		}
		restReply(w, frame)
	}, RESTOptions{})
	for _, frame := range []string{"", "$3\r\nx\r\n", "$1\r\nx\r\n+extra\r\n", ":9223372036854775808\r\n", ":1\r\n", "*257\r\n", "$1048577\r\n", "+" + strings.Repeat("x", 4096) + "\r\n", "wrong-content-type", "declared-overflow"} {
		payload.Store(frame)
		if value, err := client.QueryNative(context.Background(), "GET", "key"); err == nil || value != nil {
			t.Fatal("malformed or excessive reply was accepted")
		}
	}
	length := MaxResponseBytes - len(fmt.Sprintf("$%d\r\n", MaxResponseBytes)) - 2
	payload.Store(fmt.Sprintf("$%d\r\n%s\r\n", length, strings.Repeat("x", length)))
	if value := mustRESTValue(t, client, "GET", "boundary"); len(value.(NativeBytes)) != length {
		t.Fatal("exact response byte bound was truncated")
	}
	payload.Store("$0\r\n\r\n")
	if value := mustRESTValue(t, client, "GET", "key"); len(value.(NativeBytes)) != 0 {
		t.Fatal("empty bulk read failed after refused replies")
	}
	payload.Store("$-1\r\n")
	if value := mustRESTValue(t, client, "GET", "missing"); value != nil {
		t.Fatal("missing value became empty data")
	}
	payload.Store("*1\r\n$1\r\nx\r\n")
	for _, args := range [][]string{{"MGET", "a", "b"}, {"HMGET", "hash", "a", "b"}} {
		if value, err := client.Query(context.Background(), args...); value != nil || !errors.Is(err, ErrRESTProtocol) {
			t.Fatal("truncated multi-read reply became a partial success")
		}
	}
}

func TestRESTHTTPStatusRedirectAndTLSVerification(t *testing.T) {
	var followed atomic.Int32
	other := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { followed.Add(1); restReply(w, "$1\r\nx\r\n") }))
	defer other.Close()
	for _, status := range []int{302, 400, 401, 403, 429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			client, _ := restFixture(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", other.URL+"/synthetic-secret")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "synthetic-secret-provider-body")
			}, RESTOptions{})
			value, err := client.Query(context.Background(), "GET", "key")
			if value != nil || err == nil || strings.Contains(err.Error(), "synthetic-secret") {
				t.Fatal("HTTP failure leaked or became success")
			}
			if status == 400 || status == 401 || status == 403 {
				if !errors.Is(err, ErrRESTDenied) {
					t.Fatal("denied request lost classification")
				}
			} else {
				var typed *RESTHTTPError
				if !errors.As(err, &typed) || typed.Status != status {
					t.Fatal("HTTP status was not preserved")
				}
			}
		})
	}
	if followed.Load() != 0 {
		t.Fatal("redirect forwarded an authenticated request")
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { restReply(w, "$1\r\nx\r\n") }))
	defer server.Close()
	client, err := NewRESTClient(server.URL, RESTOptions{TokenProvider: func(context.Context) (string, error) { return "synthetic-secret-token", nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if value, err := client.Query(context.Background(), "GET", "key"); value != nil || !errors.Is(err, ErrRESTTransport) || strings.Contains(err.Error(), server.URL) {
		t.Fatal("untrusted TLS certificate was accepted or raw network error leaked")
	}
}

func TestRESTEndpointAndTransportConfiguration(t *testing.T) {
	token := func(context.Context) (string, error) { return "token", nil }
	for _, endpoint := range []string{"http://example.com", "https://user:secret@example.com", "https://example.com/path", "https://example.com?token=secret", "https://example.com#secret", "redis://example.com", "https://example.com?", "http://localhost:1234"} {
		if c, err := NewRESTClient(endpoint, RESTOptions{TokenProvider: token}); err == nil || c != nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("unsafe endpoint accepted or exposed")
		}
	}
	for _, options := range []RESTOptions{{}, {TokenProvider: token, Timeout: -1}, {TokenProvider: token, Timeout: 3 * time.Minute}, {TokenProvider: token, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}, {TokenProvider: token, Transport: &http.Transport{Proxy: http.ProxyFromEnvironment}}} {
		if c, err := NewRESTClient("https://fixture.upstash.io", options); err == nil || c != nil {
			t.Fatal("unsafe or unbounded transport configuration accepted")
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { restReply(w, "$1\r\nx\r\n") }))
	defer server.Close()
	c, err := NewRESTClient(server.URL, RESTOptions{TokenProvider: token, InsecureLoopbackForTesting: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := mustRESTValue(t, c, "GET", "key"); string(got.(NativeBytes)) != "x" {
		t.Fatal("explicit owned loopback fixture failed")
	}
}

func TestRESTTokenFailureAndNoncooperativeAdmissionAreBounded(t *testing.T) {
	for _, provider := range []RESTTokenProvider{
		func(context.Context) (string, error) { return "", errors.New("synthetic-secret") },
		func(context.Context) (string, error) { panic("synthetic-secret") },
		func(context.Context) (string, error) { return "synthetic\r\nsecret", nil },
	} {
		client, _ := restFixture(t, func(w http.ResponseWriter, _ *http.Request) {
			t.Error("failed token caused network I/O")
			w.WriteHeader(500)
		}, RESTOptions{TokenProvider: provider})
		if value, err := client.Query(context.Background(), "GET", "key"); value != nil || !errors.Is(err, ErrRESTAuth) || strings.Contains(err.Error(), "synthetic-secret") {
			t.Fatal("token error/panic/header injection escaped")
		}
	}
	var started atomic.Int32
	entered := make(chan struct{}, restConcurrency)
	blocked := make(chan struct{})
	client, _ := restFixture(t, func(w http.ResponseWriter, _ *http.Request) { t.Error("late token caused HTTP"); w.WriteHeader(500) }, RESTOptions{TokenProvider: func(context.Context) (string, error) {
		started.Add(1)
		entered <- struct{}{}
		<-blocked
		return "late-token", nil
	}})
	defer close(blocked)
	for i := 0; i < restConcurrency; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := client.Query(ctx, "GET", "key"); done <- err }()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("token provider was not invoked")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatal("token wait ignored caller abort")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("noncooperative token provider blocked caller")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if value, err := client.Query(ctx, "GET", "key"); value != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("saturated token admission ignored deadline")
	}
	if started.Load() != restConcurrency {
		t.Fatal("noncooperative token callbacks bypassed admission bound")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRESTCallerAbortDeadlineBodyAndReopen(t *testing.T) {
	for _, stage := range []string{"headers", "body", "deadline"} {
		t.Run(stage, func(t *testing.T) {
			started := make(chan struct{})
			disconnected := make(chan struct{}, 1)
			finishHandlers := make(chan struct{})
			var requests atomic.Int32
			options := RESTOptions{}
			if stage == "deadline" {
				options.Timeout = 500 * time.Millisecond
			}
			client, _ := restFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if !drainRESTFixtureRequest(w, r) {
					return
				}
				if requests.Add(1) > 1 {
					restReply(w, "$2\r\nok\r\n")
					return
				}
				if stage == "body" {
					w.Header().Set("Content-Type", "application/octet-stream")
					_, _ = io.WriteString(w, "$5\r\nab")
					w.(http.Flusher).Flush()
				}
				close(started)
				select {
				case <-r.Context().Done():
					disconnected <- struct{}{}
				case <-finishHandlers:
				case <-time.After(3 * time.Second):
				}
			}, options)
			// This cleanup runs before the fixture's client/server cleanup so
			// an assertion failure cannot leave httptest.Server.Close waiting.
			t.Cleanup(func() { close(finishHandlers) })
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			done := make(chan error, 1)
			go func() {
				value, err := client.QueryNative(ctx, "GET", "key")
				if value != nil {
					done <- errors.New("partial result escaped")
					return
				}
				done <- err
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("owned request did not reach service")
			}
			if stage != "deadline" {
				cancel(errors.New("synthetic-secret-cause"))
			}
			select {
			case err := <-done:
				want := context.Canceled
				if stage == "deadline" {
					want = context.DeadlineExceeded
				}
				if !errors.Is(err, want) || strings.Contains(err.Error(), "synthetic-secret") {
					t.Fatal("dispatch cancellation lost canonical redacted error")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("dispatch cancellation did not release caller")
			}
			select {
			case <-disconnected:
			case <-time.After(2 * time.Second):
				t.Fatal("owned HTTP server did not observe disconnect")
			}
			if got := mustRESTValue(t, client, "GET", "key"); string(got.(NativeBytes)) != "ok" {
				t.Fatal("query failed after cancellation")
			}
		})
	}
}

func TestRESTCloseCancelsOwnedReadsAndRejectsNewQueries(t *testing.T) {
	started := make(chan struct{}, restConcurrency)
	disconnected := make(chan struct{}, restConcurrency)
	finishHandlers := make(chan struct{})
	client, _ := restFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if !drainRESTFixtureRequest(w, r) {
			return
		}
		started <- struct{}{}
		select {
		case <-r.Context().Done():
			disconnected <- struct{}{}
		case <-finishHandlers:
		case <-time.After(3 * time.Second):
		}
	}, RESTOptions{})
	t.Cleanup(func() { close(finishHandlers) })
	done := make(chan error, restConcurrency)
	for i := 0; i < restConcurrency; i++ {
		go func() { _, err := client.Query(context.Background(), "GET", "key"); done <- err }()
	}
	for i := 0; i < restConcurrency; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("owned reads did not start")
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not release owned HTTP operations")
	}
	for i := 0; i < restConcurrency; i++ {
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatal("close did not cancel owned read")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("owned read leaked")
		}
		select {
		case <-disconnected:
		case <-time.After(2 * time.Second):
			t.Fatal("owned HTTP server did not observe Close disconnect")
		}
	}
	if value, err := client.Query(context.Background(), "GET", "key"); value != nil || !errors.Is(err, ErrRESTClosed) {
		t.Fatal("closed REST client reopened")
	}
}

func TestRESTReusesDALgoFlatKeysAndRefusesTransactions(t *testing.T) {
	client, _ := restFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var args []string
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil || len(args) != 2 || args[1] != "literal:key" {
			t.Error("DALgo key namespace changed")
			w.WriteHeader(400)
			return
		}
		if args[0] == "EXISTS" {
			restReply(w, ":1\r\n")
		} else {
			restReply(w, "$5\r\nvalue\r\n")
		}
	}, RESTOptions{})
	db := client.DB()
	item := record.NewRecordWithData(record.NewKeyWithID("ignored-collection", "literal:key"), map[string]any{})
	if err := db.Get(context.Background(), item); err != nil {
		t.Fatal(err)
	}
	if item.Data().(map[string]any)["value"] != "value" {
		t.Fatal("DALgo value changed")
	}
	if exists, err := db.Exists(context.Background(), item.Key()); err != nil || !exists {
		t.Fatal("DALgo existence failed")
	}
	called := false
	err := db.RunReadonlyTransaction(context.Background(), func(context.Context, dal.ReadTransaction) error { called = true; return nil })
	if !errors.Is(err, dal.ErrNotSupported) || called {
		t.Fatal("REST callback transaction was invented")
	}
}
