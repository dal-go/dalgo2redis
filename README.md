# DALgo Redis adapter

Read Redis directly using native, explicitly allowed read commands. Redis is a
key-value database; this adapter does not translate SQL or enumerate all keys.

```go
client, err := dalgo2redis.Open(ctx, os.Getenv("REDIS_URL"))
if err != nil { return err }
defer client.Close()
value, err := client.Query(ctx, "GET", "customer:42")
values, err := client.Query(ctx, "HMGET", "customer:42", "name", "email")
```

`redis://user:password@host:6379/0` selects a numeric database. `rediss://`
enables verified TLS. Use environment variables for credentials and a dedicated
Redis ACL user restricted to the intended key patterns and read commands.
The adapter never exposes connection URLs or server error text in errors.

Allowed commands: GET, MGET, HGET, HMGET, HEXISTS, EXISTS, TYPE, TTL, PTTL,
STRLEN, HLEN, LLEN, SCARD, ZCARD and ZSCORE. Each call accepts at most 128 arguments
and 64 KiB of command text, and has a ten second deadline. GET/HGET return nil
for absent values; MGET/HMGET preserve positions with nil entries. Redis errors
(including WRONGTYPE and ACL denial) remain errors. Values are strings.
Each RESP reply is limited to 1 MiB before the Redis driver reads length
headers, preventing oversized bulk allocations. Oversized replies fail and close
the connection; later reads reconnect. RESP nesting is capped at 16 levels and
each array at 256 elements. Multi-key reads are not a snapshot across calls.

### Scoped one-page SCAN

Use trusted client configuration to enable enumeration:

```go
client, err := dalgo2redis.OpenWithOptions(ctx, rawURL, dalgo2redis.Options{
    ScanPrefix: "customer:",
})
// Handle err, then defer client.Close().
page, err := client.QueryNative(ctx, "SCAN", "0", "COUNT", "100")
```

Empty `ScanPrefix` disables SCAN. The prefix is literal, including glob and
backslash characters: the client forces an escaped `<prefix>*` MATCH. A supplied
MATCH must equal that exact pattern. Cursor is a decimal uint64; COUNT is 1 to
1,000 (default 100). Optional TYPE is forwarded as a literal type selector.
This performs one page only. Duplicate keys and empty nonterminal pages are
preserved; it is not a snapshot. COUNT is a server hint, not a result limit:
a reply exceeding 256 array elements or 1 MiB fails, without truncation. Malformed
cursors, wrong page shapes and any returned key outside the literal prefix cause
the entire page to fail. Whole-scan iteration/work limits belong to the caller.

The configuration must come from a trusted operator or server-owned resource,
not a browser request. Redis ACL key patterns alone do not filter SCAN output.
ScanPrefix scopes enumeration only; it does not restrict GET/MGET keys or replace
Redis ACLs. KEYS, writes, scripts, administrative and transaction commands remain
refused before I/O.

### Lossless native results and cancellation

`QueryNative` uses the same read allowlist but returns closed `NativeValue`
variants whose JSON matches the independent TypeScript RESP contract:

```json
{"kind":"array","values":[{"kind":"bytes","base64":"AP+ADQo="},{"kind":"integer","value":"9223372036854775807"},null]}
```

Bulk data, including SCAN cursor/key bytes, uses base64. RESP integers use signed
decimal strings; TYPE's simple status stays a string. Missing values are null,
and empty bytes/arrays remain distinct. Legacy `Query` and DALgo record reads
retain Go strings/int64 values; ordinary JSON encoding of those legacy strings
is for text and cannot preserve arbitrary non-UTF8 bytes or JavaScript int64s.

Reads use dedicated operation-owned go-redis drivers because socket deadlines
alone do not promptly interrupt an in-flight read on caller cancellation. Each
read reconnects/authenticates, with extra latency compared with pooled reuse.
Concurrent reads are admitted up to the original configured pool/active limit;
each driver owns at most one connection, and the initial verification connection
is closed before reads begin. Cancellation closes only that operation's driver.
Client.Close cancels and waits for active reads; queued reads cannot open after
close. The same ten-second journey budget includes admission, connect/auth,
execution and native conversion, with a fresh cancellation check before success.
Later reads can connect again after an aborted or oversized read.

`client.DB()` implements DALgo string record reads (`key` and `value` map
fields). Record IDs are exact Redis keys in the chosen numeric database;
collection names do not add a prefix. Structured queries, transactions, schema
discovery and writes return `dal.ErrNotSupported`.

Run unit tests with `go test ./...`. Set `REDIS_TEST_URL` to a disposable real
Redis database to run integration tests. Fixtures use unique keys and remove
only those keys. CI runs these tests against Redis 7.4.

### Upstash REST reads

`NewRESTClient(endpoint, RESTOptions)` is an independent HTTPS transport in this
same adapter. It sends the documented JSON command array to the endpoint root,
with `Authorization: Bearer` and `Upstash-Response-Format: resp2`. It does not
send `Upstash-Encoding`, use query-string tokens, follow redirects, or retry with
other credentials. See the [official REST contract](https://upstash.com/docs/redis/features/restapi).

```go
client, err := dalgo2redis.NewRESTClient(endpoint, dalgo2redis.RESTOptions{
    TokenProvider: func(ctx context.Context) (string, error) {
        return readScopedToken(ctx) // application-owned secret/token provider
    },
    Options: dalgo2redis.Options{ScanPrefix: "owned:"}, // omit to disable SCAN
})
// Handle err, then defer client.Close(). No network request occurs until Query.
value, err := client.QueryNative(ctx, "GET", "owned:key")
```

`Query`, `QueryNative`, and `DB` reuse the existing command policy, flat-key
DALgo backend, lossless native values and SCAN response checks. JSON command
arguments and the configured prefix must be valid UTF-8; invalid input is
refused rather than changing a binary key during JSON encoding. Returned bulk
bytes, including non-UTF8 data and SCAN key suffixes, remain exact. Legacy Go
strings/int64 and lossless tagged JSON are the same two contracts as TCP.

The complete encoded command must fit 64 KiB. RESP2 replies retain the shared
1 MiB frame, 256-element array and depth-16 limits, and must contain exactly one
frame. A whole page exceeding its byte/shape/prefix bounds fails without partial
results. SCAN's COUNT remains a hint; no snapshot is implied. Upstash's default
Read Only token can deny SCAN/KEYS. Enumeration requires a separately permitted,
scoped ACL-user REST token, and never falls back to KEYS or an admin token.

Each client owns a cloned standard HTTP transport with TLS verification,
TLS1.2 minimum, no proxies, a 64 KiB response-header cap and four concurrent
requests. HTTP loopback is available only through the explicit
`InsecureLoopbackForTesting` option and an IP loopback endpoint. Supplied custom
dialing/TLS callbacks are trusted application configuration. Token providers
receive the same end-to-end context as fetch, body decoding and conversion;
the default deadline is ten seconds. Caller cancellation and deadline errors
retain the standard context sentinels without custom causes or backend text.
Cleanup precedes the final delivery check. Close cancels owned requests and
closes their idle connections, without touching a TCP client's pool. A token
provider that ignores context can retain its own work, but at most four calls
per REST client remain outstanding and neither callers nor Close wait for them.

Only read commands are implemented. The service has writes and atomic
`multi-exec` batches, but this adapter adds neither. Callback transactions,
structured queries and schema discovery remain refused. Tests use owned
HTTP/TLS contract fixtures; they do not establish hosted Upstash authentication,
quota behavior, CORS or installed CLI support. No hosted account is required
for those local fixtures.
