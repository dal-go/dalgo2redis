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
STRLEN, HLEN, LLEN, SCARD and ZCARD. Each call accepts at most 128 arguments
and 64 KiB of command text, and has a ten second deadline. GET/HGET return nil
for absent values; MGET/HMGET preserve positions with nil entries. Redis errors
(including WRONGTYPE and ACL denial) remain errors. Values are strings.
Value sizes are controlled by Redis data and server ACLs; there is no hard
response byte limit. Multi-key reads are not a snapshot across calls.

`client.DB()` implements DALgo string record reads (`key` and `value` map
fields). Record IDs are exact Redis keys in the chosen numeric database;
collection names do not add a prefix. Structured queries, transactions, schema
discovery and writes return `dal.ErrNotSupported`.

Run unit tests with `go test ./...`. Set `REDIS_TEST_URL` to a disposable real
Redis database to run integration tests. Fixtures use unique keys and remove
only those keys. CI runs these tests against Redis 7.4.
