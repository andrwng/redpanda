# loadgen

A standalone franz-go CLI that drives sustained load at a single Redpanda cluster
with **schema-valid, Schema-Registry-encoded** structured data (Protobuf or Avro), to
exercise Iceberg Topics. Records are generated and encoded in Go, then produced/consumed
directly via franz-go. It also ships a `derecurse` subcommand that makes a recursive
Protobuf schema Iceberg-compatible.

All examples run from this directory (`tests/go/loadgen`). Build a binary with
`go build -o loadgen ./cmd/loadgen`, or use `go run ./cmd/loadgen` as shown.

## Generate load (`run`)

`run` reads a YAML file of workloads and drives them concurrently. Extra positional
arguments are Protobuf **import roots** (needed only to resolve `import` statements in a
`.proto`; ignored for Avro).

```
go run ./cmd/loadgen --config workloads.yaml [--shard.count N --shard.index I] [import-root ...]
```

### Minimal Protobuf workload — drive max load

`workloads.yaml`:

```yaml
brokers: rp-1:9092,rp-2:9092
schema_registry: http://rp-1:8081
metrics_addr: ":9100"            # optional Prometheus /metrics endpoint
workloads:
  - name: orders
    schema:
      file: /path/to/schema.proto
      format: protobuf
      message: com.example.v1.Root   # fully-qualified root message
      subject: orders-value
    topic: orders
    direction: produce
    data:
      source: pre_encoded          # bake a pool once, replay (max throughput)
      pool_size: 20000
      seed: 1
    throughput:
      rate: 0                      # 0 = unthrottled → push to saturation
    clients: 8                     # parallel producer goroutines
```

```
# import root(s) resolve the schema's imports (e.g. google/type/*)
go run ./cmd/loadgen --config workloads.yaml /path/to/proto/import/root
```

A throughput line is printed per workload on exit; if `metrics_addr` is set, live
counters are at `http://localhost:9100/metrics`. To find the cluster's ceiling, raise
`clients` (and add hosts — see Multi-host) until aggregate throughput plateaus.

### Avro workload

Avro schemas are self-contained, so no import roots and no `message` field:

```yaml
workloads:
  - name: events
    schema:
      file: /path/to/schema.avsc
      format: avro
      subject: events-value
    topic: events
    direction: produce
    data: { source: pre_encoded, pool_size: 10000, seed: 1 }
    throughput: { rate: 50000 }    # steady 50k rec/s
    clients: 4
```

```
go run ./cmd/loadgen --config workloads.yaml
```

### Produce + consume with lag, oscillating rate

```yaml
workloads:
  - name: orders
    schema: { file: /path/to/schema.proto, format: protobuf, message: com.example.v1.Root, subject: orders-value }
    topic: orders
    direction: produce_consume     # runs a producer AND a consumer
    group: loadgen-orders          # consumer group
    consume_lag: 30s               # consume ~30s behind head (backlog/catch-up)
    data: { source: fresh }        # generate a new record per message (unbounded variety)
    throughput:
      profile: oscillating         # bursty load between min and max
      oscillating: { min: 10000, max: 200000, period: 60s, shape: sine }
    clients: 4
```

## Multi-host

`loadgen` is a single-host process. To scale past one box's limit, run it on N hosts and
pass `--shard.count N --shard.index I` (0-based): each host takes `rate / count` of the
target. Consumers with the same `group` have partitions distributed automatically by
Kafka. Aggregate per-host `/metrics` in Prometheus.

```
# host 0 of 3
go run ./cmd/loadgen --config workloads.yaml --shard.count 3 --shard.index 0  import-root
```

## Make a schema Iceberg-compatible (`derecurse`)

Redpanda's Iceberg translation rejects recursive Protobuf (any type whose name repeats on
a descent path) and caps nesting at depth 100. `derecurse` removes the minimal set of
recursive fields (DFS back-edges) and verifies the result is acyclic and within the depth
cap.

```
go run ./cmd/loadgen derecurse \
  --in schema.proto \
  --imports /path/to/proto/import/root \
  --imports . \
  --out schema_acyclic.proto
# prints e.g. "97 fields cut, 0 cycles, max depth 62"
```

`--imports` is repeatable (one per import root). Use `schema_acyclic.proto` as the
workload `schema.file`.

## Config reference

| Field | Meaning |
| --- | --- |
| `brokers` | comma-separated seed brokers |
| `schema_registry` | Schema Registry URL |
| `metrics_addr` | optional `host:port` for a Prometheus `/metrics` endpoint |
| `shard` | `{count, index}` — this host's slice (overridable via `--shard.*`) |
| `workloads[].schema` | `{file, format: protobuf\|avro, message, subject}` (`message` is Protobuf-only) |
| `workloads[].direction` | `produce` \| `consume` \| `produce_consume` |
| `workloads[].data` | `{source: pre_encoded\|fresh, pool_size, seed}` |
| `workloads[].throughput` | `{rate, profile: steady\|oscillating, oscillating: {min,max,period,shape}}` (`rate: 0` = unthrottled) |
| `workloads[].group` | consumer group (consume / produce_consume) |
| `workloads[].consume_lag` | start consuming this far behind head |
| `workloads[].clients` | parallel producer/consumer goroutines |

## Testing

```
go test ./...          # unit + kfake/httptest integration tests, no external services
go test ./... -race
```
