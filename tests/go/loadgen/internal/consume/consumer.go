// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package consume

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// latencyHeaderKey is the record header produce.Run's WithLatencyHeader
// option stamps with the send time (UnixNano, ASCII decimal). Run observes
// it here to compute end-to-end produce-to-consume latency.
const latencyHeaderKey = "lg-ts"

// Counters tracks records and bytes received by Run, plus any end-to-end
// latency observed via the lg-ts header. It is safe for concurrent use.
type Counters struct {
	Received atomic.Int64
	Bytes    atomic.Int64
	lat      latency
}

// latency is an unsorted reservoir of observed end-to-end latencies, guarded
// by mu since Run's record loop and LatencyStats may run concurrently.
type latency struct {
	mu  sync.Mutex
	obs []time.Duration
}

// headerNanos returns t as the UnixNano encoding stamped in the lg-ts
// header.
func headerNanos(t time.Time) int64 { return t.UnixNano() }

// observe records the elapsed time since sentNanos, a UnixNano send
// timestamp, into c's latency reservoir.
func (c *Counters) observe(sentNanos int64) {
	d := time.Duration(time.Now().UnixNano() - sentNanos)
	c.lat.mu.Lock()
	c.lat.obs = append(c.lat.obs, d)
	c.lat.mu.Unlock()
}

// LatencyStats returns the p50 and p99 end-to-end latency across every
// observation recorded via observe. Both are zero if nothing has been
// observed, which is the case unless the producing workload opted into
// produce.WithLatencyHeader.
func (c *Counters) LatencyStats() (p50, p99 time.Duration) {
	c.lat.mu.Lock()
	defer c.lat.mu.Unlock()
	if len(c.lat.obs) == 0 {
		return 0, 0
	}
	s := append([]time.Duration(nil), c.lat.obs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)*50/100], s[min(len(s)-1, len(s)*99/100)]
}

// parseTSHeader parses a lg-ts header value (a UnixNano timestamp encoded
// as ASCII decimal).
func parseTSHeader(v []byte) (int64, bool) {
	n, err := strconv.ParseInt(string(v), 10, 64)
	return n, err == nil
}

// Run joins group on the cluster reachable via seeds and consumes topic
// until ctx is done, dropping every record after counting it in c. clients
// is accepted for symmetry with produce.Run; the underlying kgo client
// manages its own fetch concurrency internally, so a single client serves
// all group members.
//
// If lag is positive, consumption starts from the first offset at or after
// now-lag instead of the group's committed offsets.
func Run(ctx context.Context, seeds []string, topic, group string, lag time.Duration, clients int, c *Counters) error {
	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
	}
	if lag > 0 {
		at := time.Now().Add(-lag)
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(at.UnixMilli())))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()

	for ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		fs.EachRecord(func(r *kgo.Record) {
			c.Received.Add(1)
			c.Bytes.Add(int64(len(r.Value)))
			for _, h := range r.Headers {
				if h.Key != latencyHeaderKey {
					continue
				}
				if ns, ok := parseTSHeader(h.Value); ok {
					c.observe(ns)
				}
				break
			}
		})
	}
	return nil
}
