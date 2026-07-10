// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package produce

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/gen"
	"github.com/twmb/franz-go/pkg/kgo"
)

// latencyHeaderKey is the record header Run stamps with the send time
// (UnixNano, ASCII decimal) when WithLatencyHeader is set. consume.Run
// observes the same header key to compute end-to-end produce-to-consume
// latency.
const latencyHeaderKey = "lg-ts"

// Counters tracks records and bytes successfully produced across all client
// goroutines started by Run. It is safe for concurrent use.
type Counters struct {
	Sent  atomic.Int64
	Bytes atomic.Int64
}

// Option configures optional behavior of Run.
type Option func(*runOpts)

type runOpts struct {
	stampLatency bool
}

// WithLatencyHeader opts a workload into end-to-end latency measurement:
// Run stamps every produced record with a lg-ts header containing the send
// time, which consume.Run reads to compute produce-to-consume latency. It
// is off by default since it adds a header to every record.
func WithLatencyHeader() Option {
	return func(o *runOpts) { o.stampLatency = true }
}

// Run produces records pulled from src to topic on the cluster reachable via
// seeds, using clients concurrent kgo clients each paced by pacer, until ctx
// is done.
func Run(ctx context.Context, seeds []string, topic string, src gen.RecordSource, pacer Pacer, clients int, c *Counters, opts ...Option) error {
	var ro runOpts
	for _, opt := range opts {
		opt(&ro)
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if err := pacer.Wait(ctx); err != nil {
					return
				}
				v := src.Next()
				rec := &kgo.Record{Value: v}
				if ro.stampLatency {
					rec.Headers = []kgo.RecordHeader{{
						Key:   latencyHeaderKey,
						Value: []byte(strconv.FormatInt(time.Now().UnixNano(), 10)),
					}}
				}
				cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
					if err == nil {
						c.Sent.Add(1)
						c.Bytes.Add(int64(len(v)))
					}
				})
			}
		}()
	}
	wg.Wait()
	cl.Flush(context.Background())
	return nil
}
