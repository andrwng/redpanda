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
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Counters tracks records and bytes received by Run. It is safe for
// concurrent use.
type Counters struct {
	Received atomic.Int64
	Bytes    atomic.Int64
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
		})
	}
	return nil
}
