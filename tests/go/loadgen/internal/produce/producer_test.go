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
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

type staticSource struct{}

func (staticSource) Next() []byte { return []byte("hello") }

func TestRunProducesToKfake(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	var c Counters
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err = Run(ctx, cluster.ListenAddrs(), "t", staticSource{}, NewSteadyPacer(0), 2, &c, nil)
	if err != nil && ctx.Err() == nil {
		t.Fatalf("Run failed: %v", err)
	}
	if c.Sent.Load() == 0 {
		t.Fatal("expected some records produced")
	}
	if c.Bytes.Load() != c.Sent.Load()*int64(len("hello")) {
		t.Fatalf("bytes counter %d does not match sent*len(hello) %d", c.Bytes.Load(), c.Sent.Load()*int64(len("hello")))
	}
}

func TestRunWithLatencyHeaderStampsSendTime(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	var c Counters
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	before := time.Now().UnixNano()
	err = Run(ctx, cluster.ListenAddrs(), "t", staticSource{}, NewSteadyPacer(0), 1, &c, nil, WithLatencyHeader())
	after := time.Now().UnixNano()
	if err != nil && ctx.Err() == nil {
		t.Fatalf("Run failed: %v", err)
	}
	if c.Sent.Load() == 0 {
		t.Fatal("expected some records produced")
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumeTopics("t"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	fetchCtx, fetchCancel := context.WithTimeout(context.Background(), time.Second)
	defer fetchCancel()
	fs := cl.PollFetches(fetchCtx)
	var found bool
	fs.EachRecord(func(r *kgo.Record) {
		for _, h := range r.Headers {
			if h.Key != "lg-ts" {
				continue
			}
			found = true
			ns, err := strconv.ParseInt(string(h.Value), 10, 64)
			if err != nil {
				t.Fatalf("lg-ts header %q is not a valid int64: %v", h.Value, err)
			}
			if ns < before || ns > after {
				t.Fatalf("lg-ts %d outside produce window [%d, %d]", ns, before, after)
			}
		}
	})
	if !found {
		t.Fatal("expected lg-ts header on produced record")
	}
}

func TestRunWithoutLatencyHeaderOmitsHeader(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	var c Counters
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err = Run(ctx, cluster.ListenAddrs(), "t", staticSource{}, NewSteadyPacer(0), 1, &c, nil)
	if err != nil && ctx.Err() == nil {
		t.Fatalf("Run failed: %v", err)
	}
	if c.Sent.Load() == 0 {
		t.Fatal("expected some records produced")
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumeTopics("t"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	fetchCtx, fetchCancel := context.WithTimeout(context.Background(), time.Second)
	defer fetchCancel()
	fs := cl.PollFetches(fetchCtx)
	fs.EachRecord(func(r *kgo.Record) {
		if len(r.Headers) != 0 {
			t.Fatalf("expected no headers without WithLatencyHeader, got %v", r.Headers)
		}
	})
}
