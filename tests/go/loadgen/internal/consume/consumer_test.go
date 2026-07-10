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
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRunConsumesProduced(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	p, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DefaultProduceTopic("t"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		p.Produce(context.Background(), &kgo.Record{Value: []byte("x")}, nil)
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Close()

	var c Counters
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = Run(ctx, cluster.ListenAddrs(), "t", "g1", 0, 1, &c)
	if c.Received.Load() == 0 {
		t.Fatal("expected consumed records")
	}
}

func TestRunWithLagSeeksNearNow(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	p, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DefaultProduceTopic("t"))
	if err != nil {
		t.Fatal(err)
	}
	// Records seeded well before "now"; a large lag should still pick them
	// up since AfterMilli(now-lag) is before their produce time.
	for i := 0; i < 5; i++ {
		p.Produce(context.Background(), &kgo.Record{Value: []byte("y")}, nil)
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Close()

	var c Counters
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = Run(ctx, cluster.ListenAddrs(), "t", "g2", time.Hour, 1, &c)
	if c.Received.Load() == 0 {
		t.Fatal("expected consumed records with lag seek")
	}
}

func TestLatencyFromHeader(t *testing.T) {
	c := &Counters{}
	ts := time.Now().Add(-5 * time.Millisecond)
	c.observe(headerNanos(ts))
	p50, _ := c.LatencyStats()
	if p50 < 3*time.Millisecond {
		t.Fatalf("p50 = %v, expected >=~5ms", p50)
	}
}

func TestRunObservesLatencyHeader(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	p, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DefaultProduceTopic("t"))
	if err != nil {
		t.Fatal(err)
	}
	sent := time.Now().Add(-5 * time.Millisecond)
	rec := &kgo.Record{
		Value: []byte("x"),
		Headers: []kgo.RecordHeader{{
			Key:   "lg-ts",
			Value: []byte(strconv.FormatInt(sent.UnixNano(), 10)),
		}},
	}
	if err := p.ProduceSync(context.Background(), rec).FirstErr(); err != nil {
		t.Fatal(err)
	}
	p.Close()

	var c Counters
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = Run(ctx, cluster.ListenAddrs(), "t", "g3", 0, 1, &c)
	p50, _ := c.LatencyStats()
	if p50 < 3*time.Millisecond {
		t.Fatalf("p50 = %v, expected the lg-ts header to be observed (~5ms)", p50)
	}
}
