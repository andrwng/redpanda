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
