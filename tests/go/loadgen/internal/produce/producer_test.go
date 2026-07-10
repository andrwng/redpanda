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
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
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

	err = Run(ctx, cluster.ListenAddrs(), "t", staticSource{}, NewSteadyPacer(0), 2, &c)
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
