// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package orchestrator

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/config"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/gen"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/metrics"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/produce"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/schema"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/wire"
)

func joinAddrs(a []string) string { return strings.Join(a, ",") }

// Run builds and drives every produce-side workload in c — currently
// "produce" and the produce half of "produce_consume" — against a real
// cluster, printing a throughput report for each workload when it stops.
// Consume-only workloads are skipped; later tasks extend Run to cover them.
func Run(ctx context.Context, c *config.Config, importPaths []string) error {
	seeds := strings.Split(c.Brokers, ",")

	var wg sync.WaitGroup
	errs := make(chan error, len(c.Workloads))
	for _, w := range c.Workloads {
		if w.Direction != "produce" && w.Direction != "produce_consume" {
			continue
		}

		src, err := newPreEncodedSource(ctx, c, w, importPaths)
		if err != nil {
			return fmt.Errorf("workload %q: %w", w.Name, err)
		}

		rate := w.Throughput.Rate / c.Shard.Count
		var counters produce.Counters
		wg.Add(1)
		go func(w config.Workload) {
			defer wg.Done()
			start := time.Now()
			err := produce.Run(ctx, seeds, w.Topic, src, produce.NewSteadyPacer(rate), w.Clients, &counters)
			metrics.Report(os.Stdout, w.Name, metrics.Sample{
				Records: counters.Sent.Load(),
				Bytes:   counters.Bytes.Load(),
				Window:  time.Since(start),
			})
			if err != nil {
				errs <- fmt.Errorf("workload %q: %w", w.Name, err)
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

// newPreEncodedSource loads and registers the workload's schema, then bakes
// a pool of pre-framed records so the producer hot path pays no generation
// or schema-registry cost while running.
func newPreEncodedSource(ctx context.Context, c *config.Config, w config.Workload, importPaths []string) (gen.RecordSource, error) {
	md, err := schema.LoadProto(w.Schema.File, importPaths, w.Schema.Message)
	if err != nil {
		return nil, err
	}
	protoText, err := os.ReadFile(w.Schema.File)
	if err != nil {
		return nil, err
	}
	id, err := schema.RegisterProtobuf(ctx, c.SchemaRegistry, w.Schema.Subject, string(protoText))
	if err != nil {
		return nil, err
	}

	g := gen.NewProtoGen(md, w.Data.Seed, 100, 3)
	frame := func(b []byte) []byte { return wire.FrameProtobuf(id, []int{0}, b) }
	return gen.NewPool(g.Record, frame, w.Data.PoolSize)
}
