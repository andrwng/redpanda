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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/config"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/consume"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/gen"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/metrics"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/produce"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/rpkprofile"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/schema"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
)

func joinAddrs(a []string) string { return strings.Join(a, ",") }

// Run builds and drives every workload in c against a real cluster,
// printing a throughput report for each side (produce and/or consume) when
// it stops. "produce" and "produce_consume" run a produce.Run goroutine;
// "consume" and "produce_consume" run a consume.Run goroutine. For
// produce_consume, the consume side waits ConsumeLag before starting so it
// lags behind the produce side by that amount.
//
// recs and bytesVec are the CounterVecs returned by metrics.Serve; each
// workload goroutine periodically reflects its atomic Counters into them
// labeled by workload name. Both are nil when metrics are disabled (see
// startSampler), in which case Run runs exactly as before metrics existed.
func Run(ctx context.Context, c *config.Config, importPaths []string, recs, bytesVec *prometheus.CounterVec) error {
	seeds, kgoOpts, srOpts, err := resolveConnection(c)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2*len(c.Workloads))
	for _, w := range c.Workloads {
		if w.Direction == "produce" || w.Direction == "produce_consume" {
			src, err := newSource(ctx, c, w, importPaths, srOpts)
			if err != nil {
				return fmt.Errorf("workload %q: %w", w.Name, err)
			}

			var pacer produce.Pacer
			if w.Throughput.Profile == "oscillating" && w.Throughput.Oscillating != nil {
				o := w.Throughput.Oscillating
				pacer = produce.NewOscillatingPacer(o.Min/c.Shard.Count, o.Max/c.Shard.Count, o.Period, o.Shape, nil)
			} else {
				pacer = produce.NewSteadyPacer(w.Throughput.Rate / c.Shard.Count)
			}
			var counters produce.Counters
			wg.Add(1)
			go func(w config.Workload) {
				defer wg.Done()
				start := time.Now()
				stop, sampleWG := startSampler(recs, bytesVec, w.Name, counters.Sent.Load, counters.Bytes.Load)
				err := produce.Run(ctx, seeds, w.Topic, src, pacer, w.Clients, &counters, kgoOpts)
				close(stop)
				sampleWG.Wait()
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

		if w.Direction == "consume" || w.Direction == "produce_consume" {
			var counters consume.Counters
			wg.Add(1)
			go func(w config.Workload) {
				defer wg.Done()
				if w.Direction == "produce_consume" && w.ConsumeLag > 0 {
					select {
					case <-time.After(w.ConsumeLag):
					case <-ctx.Done():
						return
					}
				}
				start := time.Now()
				stop, sampleWG := startSampler(recs, bytesVec, w.Name+"/consume", counters.Received.Load, counters.Bytes.Load)
				err := consume.Run(ctx, seeds, w.Topic, w.Group, w.ConsumeLag, w.Clients, &counters, kgoOpts)
				close(stop)
				sampleWG.Wait()
				metrics.Report(os.Stdout, w.Name+"/consume", metrics.Sample{
					Records: counters.Received.Load(),
					Bytes:   counters.Bytes.Load(),
					Window:  time.Since(start),
				})
				if err != nil {
					errs <- fmt.Errorf("workload %q: %w", w.Name, err)
				}
			}(w)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

// resolveConnection derives the kafka seed brokers and, when c.Profile is
// set, the franz-go client options and Schema Registry client options that
// authenticate against it via an rpk profile (see rpkprofile.Load). Without
// a profile, seeds come from c.Brokers and both option slices are nil,
// matching loadgen's behavior before profile support existed.
func resolveConnection(c *config.Config) (seeds []string, kgoOpts []kgo.Opt, srOpts []sr.ClientOpt, err error) {
	if c.Profile == "" {
		return strings.Split(c.Brokers, ","), nil, nil, nil
	}
	p, err := rpkprofile.Load(c.Profile)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load rpk profile %q: %w", c.Profile, err)
	}
	kgoOpts, err = p.KgoOpts()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("profile %q: %w", c.Profile, err)
	}
	srOpts, err = p.SROpts(c.SchemaRegistry)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("profile %q: %w", c.Profile, err)
	}
	return p.KafkaAPI.Brokers, kgoOpts, srOpts, nil
}

// newSource loads and registers the workload's schema, then builds the
// record source w.Data.Source selects: "fresh" generates and frames a new
// record on every Next call, while "pre_encoded" bakes a pool of pre-framed
// records up front so the producer hot path pays no generation or
// schema-registry cost while running. srOpts authenticates the schema
// registry client when c.Profile is set (see resolveConnection).
func newSource(ctx context.Context, c *config.Config, w config.Workload, importPaths []string, srOpts []sr.ClientOpt) (gen.RecordSource, error) {
	switch w.Schema.Format {
	case "avro":
		return newAvroSource(ctx, c, w, srOpts)
	default:
		return newProtobufSource(ctx, c, w, importPaths, srOpts)
	}
}

func newProtobufSource(ctx context.Context, c *config.Config, w config.Workload, importPaths []string, srOpts []sr.ClientOpt) (gen.RecordSource, error) {
	md, err := schema.LoadProto(w.Schema.File, importPaths, w.Schema.Message)
	if err != nil {
		return nil, err
	}
	protoText, err := os.ReadFile(w.Schema.File)
	if err != nil {
		return nil, err
	}
	id, err := schema.RegisterProtobuf(ctx, c.SchemaRegistry, w.Schema.Subject, string(protoText), srOpts...)
	if err != nil {
		return nil, err
	}

	g := gen.NewProtoGen(md, w.Data.Seed, 100, 3)
	frame := func(b []byte) []byte { return wire.FrameProtobuf(id, []int{0}, b) }
	if w.Data.Source == "fresh" {
		return gen.NewFresh(g.Record, frame), nil
	}
	return gen.NewPool(g.Record, frame, w.Data.PoolSize)
}

func newAvroSource(ctx context.Context, c *config.Config, w config.Workload, srOpts []sr.ClientOpt) (gen.RecordSource, error) {
	schemaText, err := os.ReadFile(w.Schema.File)
	if err != nil {
		return nil, err
	}
	id, err := schema.RegisterAvro(ctx, c.SchemaRegistry, w.Schema.Subject, string(schemaText), srOpts...)
	if err != nil {
		return nil, err
	}

	g, err := gen.NewAvroGen(string(schemaText), w.Data.Seed, 3)
	if err != nil {
		return nil, err
	}
	frame := func(b []byte) []byte { return wire.FrameAvro(id, b) }
	if w.Data.Source == "fresh" {
		return gen.NewFresh(g.Record, frame), nil
	}
	return gen.NewPool(g.Record, frame, w.Data.PoolSize)
}
