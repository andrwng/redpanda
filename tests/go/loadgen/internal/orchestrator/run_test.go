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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/config"
	"github.com/twmb/franz-go/pkg/kfake"
)

// newSchemaRegistryMock models the three-request flow franz-go's
// sr.Client.CreateSchema performs: POST to register, then a GET by ID and a
// GET by subject/version to fetch back the full SubjectSchema.
func newSchemaRegistryMock(t *testing.T, subject, schemaText, schemaType string, id int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var body any
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/subjects/"+subject+"/versions":
			body = map[string]any{"id": id}
		case r.Method == http.MethodGet && r.URL.Path == fmt.Sprintf("/schemas/ids/%d/versions", id):
			body = []map[string]any{{"subject": subject, "version": 1}}
		case r.Method == http.MethodGet && r.URL.Path == "/subjects/"+subject+"/versions/1":
			body = map[string]any{
				"subject":    subject,
				"version":    1,
				"id":         id,
				"schema":     schemaText,
				"schemaType": schemaType,
			}
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Fatal(err)
		}
	}))
}

func TestRunProduceEndToEnd(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	const protoText = `syntax="proto3"; package demo; message Root { string id = 1; int64 n = 2; }`

	sr := newSchemaRegistryMock(t, "t-value", protoText, "PROTOBUF", 1)
	defer sr.Close()

	dir := t.TempDir()
	if err := os.WriteFile(dir+"/s.proto", []byte(protoText), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &config.Config{
		Brokers:        joinAddrs(cluster.ListenAddrs()),
		SchemaRegistry: sr.URL,
		Shard:          config.Shard{Count: 1, Index: 0},
		Workloads: []config.Workload{{
			Name: "w", Topic: "t", Direction: "produce", Clients: 2,
			Schema: config.Schema{File: dir + "/s.proto", Format: "protobuf", Message: "demo.Root", Subject: "t-value"},
			Data:   config.Data{Source: "pre_encoded", PoolSize: 50, Seed: 1},
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := Run(ctx, c, []string{dir}, nil, nil); err != nil {
		t.Fatal(err)
	}
}

// TestRunProduceRecordsPrometheusCounters exercises the metrics wiring: when
// Run is given non-nil CounterVecs, it must reflect records produced into
// them before returning, even though the run is far shorter than the
// sampler's 1s tick - the final flush on the workload's stop channel (see
// startSampler) is what makes this observable without waiting a full tick.
func TestRunProduceRecordsPrometheusCounters(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	const protoText = `syntax="proto3"; package demo; message Root { string id = 1; int64 n = 2; }`

	sr := newSchemaRegistryMock(t, "t-value", protoText, "PROTOBUF", 1)
	defer sr.Close()

	dir := t.TempDir()
	if err := os.WriteFile(dir+"/s.proto", []byte(protoText), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &config.Config{
		Brokers:        joinAddrs(cluster.ListenAddrs()),
		SchemaRegistry: sr.URL,
		Shard:          config.Shard{Count: 1, Index: 0},
		Workloads: []config.Workload{{
			Name: "w", Topic: "t", Direction: "produce", Clients: 2,
			Schema: config.Schema{File: dir + "/s.proto", Format: "protobuf", Message: "demo.Root", Subject: "t-value"},
			Data:   config.Data{Source: "pre_encoded", PoolSize: 50, Seed: 1},
		}},
	}
	recs := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_records_total"}, []string{"workload"})
	bytesVec := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_bytes_total"}, []string{"workload"})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := Run(ctx, c, []string{dir}, recs, bytesVec); err != nil {
		t.Fatal(err)
	}

	if got := testutil.ToFloat64(recs.WithLabelValues("w")); got <= 0 {
		t.Fatalf("records counter for workload %q = %v, want > 0", "w", got)
	}
	if got := testutil.ToFloat64(bytesVec.WithLabelValues("w")); got <= 0 {
		t.Fatalf("bytes counter for workload %q = %v, want > 0", "w", got)
	}
}

// TestRunProduceFreshSource exercises the "fresh" data source end to end,
// confirming newSource routes to gen.NewFresh instead of the pre_encoded pool.
func TestRunProduceFreshSource(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	const protoText = `syntax="proto3"; package demo; message Root { string id = 1; int64 n = 2; }`

	sr := newSchemaRegistryMock(t, "t-value", protoText, "PROTOBUF", 1)
	defer sr.Close()

	dir := t.TempDir()
	if err := os.WriteFile(dir+"/s.proto", []byte(protoText), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &config.Config{
		Brokers:        joinAddrs(cluster.ListenAddrs()),
		SchemaRegistry: sr.URL,
		Shard:          config.Shard{Count: 1, Index: 0},
		Workloads: []config.Workload{{
			Name: "w", Topic: "t", Direction: "produce", Clients: 2,
			Schema: config.Schema{File: dir + "/s.proto", Format: "protobuf", Message: "demo.Root", Subject: "t-value"},
			Data:   config.Data{Source: "fresh", Seed: 1},
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := Run(ctx, c, []string{dir}, nil, nil); err != nil {
		t.Fatal(err)
	}
}

// TestRunProduceAvroSource exercises the "avro" schema format end to end,
// confirming newSource routes to RegisterAvro/NewAvroGen and frames records
// with wire.FrameAvro instead of the protobuf path.
func TestRunProduceAvroSource(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	const avroText = `{"type":"record","name":"Root","fields":[{"name":"id","type":"string"},{"name":"n","type":"long"}]}`

	sr := newSchemaRegistryMock(t, "t-value", avroText, "AVRO", 1)
	defer sr.Close()

	dir := t.TempDir()
	if err := os.WriteFile(dir+"/s.avsc", []byte(avroText), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &config.Config{
		Brokers:        joinAddrs(cluster.ListenAddrs()),
		SchemaRegistry: sr.URL,
		Shard:          config.Shard{Count: 1, Index: 0},
		Workloads: []config.Workload{{
			Name: "w", Topic: "t", Direction: "produce", Clients: 2,
			Schema: config.Schema{File: dir + "/s.avsc", Format: "avro", Subject: "t-value"},
			Data:   config.Data{Source: "fresh", Seed: 1},
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := Run(ctx, c, []string{dir}, nil, nil); err != nil {
		t.Fatal(err)
	}
}

// TestRunProduceConsumeCoordinatesLag exercises the produce_consume
// direction end to end: the produce side writes protobuf records while the
// consume side, started ConsumeLag after the produce side, joins a consumer
// group on the same topic and must observe records produced before it
// started.
func TestRunProduceConsumeCoordinatesLag(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	const protoText = `syntax="proto3"; package demo; message Root { string id = 1; int64 n = 2; }`

	sr := newSchemaRegistryMock(t, "t-value", protoText, "PROTOBUF", 1)
	defer sr.Close()

	dir := t.TempDir()
	if err := os.WriteFile(dir+"/s.proto", []byte(protoText), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &config.Config{
		Brokers:        joinAddrs(cluster.ListenAddrs()),
		SchemaRegistry: sr.URL,
		Shard:          config.Shard{Count: 1, Index: 0},
		Workloads: []config.Workload{{
			Name: "w", Topic: "t", Direction: "produce_consume", Clients: 2,
			Group: "g", ConsumeLag: 50 * time.Millisecond,
			Schema: config.Schema{File: dir + "/s.proto", Format: "protobuf", Message: "demo.Root", Subject: "t-value"},
			Data:   config.Data{Source: "pre_encoded", PoolSize: 50, Seed: 1},
		}},
	}

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	realStdout := os.Stdout
	os.Stdout = w
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	runErr := Run(ctx, c, []string{dir}, nil, nil)
	os.Stdout = realStdout
	w.Close()
	var out bytes.Buffer
	if _, err := out.ReadFrom(r); err != nil {
		t.Fatal(err)
	}
	if runErr != nil {
		t.Fatal(runErr)
	}
	if !strings.Contains(out.String(), "w/consume") {
		t.Fatalf("expected consume report in output, got: %s", out.String())
	}
	if strings.Contains(out.String(), "(0 records") {
		t.Fatalf("expected consume side to receive records, got: %s", out.String())
	}
}

// TestRunProduceConsumeSeparatesPrometheusLabels exercises a produce_consume
// workload's metrics wiring end to end: the produce side must be tracked
// under the workload name while the consume side must land under
// "<name>/consume", matching the stdout Report labels. Before the fix both
// samplers were started with the same label, so the produce and consume
// deltas were merged into one series and "<name>/consume" was never touched;
// asserting it is > 0 here is what pins that regression. The run is long
// enough to observe both the sampler's periodic 1s tick and its final flush
// on stop, not just the final flush alone (see
// TestRunProduceRecordsPrometheusCounters for a final-flush-only check).
func TestRunProduceConsumeSeparatesPrometheusLabels(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	const protoText = `syntax="proto3"; package demo; message Root { string id = 1; int64 n = 2; }`

	sr := newSchemaRegistryMock(t, "t-value", protoText, "PROTOBUF", 1)
	defer sr.Close()

	dir := t.TempDir()
	if err := os.WriteFile(dir+"/s.proto", []byte(protoText), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &config.Config{
		Brokers:        joinAddrs(cluster.ListenAddrs()),
		SchemaRegistry: sr.URL,
		Shard:          config.Shard{Count: 1, Index: 0},
		Workloads: []config.Workload{{
			Name: "w", Topic: "t", Direction: "produce_consume", Clients: 2,
			Group: "g", ConsumeLag: 100 * time.Millisecond,
			Schema: config.Schema{File: dir + "/s.proto", Format: "protobuf", Message: "demo.Root", Subject: "t-value"},
			Data:   config.Data{Source: "pre_encoded", PoolSize: 50, Seed: 1},
		}},
	}
	recs := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_records_total"}, []string{"workload"})
	bytesVec := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_bytes_total"}, []string{"workload"})

	// Long enough that, past the 100ms consume lag, the consume-side
	// sampler is still running well past its first 1s tick before ctx
	// cancellation triggers the final flush.
	ctx, cancel := context.WithTimeout(context.Background(), 1800*time.Millisecond)
	defer cancel()
	if err := Run(ctx, c, []string{dir}, recs, bytesVec); err != nil {
		t.Fatal(err)
	}

	if got := testutil.ToFloat64(recs.WithLabelValues("w")); got <= 0 {
		t.Fatalf("produce records counter for workload %q = %v, want > 0", "w", got)
	}
	if got := testutil.ToFloat64(bytesVec.WithLabelValues("w")); got <= 0 {
		t.Fatalf("produce bytes counter for workload %q = %v, want > 0", "w", got)
	}
	if got := testutil.ToFloat64(recs.WithLabelValues("w/consume")); got <= 0 {
		t.Fatalf("consume records counter for workload %q = %v, want > 0 (distinct from produce series)", "w/consume", got)
	}
	if got := testutil.ToFloat64(bytesVec.WithLabelValues("w/consume")); got <= 0 {
		t.Fatalf("consume bytes counter for workload %q = %v, want > 0 (distinct from produce series)", "w/consume", got)
	}
}
