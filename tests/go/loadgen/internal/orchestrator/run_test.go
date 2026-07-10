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

	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/config"
	"github.com/twmb/franz-go/pkg/kfake"
)

// newSchemaRegistryMock models the three-request flow franz-go's
// sr.Client.CreateSchema performs: POST to register, then a GET by ID and a
// GET by subject/version to fetch back the full SubjectSchema.
func newSchemaRegistryMock(t *testing.T, subject, protoText string, id int) *httptest.Server {
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
				"schema":     protoText,
				"schemaType": "PROTOBUF",
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

	sr := newSchemaRegistryMock(t, "t-value", protoText, 1)
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
	if err := Run(ctx, c, []string{dir}); err != nil {
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

	sr := newSchemaRegistryMock(t, "t-value", protoText, 1)
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
	runErr := Run(ctx, c, []string{dir})
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
