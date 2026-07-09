// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"os"
	"testing"
)

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadValidatesDirection(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/c.yaml"
	writeFile(t, path, `
brokers: rp:9092
schema_registry: http://rp:8081
workloads:
  - name: w1
    topic: t
    direction: bogus
    schema: {file: s.proto, format: protobuf, message: M, subject: t-value}
    data: {source: pre_encoded, pool_size: 10}
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for bad direction")
	}
}

func TestLoadDefaultsClientsAndShard(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/c.yaml"
	writeFile(t, path, `
brokers: rp:9092
schema_registry: http://rp:8081
workloads:
  - name: w1
    topic: t
    direction: produce
    schema: {file: s.proto, format: protobuf, message: M, subject: t-value}
    data: {source: pre_encoded, pool_size: 10}
`)
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Shard.Count != 1 {
		t.Fatalf("shard count default = %d, want 1", c.Shard.Count)
	}
	if c.Workloads[0].Clients != 1 {
		t.Fatalf("clients default = %d, want 1", c.Workloads[0].Clients)
	}
}
