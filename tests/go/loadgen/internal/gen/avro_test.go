// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package gen

import (
	"sync"
	"testing"

	"github.com/hamba/avro/v2"
)

func TestAvroGenDecodable(t *testing.T) {
	s := `{"type":"record","name":"R","fields":[{"name":"id","type":"string"},{"name":"n","type":"long"}]}`
	g, err := NewAvroGen(s, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Record()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := avro.Unmarshal(avro.MustParse(s), b, &m); err != nil {
		t.Fatalf("record does not decode: %v", err)
	}
}

// TestAvroGenConcurrentRecord mirrors how the fresh data-source path drives
// a single *AvroGen from every producer goroutine (see gen.NewFresh and
// orchestrator/run.go). Record must be safe to call concurrently.
func TestAvroGenConcurrentRecord(t *testing.T) {
	s := `{"type":"record","name":"R","fields":[{"name":"id","type":"string"},{"name":"n","type":"long"}]}`
	g, err := NewAvroGen(s, 7, 3)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := g.Record(); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}
