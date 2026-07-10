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
