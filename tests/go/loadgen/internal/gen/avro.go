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
	"math/rand"
	"sync"

	"github.com/hamba/avro/v2"
)

// AvroGen builds random, schema-conforming Avro values for a given schema.
// Generation is deterministic for a fixed seed.
//
// Record is safe for concurrent use: the fresh data-source path (see
// gen.NewFresh) drives a single AvroGen from every producer goroutine, but
// math/rand.Rand is not concurrency-safe, so mu serializes access to rnd.
type AvroGen struct {
	schema  avro.Schema
	mu      sync.Mutex
	rnd     *rand.Rand
	maxList int
}

// NewAvroGen returns a generator for schemaText. maxList bounds the length
// of array fields.
func NewAvroGen(schemaText string, seed int64, maxList int) (*AvroGen, error) {
	s, err := avro.Parse(schemaText)
	if err != nil {
		return nil, err
	}
	return &AvroGen{schema: s, rnd: rand.New(rand.NewSource(seed)), maxList: maxList}, nil
}

// Record returns a single marshaled, schema-conforming value (binary Avro,
// no framing).
func (g *AvroGen) Record() ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	v := g.value(g.schema)
	return avro.Marshal(g.schema, v)
}

func (g *AvroGen) value(s avro.Schema) any {
	switch s.Type() {
	case avro.Record:
		rec := s.(*avro.RecordSchema)
		m := make(map[string]any, len(rec.Fields()))
		for _, f := range rec.Fields() {
			m[f.Name()] = g.value(f.Type())
		}
		return m
	case avro.String:
		return g.randStr()
	case avro.Int:
		return g.rnd.Int31()
	case avro.Long:
		return g.rnd.Int63()
	case avro.Float:
		return g.rnd.Float32()
	case avro.Double:
		return g.rnd.Float64()
	case avro.Boolean:
		return g.rnd.Intn(2) == 1
	case avro.Array:
		arr := s.(*avro.ArraySchema)
		n := g.rnd.Intn(g.maxList + 1)
		out := make([]any, n)
		for i := range out {
			out[i] = g.value(arr.Items())
		}
		return out
	case avro.Union:
		// hamba/avro encodes a non-null union value as a single-entry map
		// keyed by the branch's schema name; choose the first non-null
		// branch for determinism.
		for _, b := range s.(*avro.UnionSchema).Types() {
			if b.Type() != avro.Null {
				return map[string]any{unionKey(b): g.value(b)}
			}
		}
		return nil
	default:
		return nil
	}
}

// unionKey returns the name hamba/avro uses to identify a union branch: the
// full name for named schemas (record, enum, fixed) and the bare type name
// otherwise.
func unionKey(s avro.Schema) string {
	if n, ok := s.(avro.NamedSchema); ok {
		return n.FullName()
	}
	return string(s.Type())
}

func (g *AvroGen) randStr() string {
	const alpha = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 4+g.rnd.Intn(12))
	for i := range b {
		b[i] = alpha[g.rnd.Intn(len(alpha))]
	}
	return string(b)
}
