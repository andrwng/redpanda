// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package gen

import "sync/atomic"

// RecordSource is the interface the producer consumes to pull pre-encoded,
// pre-framed records without paying generation cost on the hot path.
type RecordSource interface {
	Next() []byte
}

// Pool holds a fixed set of framed records baked ahead of time and served
// round-robin. It is safe for concurrent use.
type Pool struct {
	records [][]byte
	idx     uint64
}

// NewPool bakes size framed records by calling next to produce a payload and
// frame to wrap it (e.g. with the Confluent SR header), up front.
func NewPool(next func() ([]byte, error), frame func([]byte) []byte, size int) (*Pool, error) {
	recs := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		payload, err := next()
		if err != nil {
			return nil, err
		}
		recs = append(recs, frame(payload))
	}
	return &Pool{records: recs}, nil
}

// Next returns the next record in round-robin order.
func (p *Pool) Next() []byte {
	n := atomic.AddUint64(&p.idx, 1) - 1
	return p.records[n%uint64(len(p.records))]
}
