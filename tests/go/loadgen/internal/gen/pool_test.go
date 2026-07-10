// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package gen

import "testing"

func TestPoolBakesAndCycles(t *testing.T) {
	i := 0
	next := func() ([]byte, error) { i++; return []byte{byte(i)}, nil }
	frame := func(b []byte) []byte { return append([]byte{0x00}, b...) }
	p, err := NewPool(next, frame, 3)
	if err != nil {
		t.Fatal(err)
	}
	// framed, 3 distinct, then round-robins
	first := [][]byte{p.Next(), p.Next(), p.Next()}
	for _, r := range first {
		if r[0] != 0x00 {
			t.Fatalf("record not framed: %x", r)
		}
	}
	if string(p.Next()) != string(first[0]) {
		t.Fatal("pool should round-robin back to the first record")
	}
}
