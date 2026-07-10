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

func TestFreshGeneratesEachCall(t *testing.T) {
	i := 0
	next := func() ([]byte, error) { i++; return []byte{byte(i)}, nil }
	f := NewFresh(next, func(b []byte) []byte { return b })
	if string(f.Next()) == string(f.Next()) {
		t.Fatal("fresh should produce a new record each call")
	}
}
