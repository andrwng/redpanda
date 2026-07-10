// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package metrics

import (
	"math"
	"testing"
	"time"
)

func TestThroughput(t *testing.T) {
	r, mb := Throughput(Sample{Records: 1000, Bytes: 1 << 20, Window: time.Second})
	if r != 1000 {
		t.Fatalf("recs/s = %v, want 1000", r)
	}
	if math.Abs(mb-1.0) > 0.001 {
		t.Fatalf("MB/s = %v, want ~1.0", mb)
	}
}
