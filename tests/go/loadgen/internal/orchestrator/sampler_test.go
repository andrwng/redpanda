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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestStartSamplerNilGuardsEitherCounterVec exercises startSampler with only
// one of recs/bytesVec nil. Both must be non-nil for sampling to run: a
// caller that only nil-checked recs before calling bytesVec.WithLabelValues
// would nil-deref, so startSampler must be a no-op unless both are set.
func TestStartSamplerNilGuardsEitherCounterVec(t *testing.T) {
	recs := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_records_total"}, []string{"workload"})
	bytesVec := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_bytes_total"}, []string{"workload"})

	sent := int64(5)
	nbytes := int64(50)
	sentFn := func() int64 { return sent }
	bytesFn := func() int64 { return nbytes }

	cases := []struct {
		name     string
		recs     *prometheus.CounterVec
		bytesVec *prometheus.CounterVec
	}{
		{"nil recs", nil, bytesVec},
		{"nil bytesVec", recs, nil},
		{"both nil", nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stop, wg := startSampler(tc.recs, tc.bytesVec, "w", sentFn, bytesFn)
			close(stop)
			wg.Wait()

			if got := testutil.ToFloat64(recs.WithLabelValues("w")); got != 0 {
				t.Fatalf("recs counter = %v, want 0 (sampler must not run with a nil bytesVec)", got)
			}
			if got := testutil.ToFloat64(bytesVec.WithLabelValues("w")); got != 0 {
				t.Fatalf("bytesVec counter = %v, want 0 (sampler must not run with a nil recs)", got)
			}
		})
	}
}
