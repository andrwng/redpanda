// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package produce

import (
	"context"
	"testing"
	"time"
)

func TestSteadyPacerUnthrottledNeverBlocks(t *testing.T) {
	p := NewSteadyPacer(0)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	for i := 0; i < 1000; i++ {
		if err := p.Wait(ctx); err != nil {
			t.Fatalf("unthrottled pacer blocked: %v", err)
		}
	}
}

func TestSteadyPacerLimitsRate(t *testing.T) {
	p := NewSteadyPacer(100) // 100/s => ~10ms spacing
	start := time.Now()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_ = p.Wait(ctx)
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("5 tokens at 100/s should take >=~40ms, took %v", elapsed)
	}
}

func TestOscillatingRateAtPhases(t *testing.T) {
	base := time.Unix(0, 0)
	clock := base
	p := NewOscillatingPacer(0, 100, time.Second, "square", func() time.Time { return clock })
	op := p.(*oscillatingPacer)
	clock = base // first half => max
	if r := op.rateAt(clock); r != 100 {
		t.Fatalf("square first-half rate = %d, want 100", r)
	}
	clock = base.Add(600 * time.Millisecond) // second half => min
	if r := op.rateAt(clock); r != 0 {
		t.Fatalf("square second-half rate = %d, want 0", r)
	}
}
