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
	"sync"
	"time"
)

// Pacer blocks callers so that record production tracks a target rate.
type Pacer interface {
	// Wait blocks until the next record may be sent, or ctx is done.
	Wait(ctx context.Context) error
}

// steadyPacer schedules ticks on a fixed interval, evenly spacing calls
// across concurrent callers rather than admitting them in bursts.
type steadyPacer struct {
	interval time.Duration

	mu   sync.Mutex
	next time.Time
}

// NewSteadyPacer returns a Pacer that admits at most rate calls per second,
// evenly spaced. A rate <= 0 returns a Pacer that never blocks.
func NewSteadyPacer(rate int) Pacer {
	if rate <= 0 {
		return unlimitedPacer{}
	}
	return &steadyPacer{interval: time.Second / time.Duration(rate)}
}

func (p *steadyPacer) Wait(ctx context.Context) error {
	p.mu.Lock()
	now := time.Now()
	if p.next.IsZero() {
		p.next = now
	}
	p.next = p.next.Add(p.interval)
	d := time.Until(p.next)
	p.mu.Unlock()

	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type unlimitedPacer struct{}

func (unlimitedPacer) Wait(ctx context.Context) error {
	return ctx.Err()
}
