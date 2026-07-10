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
	"math"
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

	return sleep(ctx, d)
}

type unlimitedPacer struct{}

func (unlimitedPacer) Wait(ctx context.Context) error {
	return ctx.Err()
}

// oscillatingPacer varies the target rate between min and max over period,
// following shape ("sine" or "square"), driven by an injectable clock so
// tests can control phase deterministically.
type oscillatingPacer struct {
	min, max int
	period   time.Duration
	shape    string
	now      func() time.Time

	mu   sync.Mutex
	next time.Time
}

// NewOscillatingPacer returns a Pacer whose target rate oscillates between
// min and max over period. shape selects the waveform: "square" alternates
// between max (first half of the period) and min (second half); anything
// else (including "sine") follows a sine wave between min and max. now, if
// nil, defaults to time.Now.
func NewOscillatingPacer(min, max int, period time.Duration, shape string, now func() time.Time) Pacer {
	if now == nil {
		now = time.Now
	}
	return &oscillatingPacer{min: min, max: max, period: period, shape: shape, now: now}
}

func (p *oscillatingPacer) rateAt(t time.Time) int {
	phase := math.Mod(t.Sub(time.Unix(0, 0)).Seconds(), p.period.Seconds()) / p.period.Seconds()
	switch p.shape {
	case "square":
		if phase < 0.5 {
			return p.max
		}
		return p.min
	default: // sine
		frac := (math.Sin(2*math.Pi*phase) + 1) / 2
		return p.min + int(float64(p.max-p.min)*frac)
	}
}

func (p *oscillatingPacer) Wait(ctx context.Context) error {
	rate := p.rateAt(p.now())
	if rate <= 0 {
		return sleep(ctx, 10*time.Millisecond)
	}
	interval := time.Second / time.Duration(rate)

	p.mu.Lock()
	now := p.now()
	if p.next.IsZero() {
		p.next = now
	}
	p.next = p.next.Add(interval)
	d := time.Until(p.next)
	p.mu.Unlock()

	return sleep(ctx, d)
}

// sleep blocks for d, or until ctx is done, whichever comes first. d <= 0
// returns immediately with ctx's error.
func sleep(ctx context.Context, d time.Duration) error {
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
