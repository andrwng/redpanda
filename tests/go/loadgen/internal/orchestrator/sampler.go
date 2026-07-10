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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// startSampler starts a background goroutine that, once a second, reflects
// the delta in sentFn/bytesFn since the previous sample into recs/bytesVec
// labeled by name. recs and bytesVec are nil when metrics are disabled, in
// which case startSampler is a no-op: the returned stop channel and
// WaitGroup are still safe for the caller's close/Wait sequence below.
//
// Callers must call close(stop) once their workload's produce.Run or
// consume.Run call returns, then wg.Wait(), before treating the workload as
// finished. This guarantees a final flush runs after Run returns -
// including any counters incremented during produce.Run's post-loop
// cl.Flush(context.Background()), which is not tied to the workload's
// context and could otherwise race a sampler that stops on ctx.Done.
func startSampler(recs, bytesVec *prometheus.CounterVec, name string, sentFn, bytesFn func() int64) (stop chan struct{}, wg *sync.WaitGroup) {
	stop = make(chan struct{})
	wg = &sync.WaitGroup{}
	if recs == nil || bytesVec == nil {
		return stop, wg
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		sampleLoop(stop, recs, bytesVec, name, sentFn, bytesFn)
	}()
	return stop, wg
}

// sampleLoop runs until stop is closed, adding the delta in sentFn/bytesFn
// to recs/bytesVec every tick and once more on exit so no progress made
// between the last tick and stop is lost.
func sampleLoop(stop <-chan struct{}, recs, bytesVec *prometheus.CounterVec, name string, sentFn, bytesFn func() int64) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastSent, lastBytes int64
	flush := func() {
		if s := sentFn(); s > lastSent {
			if recs != nil {
				recs.WithLabelValues(name).Add(float64(s - lastSent))
			}
			lastSent = s
		}
		if b := bytesFn(); b > lastBytes {
			if bytesVec != nil {
				bytesVec.WithLabelValues(name).Add(float64(b - lastBytes))
			}
			lastBytes = b
		}
	}
	for {
		select {
		case <-ticker.C:
			flush()
		case <-stop:
			flush()
			return
		}
	}
}
