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
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Sample struct {
	Records int64
	Bytes   int64
	Window  time.Duration
}

func Throughput(s Sample) (recsPerSec, mbPerSec float64) {
	secs := s.Window.Seconds()
	if secs <= 0 {
		return 0, 0
	}
	return float64(s.Records) / secs, float64(s.Bytes) / secs / (1 << 20)
}

func Report(w io.Writer, name string, s Sample) {
	r, mb := Throughput(s)
	fmt.Fprintf(w, "[%s] %.0f rec/s  %.1f MB/s  (%d records in %s)\n",
		name, r, mb, s.Records, s.Window)
}

var serveAddr string

// ServeAddr returns the address Serve bound its listener to, once Serve has
// been called. It exists so callers that pass an ephemeral port (":0" or
// "127.0.0.1:0") can discover the actual port in use.
func ServeAddr() string { return serveAddr }

// Serve starts an HTTP server exposing Prometheus counters for records and
// bytes processed, each labeled by workload name, on a /metrics endpoint at
// addr. It returns the two counter vectors so callers can increment them as
// workloads make progress.
func Serve(addr string) (recs, bytes *prometheus.CounterVec) {
	recs = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "loadgen_records_total"}, []string{"workload"})
	bytes = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "loadgen_bytes_total"}, []string{"workload"})
	reg := prometheus.NewRegistry()
	reg.MustRegister(recs, bytes)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(fmt.Sprintf("metrics: failed to listen on %q: %v", addr, err))
	}
	serveAddr = ln.Addr().String()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	go func() {
		_ = http.Serve(ln, mux)
	}()
	return recs, bytes
}
