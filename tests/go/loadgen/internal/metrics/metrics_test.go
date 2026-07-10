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
	"io"
	"math"
	"net/http"
	"strings"
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

func TestServeExposesCounters(t *testing.T) {
	recs, _ := Serve("127.0.0.1:0") // returns addr via package var for test
	recs.WithLabelValues("w1").Add(5)
	time.Sleep(20 * time.Millisecond)
	resp, err := http.Get("http://" + ServeAddr() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := new(strings.Builder)
	_, _ = io.Copy(buf, resp.Body)
	if !strings.Contains(buf.String(), "loadgen_records_total") {
		t.Fatal("expected loadgen_records_total metric")
	}
}
