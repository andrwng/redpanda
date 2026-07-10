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
	"time"
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
