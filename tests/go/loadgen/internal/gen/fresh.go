// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package gen

// Fresh generates and frames a new record on every call to Next, trading the
// pre-baking cost Pool pays up front for a distinct record each time.
type Fresh struct {
	next  func() ([]byte, error)
	frame func([]byte) []byte
}

// NewFresh builds a Fresh source that calls next to produce a payload and
// frame to wrap it (e.g. with the Confluent SR header) on every Next call.
func NewFresh(next func() ([]byte, error), frame func([]byte) []byte) *Fresh {
	return &Fresh{next: next, frame: frame}
}

// Next generates and frames a new record. It returns nil if next fails.
func (f *Fresh) Next() []byte {
	b, err := f.next()
	if err != nil {
		return nil
	}
	return f.frame(b)
}
