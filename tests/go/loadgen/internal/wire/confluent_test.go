// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package wire

import (
	"bytes"
	"testing"
)

func TestFrameProtobufFirstMessage(t *testing.T) {
	// magic 0x00 | id=7 big-endian | msg-index [0x00] (root is first msg) | payload
	got := FrameProtobuf(7, []int{0}, []byte{0xAA, 0xBB})
	want := []byte{0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0xAA, 0xBB}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %x, want %x", got, want)
	}
}

func TestFrameAvro(t *testing.T) {
	got := FrameAvro(513, []byte{0x01})
	want := []byte{0x00, 0x00, 0x00, 0x02, 0x01, 0x01}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %x, want %x", got, want)
	}
}
