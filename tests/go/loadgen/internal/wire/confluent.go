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
	"encoding/binary"
)

// FrameProtobuf prepends the Confluent SR header to an encoded protobuf payload.
// Layout: 0x00 | uint32(schemaID) big-endian | message-index | payload.
// The message-index is a zig-zag varint count followed by each index; the common
// case of the first message in the file is a single 0x00 byte.
func FrameProtobuf(schemaID int, msgIndex []int, payload []byte) []byte {
	out := make([]byte, 0, 5+2*len(msgIndex)+1+len(payload))
	out = append(out, 0x00)
	out = binary.BigEndian.AppendUint32(out, uint32(schemaID))
	if len(msgIndex) == 1 && msgIndex[0] == 0 {
		out = append(out, 0x00) // shorthand for [0]
	} else {
		out = appendVarint(out, int64(len(msgIndex)))
		for _, idx := range msgIndex {
			out = appendVarint(out, int64(idx))
		}
	}
	return append(out, payload...)
}

// FrameAvro prepends the Confluent SR header to an encoded Avro payload.
func FrameAvro(schemaID int, payload []byte) []byte {
	out := make([]byte, 0, 5+len(payload))
	out = append(out, 0x00)
	out = binary.BigEndian.AppendUint32(out, uint32(schemaID))
	return append(out, payload...)
}

func appendVarint(b []byte, v int64) []byte {
	// zig-zag then base-128 varint (Confluent uses signed varints here)
	uv := uint64((v << 1) ^ (v >> 63))
	for uv >= 0x80 {
		b = append(b, byte(uv)|0x80)
		uv >>= 7
	}
	return append(b, byte(uv))
}
