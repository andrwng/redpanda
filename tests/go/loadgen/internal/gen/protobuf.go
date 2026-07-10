// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package gen

import (
	"math/rand"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ProtoGen builds random, schema-conforming protobuf messages for a given
// descriptor. Generation is deterministic for a fixed seed: fields are
// visited in descriptor order (never map iteration) so the same seed always
// produces the same byte-for-byte record.
type ProtoGen struct {
	md       protoreflect.MessageDescriptor
	rnd      *rand.Rand
	maxDepth int
	maxList  int
}

// NewProtoGen returns a generator for md. maxDepth bounds recursion through
// nested messages and maxList bounds the length of repeated fields.
func NewProtoGen(md protoreflect.MessageDescriptor, seed int64, maxDepth, maxList int) *ProtoGen {
	return &ProtoGen{md: md, rnd: rand.New(rand.NewSource(seed)), maxDepth: maxDepth, maxList: maxList}
}

// Record returns a single marshaled, schema-conforming message (no framing).
//
// dynamicpb.Message stores fields in a map, so its wire field order is not
// guaranteed to follow descriptor order and varies across message instances.
// MarshalOptions.Deterministic forces a stable field order so that two
// generators seeded identically produce byte-identical output.
func (g *ProtoGen) Record() ([]byte, error) {
	msg := g.build(g.md, 0)
	opts := proto.MarshalOptions{Deterministic: true}
	return opts.Marshal(msg.Interface())
}

func (g *ProtoGen) build(md protoreflect.MessageDescriptor, depth int) *dynamicpb.Message {
	msg := dynamicpb.NewMessage(md)
	if depth >= g.maxDepth {
		return msg // leave leaf empty to bound depth
	}
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.IsMap() {
			continue // schemas in scope have no maps; skip defensively
		}
		if fd.IsList() {
			list := msg.NewField(fd).List()
			n := g.rnd.Intn(g.maxList + 1)
			for j := 0; j < n; j++ {
				list.Append(g.scalarOrMessage(fd, depth))
			}
			msg.Set(fd, protoreflect.ValueOfList(list))
			continue
		}
		msg.Set(fd, g.scalarOrMessage(fd, depth))
	}
	return msg
}

func (g *ProtoGen) scalarOrMessage(fd protoreflect.FieldDescriptor, depth int) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(g.rnd.Intn(2) == 1)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(g.rnd.Int31())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(g.rnd.Int63())
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(g.rnd.Uint32())
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(g.rnd.Uint64())
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(g.rnd.Float32())
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(g.rnd.Float64())
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(g.randString())
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte(g.randString()))
	case protoreflect.EnumKind:
		vals := fd.Enum().Values()
		return protoreflect.ValueOfEnum(vals.Get(g.rnd.Intn(vals.Len())).Number())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		sub := g.build(fd.Message(), depth+1)
		return protoreflect.ValueOfMessage(sub)
	default:
		return protoreflect.ValueOfString("")
	}
}

func (g *ProtoGen) randString() string {
	const alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	n := 4 + g.rnd.Intn(12)
	b := make([]byte, n)
	for i := range b {
		b[i] = alpha[g.rnd.Intn(len(alpha))]
	}
	return string(b)
}
