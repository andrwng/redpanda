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
	"testing"

	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/schema"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func rootDesc(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	md, err := schema.LoadProto("../schema/testdata/simple.proto", []string{"../schema/testdata"}, "demo.Root")
	if err != nil {
		t.Fatal(err)
	}
	return md
}

func TestProtoGenProducesDecodableRecord(t *testing.T) {
	md := rootDesc(t)
	g := NewProtoGen(md, 1, 5, 3)
	b, err := g.Record()
	if err != nil {
		t.Fatal(err)
	}
	msg := dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(b, msg); err != nil {
		t.Fatalf("record does not decode against schema: %v", err)
	}
}

func TestProtoGenDeterministicWithSeed(t *testing.T) {
	md := rootDesc(t)
	a, _ := NewProtoGen(md, 99, 5, 3).Record()
	b, _ := NewProtoGen(md, 99, 5, 3).Record()
	if string(a) != string(b) {
		t.Fatal("same seed should yield identical records")
	}
}
