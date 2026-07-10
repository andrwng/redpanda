// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package schema

import "testing"

func TestLoadProtoResolvesRoot(t *testing.T) {
	md, err := LoadProto("testdata/simple.proto", []string{"testdata"}, "demo.Root")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(md.FullName()); got != "demo.Root" {
		t.Fatalf("full name = %q, want demo.Root", got)
	}
	if md.Fields().Len() != 3 {
		t.Fatalf("field count = %d, want 3", md.Fields().Len())
	}
}

func TestLoadProtoUnknownMessage(t *testing.T) {
	_, err := LoadProto("testdata/simple.proto", []string{"testdata"}, "demo.Nope")
	if err == nil {
		t.Fatal("expected error for unknown message")
	}
}
