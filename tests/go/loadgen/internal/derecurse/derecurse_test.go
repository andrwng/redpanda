// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package derecurse

import (
	"strings"
	"testing"
)

// backEdgesFromFile compiles path (whose top-level package is pkg) against
// its own directory and returns its back-edges, letting tests exercise
// BackEdges without wiring up protocompile by hand.
func backEdgesFromFile(t *testing.T, path, pkg string) []Cut {
	t.Helper()
	_ = pkg // package is recovered from the compiled descriptors, not reparsed here
	files, err := LoadFileSet(path, []string{"testdata"})
	if err != nil {
		t.Fatalf("compile %s: %v", path, err)
	}
	return BackEdges(files)
}

// contains reports whether out contains substr; a thin wrapper so tests
// read naturally without importing strings themselves.
func contains(out, substr string) bool {
	return strings.Contains(out, substr)
}

func TestBackEdgesFindsSelfReference(t *testing.T) {
	cuts := backEdgesFromFile(t, "testdata/recursive.proto", "demo")
	found := 0
	for _, c := range cuts {
		if c.Message == "demo.Node" && (c.Field == "next" || c.Field == "kids") {
			found++
		}
	}
	if found == 0 {
		t.Fatal("expected Node.next/kids flagged as recursive")
	}
}

func TestRewriteRemovesCutFields(t *testing.T) {
	src := "syntax=\"proto3\";\npackage demo;\nmessage Node { string v = 1; Node next = 2; }\n"
	out, err := Rewrite(src, []Cut{{Message: "demo.Node", Field: "next"}}, "demo")
	if err != nil {
		t.Fatal(err)
	}
	if contains(out, "next") {
		t.Fatalf("cut field still present:\n%s", out)
	}
}
