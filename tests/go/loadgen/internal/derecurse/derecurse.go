// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package derecurse makes a protobuf schema Iceberg-compatible by removing
// recursive message fields.
//
// Redpanda's Iceberg translation rejects a protobuf message whose type
// full_name repeats along a root-to-node descent path (this also catches
// mutual recursion between two or more messages) and caps nesting depth at
// 100. BackEdges finds the minimal set of message-typed fields to delete to
// make every such path acyclic, Rewrite deletes those fields from the
// original .proto source text, and Verify confirms the result is
// Iceberg-ready (0 cycles, depth <= 100).
package derecurse

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Cut identifies a single message-typed field to delete: Field on Message
// (both dot-qualified full names, e.g. "demo.Node" / "next").
type Cut struct {
	Message string
	Field   string
}

// edge is a message-typed field, kept in declaration order.
type edge struct {
	field  string
	target string
}

// messageGraph walks files in declaration order (root file first, then its
// transitive imports as supplied by the caller) and returns, for every
// message found, its declaration order and its message-typed field edges.
// Only message and group kinds produce edges; everything else is a scalar
// leaf and is not part of the recursion graph.
func messageGraph(files []protoreflect.FileDescriptor) (order []string, edges map[string][]edge) {
	edges = make(map[string][]edge)
	seen := make(map[string]bool)

	var walk func(md protoreflect.MessageDescriptor)
	walk = func(md protoreflect.MessageDescriptor) {
		full := string(md.FullName())
		if seen[full] {
			return
		}
		seen[full] = true
		order = append(order, full)

		fields := md.Fields()
		var fl []edge
		for i := 0; i < fields.Len(); i++ {
			fd := fields.Get(i)
			if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
				fl = append(fl, edge{field: string(fd.Name()), target: string(fd.Message().FullName())})
			}
		}
		edges[full] = fl

		nested := md.Messages()
		for i := 0; i < nested.Len(); i++ {
			walk(nested.Get(i))
		}
	}

	for _, fd := range files {
		msgs := fd.Messages()
		for i := 0; i < msgs.Len(); i++ {
			walk(msgs.Get(i))
		}
	}
	return order, edges
}

// dfsColor is a DFS visitation state, matching the WHITE/GREY/BLACK coloring
// used by the reference implementation.
type dfsColor int

const (
	white dfsColor = iota
	grey
	black
)

// BackEdges finds every message-typed field whose target is currently on the
// DFS stack (a back-edge, i.e. a genuine recursive pointer, including
// mutual recursion between two or more messages) and returns it as a Cut.
// Messages are visited in declaration order, and each message's fields are
// visited in declaration order, so the result is deterministic. Forward and
// cross edges are left in place, so containment relationships survive.
func BackEdges(files []protoreflect.FileDescriptor) []Cut {
	order, edges := messageGraph(files)

	color := make(map[string]dfsColor, len(order))
	var cuts []Cut

	var dfs func(u string)
	dfs = func(u string) {
		color[u] = grey
		for _, e := range edges[u] {
			if _, ok := edges[e.target]; !ok {
				continue // target not declared in the supplied files: an external leaf, never a cycle
			}
			switch color[e.target] {
			case grey:
				cuts = append(cuts, Cut{Message: u, Field: e.field})
			case white:
				dfs(e.target)
			}
		}
		color[u] = black
	}
	for _, m := range order {
		if color[m] == white {
			dfs(m)
		}
	}
	return cuts
}

// Verify rebuilds the message graph from files and reports how many
// back-edges (cycles) remain and the longest message-nesting path. A schema
// is Iceberg-ready when cycles == 0 and maxDepth <= 100.
func Verify(files []protoreflect.FileDescriptor) (cycles int, maxDepth int) {
	order, edgesByMsg := messageGraph(files)

	adj := make(map[string][]string, len(order))
	for u, es := range edgesByMsg {
		for _, e := range es {
			if _, ok := edgesByMsg[e.target]; ok {
				adj[u] = append(adj[u], e.target)
			}
		}
	}

	color := make(map[string]dfsColor, len(order))
	var dfsCycles func(u string)
	dfsCycles = func(u string) {
		color[u] = grey
		for _, t := range adj[u] {
			switch color[t] {
			case grey:
				cycles++
			case white:
				dfsCycles(t)
			}
		}
		color[u] = black
	}
	for _, m := range order {
		if color[m] == white {
			dfsCycles(m)
		}
	}

	depthMemo := make(map[string]int, len(order))
	visiting := make(map[string]bool, len(order))
	var longest func(u string) int
	longest = func(u string) int {
		if d, ok := depthMemo[u]; ok {
			return d
		}
		if visiting[u] {
			// A cycle remains despite BackEdges/Verify's contract; break the
			// recursion defensively rather than stack-overflow.
			return 1
		}
		visiting[u] = true
		best := 1
		for _, t := range adj[u] {
			if d := 1 + longest(t); d > best {
				best = d
			}
		}
		visiting[u] = false
		depthMemo[u] = best
		return best
	}
	for _, m := range order {
		if d := longest(m); d > maxDepth {
			maxDepth = d
		}
	}
	return cycles, maxDepth
}

// declRe matches a trimmed "message Foo", "enum Foo", or "oneof Foo" header
// with the opening brace already stripped off.
var declRe = regexp.MustCompile(`^(message|enum|oneof)\s+(\w+)$`)

// stripLineComments removes every "// ..." line comment from s, preserving
// line breaks so a multi-line declaration split across real code lines still
// matches declRe afterwards. It is a plain textual scan, not a tokenizer, so
// a literal "//" inside a string (e.g. a URL in an option value) would be
// misread as a comment; that never occurs in the text directly preceding a
// message/enum/oneof declaration, which is all this is used for.
func stripLineComments(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		if idx := strings.Index(line, "//"); idx >= 0 {
			lines[i] = line[:idx]
		}
	}
	return strings.Join(lines, "\n")
}

// rewriteFrame is one level of the message/enum/oneof/other brace stack
// tracked while rewriting proto source text. Every '{' pushes a frame (kind
// is "other" for constructs derecurse doesn't care about, e.g. a
// message-literal option value) so that every '}' pops the matching one.
// "oneof" is transparent for cut-eligibility (see enclosingKind) but, like
// "other", never contributes to a message's full name.
type rewriteFrame struct {
	kind string // "message", "enum", "oneof", or "other"
	name string
}

// enclosingKind walks up stack from the top, skipping transparent "oneof"
// frames, and returns the kind of the nearest frame that actually scopes a
// field statement. A field is cut-eligible only when this is "message":
// oneof members belong to their enclosing message, but enum values must
// never be mistaken for message fields.
func enclosingKind(stack []rewriteFrame) string {
	for i := len(stack) - 1; i >= 0; i-- {
		if stack[i].kind != "oneof" {
			return stack[i].kind
		}
	}
	return ""
}

// Rewrite deletes every field statement named by cuts from protoText, a
// .proto source file whose top-level package is pkg. It scans protoText
// delimiter-by-delimiter ('{', '}', ';') rather than line-by-line, tracking
// a message/enum/oneof brace stack (mirroring the descent Redpanda's
// is_recursive_type performs over compiled descriptors) and drops a field
// statement only when its enclosing frame (a oneof is transparent and
// resolves to the message it is declared in) is a message and
// (fullMessageName, fieldName) is in cuts. Scanning by delimiter rather
// than by line keeps the result correct regardless of how the source is
// wrapped, including a whole message body written on a single line.
func Rewrite(protoText string, cuts []Cut, pkg string) (string, error) {
	dels := make(map[[2]string]bool, len(cuts))
	for _, c := range cuts {
		dels[[2]string{c.Message, c.Field}] = true
	}

	var stack []rewriteFrame
	curMsg := func() string {
		var names []string
		for _, f := range stack {
			if f.kind == "message" {
				names = append(names, f.name)
			}
		}
		if len(names) == 0 {
			return ""
		}
		return pkg + "." + strings.Join(names, ".")
	}

	var out, buf strings.Builder
	flush := func() {
		out.WriteString(buf.String())
		buf.Reset()
	}
	for _, r := range protoText {
		switch r {
		case '{':
			kind, name := "other", ""
			if m := declRe.FindStringSubmatch(strings.TrimSpace(stripLineComments(buf.String()))); m != nil {
				kind, name = m[1], m[2]
			}
			flush()
			out.WriteByte('{')
			stack = append(stack, rewriteFrame{kind: kind, name: name})
		case '}':
			flush()
			out.WriteByte('}')
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		case ';':
			trimmed := strings.TrimSpace(buf.String())
			cut := false
			if enclosingKind(stack) == "message" && strings.Contains(trimmed, "=") {
				lhs := strings.TrimSpace(strings.SplitN(trimmed, "=", 2)[0])
				if fields := strings.Fields(lhs); len(fields) > 0 {
					fname := fields[len(fields)-1]
					cut = dels[[2]string{curMsg(), fname}]
				}
			}
			if cut {
				buf.Reset() // drop the cut field statement (and its trailing ';') entirely
			} else {
				flush()
				out.WriteByte(';')
			}
		default:
			buf.WriteRune(r)
		}
	}
	flush() // trailing text after the last delimiter, e.g. a final newline
	if len(stack) > 0 {
		return "", fmt.Errorf("derecurse: unbalanced message/enum braces at EOF (%d open)", len(stack))
	}
	return out.String(), nil
}

// LoadFileSet compiles file, resolving imports against importPaths, and
// returns the compiled file together with every file it transitively
// imports, root first. This is the descriptor set BackEdges and Verify
// expect: it mirrors passing a full protoc FileDescriptorSet (declared
// messages plus imported ones) to the DFS.
func LoadFileSet(file string, importPaths []string) ([]protoreflect.FileDescriptor, error) {
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			ImportPaths: importPaths,
		}),
	}
	compileFile := file
	for _, importPath := range importPaths {
		if rel, err := filepath.Rel(importPath, file); err == nil && !strings.HasPrefix(rel, "..") {
			compileFile = rel
			break
		}
	}
	fds, err := compiler.Compile(context.Background(), compileFile)
	if err != nil {
		return nil, fmt.Errorf("compile %s: %w", file, err)
	}

	var out []protoreflect.FileDescriptor
	seen := make(map[string]bool)
	var add func(fd protoreflect.FileDescriptor)
	add = func(fd protoreflect.FileDescriptor) {
		if seen[fd.Path()] {
			return
		}
		seen[fd.Path()] = true
		out = append(out, fd)
		imports := fd.Imports()
		for i := 0; i < imports.Len(); i++ {
			add(imports.Get(i).FileDescriptor)
		}
	}
	for _, fd := range fds {
		add(fd)
	}
	return out, nil
}
