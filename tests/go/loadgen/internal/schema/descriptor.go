// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package schema

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// LoadProto compiles file, resolving imports against importPaths, and
// returns the descriptor for the fully-qualified rootMessage.
func LoadProto(file string, importPaths []string, rootMessage string) (protoreflect.MessageDescriptor, error) {
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			ImportPaths: importPaths,
		}),
	}
	// SourceResolver resolves the compiled file path relative to
	// ImportPaths, so file must be made relative to whichever import path
	// contains it before being handed to Compile.
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
	for _, fd := range fds {
		if md := findByFullName(fd, rootMessage); md != nil {
			return md, nil
		}
	}
	return nil, fmt.Errorf("message %q not found", rootMessage)
}

func findByFullName(fd protoreflect.FileDescriptor, full string) protoreflect.MessageDescriptor {
	var walk func(msgs protoreflect.MessageDescriptors) protoreflect.MessageDescriptor
	walk = func(msgs protoreflect.MessageDescriptors) protoreflect.MessageDescriptor {
		for i := 0; i < msgs.Len(); i++ {
			m := msgs.Get(i)
			if string(m.FullName()) == full {
				return m
			}
			if r := walk(m.Messages()); r != nil {
				return r
			}
		}
		return nil
	}
	return walk(fd.Messages())
}
