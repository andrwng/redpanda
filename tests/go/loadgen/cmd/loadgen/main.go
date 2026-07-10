// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/config"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/derecurse"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/metrics"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/orchestrator"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "derecurse" {
		if err := runDerecurse(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "derecurse error:", err)
			os.Exit(1)
		}
		return
	}

	cfgPath := flag.String("config", "", "path to workloads YAML")
	shardCount := flag.Int("shard.count", 0, "number of load-generator hosts")
	shardIndex := flag.Int("shard.index", -1, "this host's index in [0,count)")
	flag.Parse()
	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "loadgen --config <file>")
		os.Exit(2)
	}
	c, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(1)
	}
	if *shardCount > 0 {
		c.Shard.Count = *shardCount
	}
	if *shardIndex >= 0 {
		c.Shard.Index = *shardIndex
	}
	if err := c.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(1)
	}
	var recs, bytesVec *prometheus.CounterVec
	if c.MetricsAddr != "" {
		recs, bytesVec = metrics.Serve(c.MetricsAddr)
	}
	// Extra positional args are proto import roots, consulted when
	// resolving each workload's schema file.
	importPaths := flag.Args()
	if err := orchestrator.Run(context.Background(), c, importPaths, recs, bytesVec); err != nil {
		fmt.Fprintln(os.Stderr, "run error:", err)
		os.Exit(1)
	}
}

// repeatableFlag collects every occurrence of a flag that may be passed more
// than once, e.g. multiple --imports roots.
type repeatableFlag []string

func (r *repeatableFlag) String() string { return fmt.Sprint([]string(*r)) }
func (r *repeatableFlag) Set(v string) error {
	*r = append(*r, v)
	return nil
}

// runDerecurse implements: loadgen derecurse --in <proto> --imports <root>...
// --out <proto>. It rewrites the schema at --in to remove message fields
// that make it recursive per Redpanda's Iceberg translation rules (a
// repeated full_name on a root-to-node descent path, or nesting deeper than
// 100), then reports how many fields were cut and whether the rewritten
// schema verifies as Iceberg-ready.
func runDerecurse(args []string) error {
	fs := flag.NewFlagSet("derecurse", flag.ExitOnError)
	in := fs.String("in", "", "input .proto file to make Iceberg-compatible")
	out := fs.String("out", "", "output .proto file to write")
	var imports repeatableFlag
	fs.Var(&imports, "imports", "proto import root, repeatable")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *in == "" || *out == "" {
		return fmt.Errorf("usage: loadgen derecurse --in <proto> --imports <root>... --out <proto>")
	}

	importPaths := []string(imports)
	files, err := derecurse.LoadFileSet(*in, importPaths)
	if err != nil {
		return fmt.Errorf("compile %s: %w", *in, err)
	}
	cuts := derecurse.BackEdges(files)

	src, err := os.ReadFile(*in)
	if err != nil {
		return fmt.Errorf("read %s: %w", *in, err)
	}
	pkg := string(files[0].Package())
	rewritten, err := derecurse.Rewrite(string(src), cuts, pkg)
	if err != nil {
		return fmt.Errorf("rewrite %s: %w", *in, err)
	}
	if err := os.WriteFile(*out, []byte(rewritten), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", *out, err)
	}

	// Recompile the rewritten file to verify it's now Iceberg-ready. The
	// output may live outside every --imports root, so add its own
	// directory as an extra import root for this recompile.
	verifyImports := append(append([]string{}, importPaths...), filepath.Dir(*out))
	verifyFiles, err := derecurse.LoadFileSet(*out, verifyImports)
	if err != nil {
		return fmt.Errorf("recompile %s: %w", *out, err)
	}
	cycles, maxDepth := derecurse.Verify(verifyFiles)

	fmt.Printf("%d fields cut, %d cycles, max depth %d\n", len(cuts), cycles, maxDepth)
	if cycles > 0 || maxDepth > 100 {
		return fmt.Errorf("schema is not Iceberg-ready: %d cycles, max depth %d (cap 100)", cycles, maxDepth)
	}
	return nil
}
