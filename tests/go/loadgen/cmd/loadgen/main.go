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

	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/config"
	"github.com/redpanda-data/redpanda/tests/go/loadgen/internal/orchestrator"
)

func main() {
	cfgPath := flag.String("config", "", "path to workloads YAML")
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
	// Extra positional args are proto import roots, consulted when
	// resolving each workload's schema file.
	importPaths := flag.Args()
	if err := orchestrator.Run(context.Background(), c, importPaths); err != nil {
		fmt.Fprintln(os.Stderr, "run error:", err)
		os.Exit(1)
	}
}
