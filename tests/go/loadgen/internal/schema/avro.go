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

	"github.com/twmb/franz-go/pkg/sr"
)

// RegisterAvro registers schemaText under subject in the schema registry at
// srURL, returning the globally unique schema ID assigned by the registry.
// srOpts is appended after sr.URLs(srURL), letting callers layer on
// auth/TLS (e.g. from an rpk profile via rpkprofile.Profile.SROpts).
func RegisterAvro(ctx context.Context, srURL, subject, schemaText string, srOpts ...sr.ClientOpt) (int, error) {
	opts := append([]sr.ClientOpt{sr.URLs(srURL)}, srOpts...)
	cl, err := sr.NewClient(opts...)
	if err != nil {
		return 0, err
	}
	ss, err := cl.CreateSchema(ctx, subject, sr.Schema{
		Schema: schemaText,
		Type:   sr.TypeAvro,
	})
	if err != nil {
		return 0, err
	}
	return ss.ID, nil
}
