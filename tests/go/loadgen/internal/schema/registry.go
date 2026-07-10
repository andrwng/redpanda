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

// RegisterProtobuf registers protoText under subject in the schema registry
// at srURL, returning the globally unique schema ID assigned by the registry.
func RegisterProtobuf(ctx context.Context, srURL, subject, protoText string) (int, error) {
	cl, err := sr.NewClient(sr.URLs(srURL))
	if err != nil {
		return 0, err
	}
	ss, err := cl.CreateSchema(ctx, subject, sr.Schema{
		Schema: protoText,
		Type:   sr.TypeProtobuf,
	})
	if err != nil {
		return 0, err
	}
	return ss.ID, nil
}
