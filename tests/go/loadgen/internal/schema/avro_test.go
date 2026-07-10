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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegisterAvroReturnsID(t *testing.T) {
	const avroText = `{"type":"record","name":"M","fields":[]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var body any
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/subjects/t-value/versions":
			// franz-go's CreateSchema POSTs here to register the schema.
			body = map[string]any{"id": 42}
		case r.Method == http.MethodGet && r.URL.Path == "/schemas/ids/42/versions":
			// It then looks up which subjects/versions use id 42.
			body = []map[string]any{{"subject": "t-value", "version": 1}}
		case r.Method == http.MethodGet && r.URL.Path == "/subjects/t-value/versions/1":
			// Finally it fetches the full SubjectSchema for that version.
			body = map[string]any{
				"subject":    "t-value",
				"version":    1,
				"id":         42,
				"schema":     avroText,
				"schemaType": "AVRO",
			}
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Fatal(err)
		}
	}))
	defer srv.Close()

	id, err := RegisterAvro(context.Background(), srv.URL, "t-value", avroText)
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 {
		t.Fatalf("id = %d, want 42", id)
	}
}
