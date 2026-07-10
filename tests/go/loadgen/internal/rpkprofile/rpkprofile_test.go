// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package rpkprofile

import (
	"strings"
	"testing"
)

// sampleYAML mirrors `rpk profile print` output for a Redpanda Cloud
// profile: brokers, a present-but-empty tls (TLS enabled with system root
// CAs, no client cert), and SCRAM-SHA-256 SASL credentials.
const sampleYAML = `
kafka_api:
    brokers:
        - seed.example.cloud:9092
    tls: {}
    sasl:
        user: loadtester
        password: secret
        mechanism: SCRAM-SHA-256
schema_registry: {}
`

func TestParseSample(t *testing.T) {
	p, err := Parse([]byte(sampleYAML))
	if err != nil {
		t.Fatal(err)
	}
	if got := p.KafkaAPI.Brokers; len(got) != 1 || got[0] != "seed.example.cloud:9092" {
		t.Fatalf("brokers = %v, want [seed.example.cloud:9092]", got)
	}
	if p.KafkaAPI.TLS == nil {
		t.Fatal("expected present-but-empty tls to parse as non-nil *TLS")
	}
	if p.KafkaAPI.SASL == nil {
		t.Fatal("expected sasl to be non-nil")
	}
	if p.KafkaAPI.SASL.Mechanism != "SCRAM-SHA-256" {
		t.Fatalf("sasl.mechanism = %q, want SCRAM-SHA-256", p.KafkaAPI.SASL.Mechanism)
	}
	if p.KafkaAPI.SASL.User != "loadtester" || p.KafkaAPI.SASL.Password != "secret" {
		t.Fatalf("sasl user/password = %q/%q, want loadtester/secret", p.KafkaAPI.SASL.User, p.KafkaAPI.SASL.Password)
	}
}

func TestParseAbsentTLSIsNil(t *testing.T) {
	p, err := Parse([]byte("kafka_api:\n    brokers: [seed:9092]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if p.KafkaAPI.TLS != nil {
		t.Fatalf("expected absent tls key to parse as nil *TLS, got %+v", p.KafkaAPI.TLS)
	}
	if p.KafkaAPI.SASL != nil {
		t.Fatalf("expected absent sasl key to parse as nil *SASL, got %+v", p.KafkaAPI.SASL)
	}
}

func TestMechanism(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		{"SCRAM-SHA-256", "SCRAM-SHA-256"},
		{"SCRAM-SHA-512", "SCRAM-SHA-512"},
		{"PLAIN", "PLAIN"},
		{"scram-sha-256", "SCRAM-SHA-256"},
	}
	for _, tc := range cases {
		m, err := mechanism(&SASL{User: "u", Password: "p", Mechanism: tc.name})
		if err != nil {
			t.Fatalf("mechanism(%q): %v", tc.name, err)
		}
		if m.Name() != tc.want {
			t.Fatalf("mechanism(%q).Name() = %q, want %q", tc.name, m.Name(), tc.want)
		}
	}
}

func TestMechanismRejectsUnknown(t *testing.T) {
	if _, err := mechanism(&SASL{Mechanism: "bogus"}); err == nil {
		t.Fatal("expected error for unknown mechanism")
	}
}

func TestKgoOptsNonEmpty(t *testing.T) {
	p, err := Parse([]byte(sampleYAML))
	if err != nil {
		t.Fatal(err)
	}
	opts, err := p.KgoOpts()
	if err != nil {
		t.Fatal(err)
	}
	if len(opts) == 0 {
		t.Fatal("expected non-empty kgo opts")
	}
}

func TestBuildTLSConfig(t *testing.T) {
	cfg, err := buildTLSConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil {
		t.Fatal("buildTLSConfig(nil) returned nil *tls.Config")
	}

	cfg, err = buildTLSConfig(&TLS{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil {
		t.Fatal("buildTLSConfig(&TLS{}) returned nil *tls.Config")
	}
}

func TestSROptsIncludesBasicAuthAndTLS(t *testing.T) {
	p, err := Parse([]byte(sampleYAML))
	if err != nil {
		t.Fatal(err)
	}
	opts, err := p.SROpts("https://sr:30081")
	if err != nil {
		t.Fatal(err)
	}
	// sr.ClientOpt values aren't directly inspectable, but we expect at
	// least URLs + basic auth + TLS since the sample profile has both SASL
	// and (present-but-empty) TLS set.
	if len(opts) < 3 {
		t.Fatalf("SROpts with sasl+tls = %d opts, want >= 3 (urls, basic auth, tls)", len(opts))
	}
}

func TestSROptsWithoutSASLOrTLS(t *testing.T) {
	p, err := Parse([]byte("kafka_api:\n    brokers: [seed:9092]\n"))
	if err != nil {
		t.Fatal(err)
	}
	opts, err := p.SROpts("http://sr:8081")
	if err != nil {
		t.Fatal(err)
	}
	if len(opts) != 1 {
		t.Fatalf("SROpts without sasl/tls over http = %d opts, want 1 (just urls)", len(opts))
	}
}

func TestLoadReportsMissingBinary(t *testing.T) {
	// Force a lookup failure by shadowing PATH; exec.Command still resolves
	// "rpk" by name, so an empty PATH guarantees exec.ErrNotFound.
	t.Setenv("PATH", "")
	_, err := Load("")
	if err == nil {
		t.Fatal("expected error when rpk is not in PATH")
	}
	if !strings.Contains(err.Error(), "rpk") {
		t.Fatalf("error %q does not mention rpk", err.Error())
	}
}
