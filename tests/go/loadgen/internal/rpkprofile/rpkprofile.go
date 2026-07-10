// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package rpkprofile resolves broker addresses and Kafka SASL/TLS
// credentials from an rpk profile (as printed by `rpk profile print`) into
// franz-go client options, so loadgen can connect to a cluster using the
// same profile rpk itself uses instead of duplicating brokers/credentials in
// the workload YAML.
package rpkprofile

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/pkg/sr"
	"gopkg.in/yaml.v3"
)

// Profile is the subset of `rpk profile print`'s YAML output loadgen needs
// to connect to a cluster: the Kafka API's brokers, TLS, and SASL settings.
type Profile struct {
	KafkaAPI KafkaAPI `yaml:"kafka_api"`
}

// KafkaAPI holds the Kafka listener settings from an rpk profile. TLS and
// SASL are pointers because rpk profile YAML uses their presence, not their
// contents, to signal whether TLS/SASL is enabled: an empty `tls: {}` still
// means TLS is on (with system root CAs), while an absent key means no TLS.
type KafkaAPI struct {
	Brokers []string `yaml:"brokers"`
	TLS     *TLS     `yaml:"tls"`
	SASL    *SASL    `yaml:"sasl"`
}

// TLS holds the optional client certificate and CA settings for the Kafka
// API's TLS config. TruststoreFile is treated as an alias for CAFile.
type TLS struct {
	CAFile         string `yaml:"ca_file"`
	CertFile       string `yaml:"cert_file"`
	KeyFile        string `yaml:"key_file"`
	TruststoreFile string `yaml:"truststore_file"`
}

// SASL holds the Kafka API's SASL credentials and mechanism, e.g.
// SCRAM-SHA-256, SCRAM-SHA-512, or PLAIN.
type SASL struct {
	User      string `yaml:"user"`
	Password  string `yaml:"password"`
	Mechanism string `yaml:"mechanism"`
}

// Parse decodes the YAML output of `rpk profile print` into a Profile.
func Parse(data []byte) (*Profile, error) {
	var p Profile
	if err := yaml.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("parse rpk profile: %w", err)
	}
	return &p, nil
}

// Load runs `rpk profile print` (or `rpk profile print <name>` when name is
// non-empty) and parses its output.
func Load(name string) (*Profile, error) {
	args := []string{"profile", "print"}
	if name != "" {
		args = append(args, name)
	}
	cmd := exec.Command("rpk", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return nil, fmt.Errorf("rpk not found in PATH: %w", err)
		}
		return nil, fmt.Errorf("rpk profile print failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return Parse(stdout.Bytes())
}

// KgoOpts translates p's Kafka API settings into franz-go client options:
// the seed brokers, plus TLS and/or SASL when the profile enables them.
func (p *Profile) KgoOpts() ([]kgo.Opt, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(p.KafkaAPI.Brokers...)}
	if p.KafkaAPI.TLS != nil {
		cfg, err := buildTLSConfig(p.KafkaAPI.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	}
	if p.KafkaAPI.SASL != nil {
		mech, err := mechanism(p.KafkaAPI.SASL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mech))
	}
	return opts, nil
}

// SROpts translates p's Kafka API SASL/TLS settings into Schema Registry
// client options for srURL. The registry's URL is never present in an rpk
// profile for Redpanda Cloud, so callers must supply it separately; when
// the profile has SASL credentials, Redpanda Cloud's Schema Registry
// expects the same user/password as HTTP basic auth over TLS.
func (p *Profile) SROpts(srURL string) ([]sr.ClientOpt, error) {
	opts := []sr.ClientOpt{sr.URLs(srURL)}
	if p.KafkaAPI.SASL != nil {
		opts = append(opts, sr.BasicAuth(p.KafkaAPI.SASL.User, p.KafkaAPI.SASL.Password))
	}
	if strings.HasPrefix(srURL, "https://") || p.KafkaAPI.TLS != nil {
		cfg, err := buildTLSConfig(p.KafkaAPI.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sr.DialTLSConfig(cfg))
	}
	return opts, nil
}

// buildTLSConfig builds a *tls.Config from t: a nil t (or one with no CA or
// client cert fields set) yields an empty config that falls back to system
// root CAs, matching rpk's `tls: {}` meaning "TLS enabled, no overrides".
func buildTLSConfig(t *TLS) (*tls.Config, error) {
	cfg := &tls.Config{}
	if t == nil {
		return cfg, nil
	}
	caFile := t.CAFile
	if caFile == "" {
		caFile = t.TruststoreFile
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read ca file %q: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no valid certificates found in ca file %q", caFile)
		}
		cfg.RootCAs = pool
	}
	if t.CertFile != "" && t.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// mechanism maps an rpk profile's SASL mechanism name to a franz-go SASL
// mechanism.
func mechanism(s *SASL) (sasl.Mechanism, error) {
	switch strings.ToUpper(s.Mechanism) {
	case "SCRAM-SHA-256":
		return scram.Auth{User: s.User, Pass: s.Password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: s.User, Pass: s.Password}.AsSha512Mechanism(), nil
	case "PLAIN":
		return plain.Auth{User: s.User, Pass: s.Password}.AsMechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported sasl mechanism %q", s.Mechanism)
	}
}
