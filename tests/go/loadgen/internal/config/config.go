// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Brokers        string     `yaml:"brokers"`
	SchemaRegistry string     `yaml:"schema_registry"`
	MetricsAddr    string     `yaml:"metrics_addr"`
	Shard          Shard      `yaml:"shard"`
	Workloads      []Workload `yaml:"workloads"`
}

type Shard struct {
	Count int `yaml:"count"`
	Index int `yaml:"index"`
}

type Workload struct {
	Name       string        `yaml:"name"`
	Schema     Schema        `yaml:"schema"`
	Topic      string        `yaml:"topic"`
	Direction  string        `yaml:"direction"`
	Data       Data          `yaml:"data"`
	Throughput Throughput    `yaml:"throughput"`
	Group      string        `yaml:"group"`
	ConsumeLag time.Duration `yaml:"consume_lag"`
	Clients    int           `yaml:"clients"`
}

type Schema struct {
	File    string `yaml:"file"`
	Format  string `yaml:"format"`
	Message string `yaml:"message"`
	Subject string `yaml:"subject"`
}

type Data struct {
	Source   string `yaml:"source"`
	PoolSize int    `yaml:"pool_size"`
	Seed     int64  `yaml:"seed"`
	Mapping  string `yaml:"mapping"`
}

type Throughput struct {
	Rate        int          `yaml:"rate"`
	Profile     string       `yaml:"profile"`
	Oscillating *Oscillating `yaml:"oscillating"`
}

type Oscillating struct {
	Min    int           `yaml:"min"`
	Max    int           `yaml:"max"`
	Period time.Duration `yaml:"period"`
	Shape  string        `yaml:"shape"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Shard.Count == 0 {
		c.Shard.Count = 1
	}
	for i := range c.Workloads {
		if c.Workloads[i].Clients == 0 {
			c.Workloads[i].Clients = 1
		}
		if c.Workloads[i].Data.Source == "" {
			c.Workloads[i].Data.Source = "pre_encoded"
		}
		if c.Workloads[i].Throughput.Profile == "" {
			c.Workloads[i].Throughput.Profile = "steady"
		}
	}
}

func (c *Config) Validate() error {
	if c.Brokers == "" {
		return fmt.Errorf("brokers required")
	}
	if c.Shard.Index < 0 || c.Shard.Index >= c.Shard.Count {
		return fmt.Errorf("shard.index %d out of range [0,%d)", c.Shard.Index, c.Shard.Count)
	}
	for _, w := range c.Workloads {
		switch w.Direction {
		case "produce", "consume", "produce_consume":
		default:
			return fmt.Errorf("workload %q: invalid direction %q", w.Name, w.Direction)
		}
		switch w.Data.Source {
		case "fresh", "pre_encoded":
		default:
			return fmt.Errorf("workload %q: invalid data.source %q", w.Name, w.Data.Source)
		}
		if w.Data.Source == "pre_encoded" && w.Data.PoolSize <= 0 {
			return fmt.Errorf("workload %q: pre_encoded requires pool_size > 0", w.Name)
		}
		if w.Schema.Format != "protobuf" && w.Schema.Format != "avro" && w.Schema.Format != "json" {
			return fmt.Errorf("workload %q: invalid schema.format %q", w.Name, w.Schema.Format)
		}
	}
	return nil
}
