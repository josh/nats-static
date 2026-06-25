package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Config is the on-disk config.json schema. The Helm chart renders this file
// and injects only the auth *_file path(s) for the selected NATS auth method;
// the credential material itself lives in the referenced files (mounted from a
// Secret), never inline.
type Config struct {
	NATS struct {
		URL          string `json:"url"`
		UserFile     string `json:"user_file,omitempty"`
		PasswordFile string `json:"password_file,omitempty"`
		TokenFile    string `json:"token_file,omitempty"`
		CredsFile    string `json:"creds_file,omitempty"`
		NkeySeedFile string `json:"nkey_seed_file,omitempty"`
	} `json:"nats"`
	ObjectStore struct {
		Bucket string `json:"bucket"`
	} `json:"object_store"`
	HTTP struct {
		Listen string `json:"listen"`
	} `json:"http"`
	Multipart struct {
		// IdleTimeout is how long an upload session may go without receiving a
		// chunk before it is aborted. A Go duration string (e.g. "30s").
		IdleTimeout string `json:"idle_timeout,omitempty"`
		// MaxSessions caps concurrent in-flight upload sessions on a replica,
		// bounding memory.
		MaxSessions int `json:"max_sessions,omitempty"`
	} `json:"multipart"`
}

// loadConfig reads and parses config.json, applying defaults for any omitted
// fields so the binary runs against a local NATS with an empty config.
func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.NATS.URL == "" {
		cfg.NATS.URL = nats.DefaultURL
	}
	if cfg.ObjectStore.Bucket == "" {
		cfg.ObjectStore.Bucket = "static"
	}
	if cfg.HTTP.Listen == "" {
		cfg.HTTP.Listen = ":8080"
	}
	if cfg.Multipart.IdleTimeout == "" {
		cfg.Multipart.IdleTimeout = "30s"
	}
	if cfg.Multipart.MaxSessions == 0 {
		cfg.Multipart.MaxSessions = 64
	}
	return &cfg, nil
}

// natsOptions builds the connection options for the configured auth method,
// selected by whichever *_file field is set. Exactly one method is expected
// (the chart enforces this); an empty config connects anonymously.
func (c *Config) natsOptions() ([]nats.Option, error) {
	switch {
	case c.NATS.UserFile != "" || c.NATS.PasswordFile != "":
		user, err := readTrimmed(c.NATS.UserFile)
		if err != nil {
			return nil, err
		}
		pass, err := readTrimmed(c.NATS.PasswordFile)
		if err != nil {
			return nil, err
		}
		return []nats.Option{nats.UserInfo(user, pass)}, nil
	case c.NATS.TokenFile != "":
		token, err := readTrimmed(c.NATS.TokenFile)
		if err != nil {
			return nil, err
		}
		return []nats.Option{nats.Token(token)}, nil
	case c.NATS.CredsFile != "":
		return []nats.Option{nats.UserCredentials(c.NATS.CredsFile)}, nil
	case c.NATS.NkeySeedFile != "":
		opt, err := nats.NkeyOptionFromSeed(c.NATS.NkeySeedFile)
		if err != nil {
			return nil, err
		}
		return []nats.Option{opt}, nil
	default:
		return nil, nil
	}
}

// idleTimeout parses the configured multipart idle timeout, falling back to 30s
// if it is unset or invalid.
func (c *Config) idleTimeout() time.Duration {
	if d, err := time.ParseDuration(c.Multipart.IdleTimeout); err == nil && d > 0 {
		return d
	}
	return 30 * time.Second
}

func readTrimmed(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
