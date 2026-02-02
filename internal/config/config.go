package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	DBPath    string   `yaml:"db_path"`
	Watchlist []string `yaml:"watchlist"`

	Realtime struct {
		IntervalSeconds int   `yaml:"interval_seconds"`
		OnlyDuringHours *bool `yaml:"only_during_trading_hours"`
	} `yaml:"realtime"`

	Toplist struct {
		Size int    `yaml:"size"`
		FS   string `yaml:"fs"`
		FID  string `yaml:"fid"`
	} `yaml:"toplist"`
}

func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return Config{}, err
	}

	applyDefaults(&cfg)
	if cfg.DBPath == "" {
		return Config{}, fmt.Errorf("db_path is required")
	}
	if cfg.Realtime.IntervalSeconds <= 0 {
		return Config{}, fmt.Errorf("realtime.interval_seconds must be > 0")
	}
	if cfg.Toplist.Size <= 0 {
		cfg.Toplist.Size = 20
	}
	if cfg.Toplist.FID == "" {
		cfg.Toplist.FID = "f62"
	}
	if cfg.Toplist.FS == "" {
		cfg.Toplist.FS = "m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23"
	}
	return cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Realtime.IntervalSeconds == 0 {
		cfg.Realtime.IntervalSeconds = 10
	}
	if cfg.Realtime.OnlyDuringHours == nil {
		// Safe default for scraping: don't run if market is closed.
		v := true
		cfg.Realtime.OnlyDuringHours = &v
	}
}
