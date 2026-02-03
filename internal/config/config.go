package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	DBPath        string   `yaml:"db_path"`
	RetentionDays int      `yaml:"retention_days"`
	Watchlist     []string `yaml:"watchlist"`

	Realtime struct {
		IntervalSeconds int   `yaml:"interval_seconds"`
		OnlyDuringHours *bool `yaml:"only_during_trading_hours"`
	} `yaml:"realtime"`

	Persist struct {
		IntervalSeconds int `yaml:"interval_seconds"`
	} `yaml:"persist"`

	Cleanup struct {
		Enabled *bool  `yaml:"enabled"`
		RunAt   string `yaml:"run_at"` // "HH:MM" in Asia/Shanghai
	} `yaml:"cleanup"`

	Toplist struct {
		Size int    `yaml:"size"`
		FS   string `yaml:"fs"`
		FID  string `yaml:"fid"`
	} `yaml:"toplist"`

	Industry BoardConfig `yaml:"industry"`
	Concept  BoardConfig `yaml:"concept"`

	MarketAgg MarketAggConfig `yaml:"market_agg"`

	BoardTrend BoardTrendConfig `yaml:"board_trend"`
}

type BoardConfig struct {
	Enabled         bool   `yaml:"enabled" json:"enabled"`
	IntervalSeconds int    `yaml:"interval_seconds" json:"interval_seconds"`
	FS              string `yaml:"fs" json:"fs"`
	FID             string `yaml:"fid" json:"fid"`

	// If true, fetch all pages; otherwise fetch only the first page up to TopSize.
	CollectAll bool `yaml:"collect_all" json:"collect_all"`
	TopSize    int  `yaml:"top_size" json:"top_size"`
}

type MarketAggConfig struct {
	Enabled         bool   `yaml:"enabled" json:"enabled"`
	IntervalSeconds int    `yaml:"interval_seconds" json:"interval_seconds"`
	FS              string `yaml:"fs" json:"fs"`
	FID             string `yaml:"fid" json:"fid"`
	Concurrency     int    `yaml:"concurrency" json:"concurrency"`
}

type BoardTrendConfig struct {
	BatchSize   int `yaml:"batch_size" json:"batch_size"`
	Concurrency int `yaml:"concurrency" json:"concurrency"`
	GapMS       int `yaml:"gap_ms" json:"gap_ms"`
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

	if err := NormalizeAndValidate(&cfg); err != nil {
		return Config{}, err
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
	if cfg.Persist.IntervalSeconds == 0 {
		cfg.Persist.IntervalSeconds = 60
	}
	if cfg.RetentionDays == 0 {
		cfg.RetentionDays = 30
	}
	if cfg.Cleanup.RunAt == "" {
		cfg.Cleanup.RunAt = "03:10"
	}
	if cfg.Cleanup.Enabled == nil {
		v := true
		cfg.Cleanup.Enabled = &v
	}
}

// NormalizeAndValidate applies defaults and checks invariants.
func NormalizeAndValidate(cfg *Config) error {
	applyDefaults(cfg)
	if cfg.DBPath == "" {
		return fmt.Errorf("db_path is required")
	}
	if cfg.Realtime.IntervalSeconds <= 0 {
		return fmt.Errorf("realtime.interval_seconds must be > 0")
	}
	if cfg.Persist.IntervalSeconds <= 0 {
		return fmt.Errorf("persist.interval_seconds must be > 0")
	}
	if cfg.RetentionDays < 1 {
		return fmt.Errorf("retention_days must be >= 1")
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
	applyBoardDefaults(&cfg.Industry, true, "m:90+t:2")
	applyBoardDefaults(&cfg.Concept, true, "m:90+t:3")
	applyMarketAggDefaults(&cfg.MarketAgg)
	applyBoardTrendDefaults(&cfg.BoardTrend)
	return nil
}

func applyBoardDefaults(b *BoardConfig, enabled bool, fs string) {
	// If user didn't mention this block at all, keep a useful default.
	if !b.Enabled && b.IntervalSeconds == 0 && b.FS == "" && b.FID == "" && b.TopSize == 0 && !b.CollectAll {
		b.Enabled = enabled
	}
	if !b.Enabled {
		return
	}
	if b.FS == "" {
		b.FS = fs
	}
	if b.FID == "" {
		b.FID = "f62"
	}
	if b.IntervalSeconds == 0 {
		b.IntervalSeconds = 10
	}
	if b.TopSize == 0 {
		b.TopSize = 100
	}
}

func applyMarketAggDefaults(m *MarketAggConfig) {
	// If user didn't specify this block, keep it disabled by default to avoid heavy traffic.
	if !m.Enabled && m.IntervalSeconds == 0 && m.FS == "" && m.FID == "" && m.Concurrency == 0 {
		m.Enabled = false
	}
	if !m.Enabled {
		return
	}
	if m.IntervalSeconds == 0 {
		m.IntervalSeconds = 60
	}
	if m.FS == "" {
		m.FS = "m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23"
	}
	if m.FID == "" {
		m.FID = "f62"
	}
	if m.Concurrency == 0 {
		m.Concurrency = 4
	}
	if m.Concurrency < 1 {
		m.Concurrency = 1
	}
	if m.Concurrency > 10 {
		m.Concurrency = 10
	}
}

func applyBoardTrendDefaults(b *BoardTrendConfig) {
	if b.BatchSize == 0 {
		b.BatchSize = 20
	}
	if b.Concurrency == 0 {
		b.Concurrency = 2
	}
	if b.GapMS == 0 {
		b.GapMS = 400
	}
	if b.BatchSize < 5 {
		b.BatchSize = 5
	}
	if b.BatchSize > 100 {
		b.BatchSize = 100
	}
	if b.Concurrency < 1 {
		b.Concurrency = 1
	}
	if b.Concurrency > 6 {
		b.Concurrency = 6
	}
	if b.GapMS < 100 {
		b.GapMS = 100
	}
	if b.GapMS > 5000 {
		b.GapMS = 5000
	}
}
