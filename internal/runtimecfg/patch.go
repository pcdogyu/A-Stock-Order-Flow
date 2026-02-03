package runtimecfg

import "github.com/pcdogyu/A-Stock-Order-Flow/internal/config"

// Patch is a partial update for settings exposed in the web UI.
// Fields are pointers so "not set" can be distinguished from zero values.
type Patch struct {
	Watchlist []string `json:"watchlist,omitempty"`

	RealtimeIntervalSeconds *int  `json:"realtime_interval_seconds,omitempty"`
	OnlyDuringTradingHours  *bool `json:"only_during_trading_hours,omitempty"`

	ToplistSize *int `json:"toplist_size,omitempty"`

	IndustryEnabled         *bool `json:"industry_enabled,omitempty"`
	IndustryIntervalSeconds *int  `json:"industry_interval_seconds,omitempty"`

	ConceptEnabled         *bool `json:"concept_enabled,omitempty"`
	ConceptIntervalSeconds *int  `json:"concept_interval_seconds,omitempty"`
	ConceptCollectAll      *bool `json:"concept_collect_all,omitempty"`
	ConceptTopSize         *int  `json:"concept_top_size,omitempty"`

	MarketAggEnabled         *bool `json:"market_agg_enabled,omitempty"`
	MarketAggIntervalSeconds *int  `json:"market_agg_interval_seconds,omitempty"`
	MarketAggConcurrency     *int  `json:"market_agg_concurrency,omitempty"`

	BoardTrendBatchSize   *int `json:"board_trend_batch_size,omitempty"`
	BoardTrendConcurrency *int `json:"board_trend_concurrency,omitempty"`
	BoardTrendGapMS       *int `json:"board_trend_gap_ms,omitempty"`
}

func (p Patch) Apply(cfg *config.Config) {
	if p.Watchlist != nil {
		cfg.Watchlist = p.Watchlist
	}
	if p.RealtimeIntervalSeconds != nil {
		cfg.Realtime.IntervalSeconds = *p.RealtimeIntervalSeconds
	}
	if p.OnlyDuringTradingHours != nil {
		cfg.Realtime.OnlyDuringHours = p.OnlyDuringTradingHours
	}
	if p.ToplistSize != nil {
		cfg.Toplist.Size = *p.ToplistSize
	}

	if p.IndustryEnabled != nil {
		cfg.Industry.Enabled = *p.IndustryEnabled
	}
	if p.IndustryIntervalSeconds != nil {
		cfg.Industry.IntervalSeconds = *p.IndustryIntervalSeconds
	}

	if p.ConceptEnabled != nil {
		cfg.Concept.Enabled = *p.ConceptEnabled
	}
	if p.ConceptIntervalSeconds != nil {
		cfg.Concept.IntervalSeconds = *p.ConceptIntervalSeconds
	}
	if p.ConceptCollectAll != nil {
		cfg.Concept.CollectAll = *p.ConceptCollectAll
	}
	if p.ConceptTopSize != nil {
		cfg.Concept.TopSize = *p.ConceptTopSize
	}

	if p.MarketAggEnabled != nil {
		cfg.MarketAgg.Enabled = *p.MarketAggEnabled
	}
	if p.MarketAggIntervalSeconds != nil {
		cfg.MarketAgg.IntervalSeconds = *p.MarketAggIntervalSeconds
	}
	if p.MarketAggConcurrency != nil {
		cfg.MarketAgg.Concurrency = *p.MarketAggConcurrency
	}

	if p.BoardTrendBatchSize != nil {
		cfg.BoardTrend.BatchSize = *p.BoardTrendBatchSize
	}
	if p.BoardTrendConcurrency != nil {
		cfg.BoardTrend.Concurrency = *p.BoardTrendConcurrency
	}
	if p.BoardTrendGapMS != nil {
		cfg.BoardTrend.GapMS = *p.BoardTrendGapMS
	}
}
