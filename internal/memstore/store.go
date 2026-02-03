package memstore

import (
	"sync"
	"time"

	"github.com/pcdogyu/A-Stock-Order-Flow/internal/eastmoney"
)

// Store keeps the latest realtime fetch results in memory.
// This avoids writing every tick to SQLite; a separate snapshot task persists periodically.
type Store struct {
	mu sync.RWMutex

	northbound struct {
		tsUTC time.Time
		val   eastmoney.NorthboundRT
		ok    bool
	}

	fundflow struct {
		tsUTC time.Time
		byCode map[string]eastmoney.FundflowRT
	}

	toplist struct {
		tsUTC time.Time
		byFID map[string][]eastmoney.TopItem
	}

	boards struct {
		tsUTC time.Time
		// key: boardType ("industry"/"concept") + ":" + fid
		byKey map[string][]eastmoney.TopItem
	}

	agg struct {
		tsUTC time.Time
		// key: source + ":" + fid
		byKey map[string]float64
	}
}

func New() *Store {
	return &Store{
		fundflow: struct {
			tsUTC  time.Time
			byCode map[string]eastmoney.FundflowRT
		}{byCode: make(map[string]eastmoney.FundflowRT)},
		toplist: struct {
			tsUTC time.Time
			byFID map[string][]eastmoney.TopItem
		}{byFID: make(map[string][]eastmoney.TopItem)},
		boards: struct {
			tsUTC time.Time
			byKey map[string][]eastmoney.TopItem
		}{byKey: make(map[string][]eastmoney.TopItem)},
		agg: struct {
			tsUTC time.Time
			byKey map[string]float64
		}{byKey: make(map[string]float64)},
	}
}

func (s *Store) SetNorthbound(tsUTC time.Time, v eastmoney.NorthboundRT) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.northbound.tsUTC = tsUTC
	s.northbound.val = v
	s.northbound.ok = true
}

func (s *Store) SetFundflow(tsUTC time.Time, rows []eastmoney.FundflowRT) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fundflow.tsUTC = tsUTC
	if s.fundflow.byCode == nil {
		s.fundflow.byCode = make(map[string]eastmoney.FundflowRT)
	}
	for _, r := range rows {
		if r.Code != "" {
			s.fundflow.byCode[r.Code] = r
		}
	}
}

func (s *Store) SetToplist(tsUTC time.Time, fid string, rows []eastmoney.TopItem) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.toplist.tsUTC = tsUTC
	if s.toplist.byFID == nil {
		s.toplist.byFID = make(map[string][]eastmoney.TopItem)
	}
	s.toplist.byFID[fid] = append([]eastmoney.TopItem(nil), rows...)
}

func (s *Store) SetBoard(tsUTC time.Time, boardType, fid string, rows []eastmoney.TopItem) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.boards.tsUTC = tsUTC
	if s.boards.byKey == nil {
		s.boards.byKey = make(map[string][]eastmoney.TopItem)
	}
	key := boardType + ":" + fid
	s.boards.byKey[key] = append([]eastmoney.TopItem(nil), rows...)
}

func (s *Store) SetAgg(tsUTC time.Time, source, fid string, value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.agg.tsUTC = tsUTC
	if s.agg.byKey == nil {
		s.agg.byKey = make(map[string]float64)
	}
	key := source + ":" + fid
	s.agg.byKey[key] = value
}

type Snapshot struct {
	TSUTC time.Time

	Northbound *eastmoney.NorthboundRT
	Fundflow   []eastmoney.FundflowRT

	ToplistByFID map[string][]eastmoney.TopItem
	BoardsByKey  map[string][]eastmoney.TopItem
	AggByKey     map[string]float64
}

func (s *Store) Snapshot(tsUTC time.Time) Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var nb *eastmoney.NorthboundRT
	if s.northbound.ok {
		tmp := s.northbound.val
		nb = &tmp
	}

	ff := make([]eastmoney.FundflowRT, 0, len(s.fundflow.byCode))
	for _, v := range s.fundflow.byCode {
		ff = append(ff, v)
	}

	top := make(map[string][]eastmoney.TopItem, len(s.toplist.byFID))
	for k, v := range s.toplist.byFID {
		top[k] = append([]eastmoney.TopItem(nil), v...)
	}

	boards := make(map[string][]eastmoney.TopItem, len(s.boards.byKey))
	for k, v := range s.boards.byKey {
		boards[k] = append([]eastmoney.TopItem(nil), v...)
	}

	agg := make(map[string]float64, len(s.agg.byKey))
	for k, v := range s.agg.byKey {
		agg[k] = v
	}

	return Snapshot{
		TSUTC:        tsUTC,
		Northbound:   nb,
		Fundflow:     ff,
		ToplistByFID: top,
		BoardsByKey:  boards,
		AggByKey:     agg,
	}
}

