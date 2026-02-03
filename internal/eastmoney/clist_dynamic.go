package eastmoney

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// The clist endpoint returns a dynamic field keyed by fid (e.g. "f62").
// To keep the rest of the code strongly typed we decode into map[string]any for each diff row.

func (c *Client) TopListDynamic(ctx context.Context, fs, fid string, size int) ([]TopItem, error) {
	var lastErr error
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}
		_, items, err := c.clistPage(ctx, fs, fid, 1, size)
		if err == nil {
			return items, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func (c *Client) BoardListAll(ctx context.Context, fs, fid string) ([]TopItem, error) {
	const pageSize = 100 // seems capped by API
	total, first, err := c.clistPage(ctx, fs, fid, 1, pageSize)
	if err != nil {
		return nil, err
	}
	out := append([]TopItem(nil), first...)
	if total <= len(out) {
		return out, nil
	}
	pages := (total + pageSize - 1) / pageSize
	for pn := 2; pn <= pages; pn++ {
		_, items, err := c.clistPage(ctx, fs, fid, pn, pageSize)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
	}
	return out, nil
}

func (c *Client) BoardListTop(ctx context.Context, fs, fid string, topSize int) ([]TopItem, error) {
	if topSize <= 0 {
		topSize = 100
	}
	if topSize > 100 {
		topSize = 100
	}
	_, items, err := c.clistPage(ctx, fs, fid, 1, topSize)
	return items, err
}

func (c *Client) clistPage(ctx context.Context, fs, fid string, pn, pz int) (total int, items []TopItem, err error) {
	u := "https://push2.eastmoney.com/api/qt/clist/get"
	q := url.Values{}
	q.Set("pn", strconv.Itoa(pn))
	q.Set("pz", strconv.Itoa(pz))
	q.Set("po", "1")
	q.Set("np", "1")
	q.Set("fltt", "2")
	q.Set("invt", "2")
	q.Set("fid", fid)
	q.Set("fs", fs)
	q.Set("fields", "f12,f14,f2,f3,"+fid)
	u = u + "?" + q.Encode()

	var raw struct {
		RC   int `json:"rc"`
		Data *struct {
			Total int               `json:"total"`
			Diff  []json.RawMessage `json:"diff"`
		} `json:"data"`
	}
	if err := c.getJSON(ctx, u, &raw); err != nil {
		return 0, nil, err
	}
	if raw.RC != 0 || raw.Data == nil {
		return 0, nil, fmt.Errorf("unexpected response rc=%d", raw.RC)
	}

	out := make([]TopItem, 0, len(raw.Data.Diff))
	for i, msg := range raw.Data.Diff {
		var m map[string]any
		if err := json.Unmarshal(msg, &m); err != nil {
			continue
		}
		code, _ := m["f12"].(string)
		name, _ := m["f14"].(string)
		price := asFloat(m["f2"])
		pct := asFloat(m["f3"])
		value := asFloat(m[fid])
		out = append(out, TopItem{Rank: (pn-1)*pz + i + 1, Code: code, Name: name, Price: price, Pct: pct, Value: value})
	}
	return raw.Data.Total, out, nil
}

type QuoteItem struct {
	Code     string  `json:"code"`
	Name     string  `json:"name"`
	Price    float64 `json:"price"`
	Pct      float64 `json:"pct"`
	Open     float64 `json:"open"`
	High     float64 `json:"high"`
	Low      float64 `json:"low"`
	PreClose float64 `json:"pre_close"`
}

// BoardConstituents returns a page of constituent stocks for a board code (e.g. BK0457).
// boardCode: "BKxxxx"; pn: 1-based; pz: <= 100.
func (c *Client) BoardConstituents(ctx context.Context, boardCode string, pn, pz int) (total int, rows []QuoteItem, err error) {
	if boardCode == "" {
		return 0, nil, fmt.Errorf("boardCode is required")
	}
	if pn <= 0 {
		pn = 1
	}
	if pz <= 0 {
		pz = 50
	}
	if pz > 100 {
		pz = 100
	}

	fs := "b:" + boardCode
	u := "https://push2.eastmoney.com/api/qt/clist/get"
	q := url.Values{}
	q.Set("pn", strconv.Itoa(pn))
	q.Set("pz", strconv.Itoa(pz))
	q.Set("po", "1")
	q.Set("np", "1")
	q.Set("fltt", "2")
	q.Set("invt", "2")
	q.Set("fs", fs)
	// f12 code, f14 name, f2 price, f3 pct, f17 open, f15 high, f16 low, f18 preclose
	q.Set("fields", "f12,f14,f2,f3,f17,f15,f16,f18")
	u = u + "?" + q.Encode()

	var lastErr error
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}

		var raw struct {
			RC   int `json:"rc"`
			Data *struct {
				Total int               `json:"total"`
				Diff  []json.RawMessage `json:"diff"`
			} `json:"data"`
		}
		if err := c.getJSON(ctx, u, &raw); err != nil {
			lastErr = err
			continue
		}
		if raw.RC != 0 || raw.Data == nil {
			lastErr = fmt.Errorf("unexpected response rc=%d", raw.RC)
			continue
		}

		out := make([]QuoteItem, 0, len(raw.Data.Diff))
		for _, msg := range raw.Data.Diff {
			var m map[string]any
			if err := json.Unmarshal(msg, &m); err != nil {
				continue
			}
			code, _ := m["f12"].(string)
			name, _ := m["f14"].(string)
			out = append(out, QuoteItem{
				Code:     code,
				Name:     name,
				Price:    asFloat(m["f2"]),
				Pct:      asFloat(m["f3"]),
				Open:     asFloat(m["f17"]),
				High:     asFloat(m["f15"]),
				Low:      asFloat(m["f16"]),
				PreClose: asFloat(m["f18"]),
			})
		}
		return raw.Data.Total, out, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("unknown error")
	}
	return 0, nil, lastErr
}

func asFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case json.Number:
		f, _ := t.Float64()
		return f
	default:
		return 0
	}
}
