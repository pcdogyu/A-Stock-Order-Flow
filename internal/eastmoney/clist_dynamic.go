package eastmoney

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
)

// The clist endpoint returns a dynamic field keyed by fid (e.g. "f62").
// To keep the rest of the code strongly typed we decode into map[string]any for each diff row.

func (c *Client) TopListDynamic(ctx context.Context, fs, fid string, size int) ([]TopItem, error) {
	_, items, err := c.clistPage(ctx, fs, fid, 1, size)
	return items, err
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
			Diff []json.RawMessage `json:"diff"`
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
