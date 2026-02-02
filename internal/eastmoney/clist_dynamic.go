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
	u := "https://push2.eastmoney.com/api/qt/clist/get"
	q := url.Values{}
	q.Set("pn", "1")
	q.Set("pz", strconv.Itoa(size))
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
			Diff []json.RawMessage `json:"diff"`
		} `json:"data"`
	}
	if err := c.getJSON(ctx, u, &raw); err != nil {
		return nil, err
	}
	if raw.RC != 0 || raw.Data == nil {
		return nil, fmt.Errorf("unexpected response rc=%d", raw.RC)
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
		out = append(out, TopItem{Rank: i + 1, Code: code, Name: name, Price: price, Pct: pct, Value: value})
	}
	return out, nil
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

