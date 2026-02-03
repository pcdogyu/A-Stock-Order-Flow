package eastmoney

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
)

type TrendPoint struct {
	TS    string  `json:"ts"`
	Price float64 `json:"price"`
}

// StockTrends1D returns today's intraday trend points for a stock secid (e.g. "1.600519").
func (c *Client) StockTrends1D(ctx context.Context, secid string) ([]TrendPoint, error) {
	if secid == "" {
		return nil, fmt.Errorf("secid is required")
	}

	return fetchTrends(ctx, c, "https://push2.eastmoney.com/api/qt/stock/trends2/get", secid)
}

// BoardTrends1D returns today's intraday trend points for a board code (e.g. "BK0457").
// Eastmoney uses "90.BKxxxx" as secid for boards.
func (c *Client) BoardTrends1D(ctx context.Context, boardCode string) ([]TrendPoint, error) {
	if boardCode == "" {
		return nil, fmt.Errorf("boardCode is required")
	}
	secid := "90." + boardCode
	out, err := fetchTrends(ctx, c, "https://push2his.eastmoney.com/api/qt/stock/trends2/get", secid)
	if err == nil {
		return out, nil
	}
	// Fallback to push2 domain if push2his fails.
	return fetchTrends(ctx, c, "https://push2.eastmoney.com/api/qt/stock/trends2/get", secid)
}

func fetchTrends(ctx context.Context, c *Client, baseURL, secid string) ([]TrendPoint, error) {
	u := baseURL
	q := url.Values{}
	q.Set("secid", secid)
	q.Set("ndays", "1")
	q.Set("iscr", "0")
	q.Set("iscca", "0")
	q.Set("fields1", "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11")
	q.Set("fields2", "f51,f52,f53,f54,f55,f56,f57,f58")
	u = u + "?" + q.Encode()

	var raw struct {
		RC   int `json:"rc"`
		Data *struct {
			Trends []string `json:"trends"`
		} `json:"data"`
	}
	if err := c.getJSON(ctx, u, &raw); err != nil {
		return nil, err
	}
	if raw.RC != 0 || raw.Data == nil {
		return nil, fmt.Errorf("unexpected response rc=%d", raw.RC)
	}

	out := make([]TrendPoint, 0, len(raw.Data.Trends))
	for _, s := range raw.Data.Trends {
		parts := splitComma(s)
		if len(parts) < 2 {
			continue
		}
		price, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}
		out = append(out, TrendPoint{TS: parts[0], Price: price})
	}
	return out, nil
}
