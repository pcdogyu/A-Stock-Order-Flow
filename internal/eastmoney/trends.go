package eastmoney

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"
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

	out, err := fetchTrends(ctx, c, "https://push2.eastmoney.com/api/qt/stock/trends2/get", secid)
	if err == nil {
		return out, nil
	}
	// Fallback: trends2/get can be flaky; for indices in particular we often can still fetch 1-minute kline.
	alt, err2 := fetchTrendsViaKline1m(ctx, c, secid)
	if err2 == nil && len(alt) > 0 {
		return alt, nil
	}
	return nil, err
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

func fetchTrendsViaKline1m(ctx context.Context, c *Client, secid string) ([]TrendPoint, error) {
	u := "https://push2his.eastmoney.com/api/qt/stock/kline/get"
	q := url.Values{}
	q.Set("secid", secid)
	q.Set("klt", "1")       // 1-minute
	q.Set("fqt", "1")       // adjusted type; doesn't matter for index, keep stable
	q.Set("beg", "0")       // last period
	q.Set("end", "20500101")
	q.Set("fields1", "f1,f2,f3,f4,f5")
	q.Set("fields2", "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61")
	u = u + "?" + q.Encode()

	var raw struct {
		RC   int `json:"rc"`
		Data *struct {
			Klines []string `json:"klines"`
		} `json:"data"`
	}
	if err := c.getJSON(ctx, u, &raw); err != nil {
		return nil, err
	}
	if raw.RC != 0 || raw.Data == nil {
		return nil, fmt.Errorf("unexpected response rc=%d", raw.RC)
	}

	// Filter to "today" in Beijing time to avoid occasional carry-over in the response.
	loc, _ := time.LoadLocation("Asia/Shanghai")
	todayPrefix := time.Now().In(loc).Format("2006-01-02")

	out := make([]TrendPoint, 0, len(raw.Data.Klines))
	for _, s := range raw.Data.Klines {
		parts := splitComma(s)
		// Format: "YYYY-MM-DD HH:MM,open,close,high,low,..."
		if len(parts) < 3 {
			continue
		}
		ts := parts[0]
		if len(ts) >= 10 && ts[:10] != todayPrefix {
			continue
		}
		price, err := strconv.ParseFloat(parts[2], 64) // close
		if err != nil {
			continue
		}
		out = append(out, TrendPoint{TS: ts, Price: price})
	}
	return out, nil
}
