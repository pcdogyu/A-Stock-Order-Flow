package eastmoney

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
)

type BoardFundflowDaily struct {
	TradeDate string
	Code      string
	Name      string
	NetMain   float64
	NetXL     float64
	NetL      float64
	NetM      float64
	NetS      float64
}

// BoardFundflowDailySeries returns daily fundflow series for a board code (e.g. BK0457).
// It uses the same fflow kline endpoint as stocks, but with secid "90.BKxxxx".
func (c *Client) BoardFundflowDailySeries(ctx context.Context, boardCode string, limit int) ([]BoardFundflowDaily, error) {
	if boardCode == "" {
		return nil, fmt.Errorf("boardCode is required")
	}
	if limit <= 0 {
		limit = 200
	}
	secid := "90." + boardCode
	u := "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get"
	q := url.Values{}
	q.Set("secid", secid)
	q.Set("klt", "101") // daily
	q.Set("lmt", strconv.Itoa(limit))
	q.Set("fields1", "f1,f2,f3,f7")
	q.Set("fields2", "f51,f52,f53,f54,f55,f56")
	u = u + "?" + q.Encode()

	var resp fflowKlineResp
	if err := c.getJSON(ctx, u, &resp); err != nil {
		return nil, err
	}
	if resp.RC != 0 || resp.Data == nil || len(resp.Data.Klines) == 0 {
		return nil, fmt.Errorf("unexpected response rc=%d", resp.RC)
	}

	out := make([]BoardFundflowDaily, 0, len(resp.Data.Klines))
	for _, line := range resp.Data.Klines {
		parts := splitComma(line)
		if len(parts) < 6 {
			continue
		}
		main, _ := strconv.ParseFloat(parts[1], 64)
		small, _ := strconv.ParseFloat(parts[2], 64)
		medium, _ := strconv.ParseFloat(parts[3], 64)
		large, _ := strconv.ParseFloat(parts[4], 64)
		xl, _ := strconv.ParseFloat(parts[5], 64)

		out = append(out, BoardFundflowDaily{
			TradeDate: parts[0],
			Code:      resp.Data.Code,
			Name:      resp.Data.Name,
			NetMain:   main,
			NetS:      small,
			NetM:      medium,
			NetL:      large,
			NetXL:     xl,
		})
	}
	return out, nil
}
