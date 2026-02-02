package eastmoney

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	hc *http.Client
}

func NewClient() *Client {
	return &Client{
		hc: &http.Client{
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     30 * time.Second,
				// Some public endpoints occasionally misbehave with HTTP/2 and/or keep-alives.
				ForceAttemptHTTP2: false,
			},
			Timeout: 20 * time.Second,
		},
	}
}

// NorthboundRealtime uses the (free) push2.kamt endpoint; it returns HK->SH and HK->SZ.
func (c *Client) NorthboundRealtime(ctx context.Context) (NorthboundRT, error) {
	u := "https://push2.eastmoney.com/api/qt/kamt/get"
	q := url.Values{}
	q.Set("fields1", "f1,f3")
	q.Set("fields2", "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65,f66,f67,f68")
	u = u + "?" + q.Encode()

	var resp kamtResp
	if err := c.getJSON(ctx, u, &resp); err != nil {
		return NorthboundRT{}, err
	}
	if resp.RC != 0 || resp.Data == nil {
		return NorthboundRT{}, fmt.Errorf("unexpected response rc=%d", resp.RC)
	}

	out := NorthboundRT{
		TradeDate: resp.Data.HK2SH.Date2,
		SH: NorthboundLeg{
			DayNetAmtIn: resp.Data.HK2SH.DayNetAmtIn,
			NetBuyAmt:   resp.Data.HK2SH.NetBuyAmt,
			BuyAmt:      resp.Data.HK2SH.BuyAmt,
			SellAmt:     resp.Data.HK2SH.SellAmt,
			UpdateTime:  resp.Data.HK2SH.UpdateTime,
		},
		SZ: NorthboundLeg{
			DayNetAmtIn: resp.Data.HK2SZ.DayNetAmtIn,
			NetBuyAmt:   resp.Data.HK2SZ.NetBuyAmt,
			BuyAmt:      resp.Data.HK2SZ.BuyAmt,
			SellAmt:     resp.Data.HK2SZ.SellAmt,
			UpdateTime:  resp.Data.HK2SZ.UpdateTime,
		},
	}
	return out, nil
}

// FundflowRealtime fetches "today net inflow" for a list of secids: ["1.600519","0.000001"].
func (c *Client) FundflowRealtime(ctx context.Context, secids []string) ([]FundflowRT, error) {
	if len(secids) == 0 {
		return nil, nil
	}
	u := "https://push2.eastmoney.com/api/qt/ulist.np/get"
	q := url.Values{}
	q.Set("fltt", "2")
	q.Set("secids", joinComma(secids))
	// Fields:
	// f12: code, f14: name, f62: main net, f66: xl, f72: l, f78: m, f84: s
	q.Set("fields", "f12,f14,f62,f66,f72,f78,f84")
	u = u + "?" + q.Encode()

	var resp ulistResp
	if err := c.getJSON(ctx, u, &resp); err != nil {
		return nil, err
	}
	if resp.RC != 0 || resp.Data == nil {
		return nil, fmt.Errorf("unexpected response rc=%d", resp.RC)
	}

	out := make([]FundflowRT, 0, len(resp.Data.Diff))
	for _, d := range resp.Data.Diff {
		out = append(out, FundflowRT{
			Code:     d.F12,
			Name:     d.F14,
			NetMain:  d.F62,
			NetXL:    d.F66,
			NetL:     d.F72,
			NetM:     d.F78,
			NetS:     d.F84,
			RawSecID: "", // not returned
		})
	}
	return out, nil
}

// FundflowDailyLatest returns the latest available daily record for secid.
// This is used for T+0 after close; during trading it may represent a partial day.
func (c *Client) FundflowDailyLatest(ctx context.Context, secid string) (FundflowDaily, error) {
	u := "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get"
	q := url.Values{}
	q.Set("secid", secid)
	q.Set("klt", "101") // daily
	q.Set("lmt", "1")
	q.Set("fields1", "f1,f2,f3,f7")
	// fields2 output comes as a compact CSV in data.klines, so field list isn't strictly required,
	// but keeping it avoids surprises if the API changes default fields.
	q.Set("fields2", "f51,f52,f53,f54,f55,f56")
	u = u + "?" + q.Encode()

	var resp fflowKlineResp
	if err := c.getJSON(ctx, u, &resp); err != nil {
		return FundflowDaily{}, err
	}
	if resp.RC != 0 || resp.Data == nil || len(resp.Data.Klines) == 0 {
		return FundflowDaily{}, fmt.Errorf("unexpected response rc=%d", resp.RC)
	}

	// Format: "YYYY-MM-DD,main,small,medium,large,xl"
	parts := splitComma(resp.Data.Klines[0])
	if len(parts) < 6 {
		return FundflowDaily{}, fmt.Errorf("unexpected kline format: %q", resp.Data.Klines[0])
	}
	main, _ := strconv.ParseFloat(parts[1], 64)
	small, _ := strconv.ParseFloat(parts[2], 64)
	medium, _ := strconv.ParseFloat(parts[3], 64)
	large, _ := strconv.ParseFloat(parts[4], 64)
	xl, _ := strconv.ParseFloat(parts[5], 64)

	return FundflowDaily{
		TradeDate: parts[0],
		SecID:     secid,
		Code:      resp.Data.Code,
		Name:      resp.Data.Name,
		NetMain:   main,
		NetS:      small,
		NetM:      medium,
		NetL:      large,
		NetXL:     xl,
	}, nil
}

// MarginLatestByCode pulls latest per-stock margin record (融资融券) from Eastmoney datacenter.
// NOTE: The datacenter filter grammar is fragile; for stability we filter by code only and take latest record.
func (c *Client) MarginLatestByCode(ctx context.Context, code string) (MarginDaily, error) {
	u := "https://datacenter-web.eastmoney.com/api/data/v1/get"
	q := url.Values{}
	q.Set("reportName", "RPTA_WEB_RZRQ_GGMX")
	q.Set("columns", "ALL")
	// SCODE seems to require double-quote string OR numeric; double-quote keeps leading zeros safe.
	q.Set("filter", fmt.Sprintf(`(SCODE="%s")`, code))
	q.Set("pageNumber", "1")
	q.Set("pageSize", "1")
	q.Set("sortColumns", "DATE")
	q.Set("sortTypes", "-1")
	u = u + "?" + q.Encode()

	var resp datacenterResp[marginRow]
	if err := c.getJSON(ctx, u, &resp); err != nil {
		return MarginDaily{}, err
	}
	if !resp.Success || resp.Result == nil || len(resp.Result.Data) == 0 {
		return MarginDaily{}, fmt.Errorf("no margin data for code=%s", code)
	}
	r := resp.Result.Data[0]
	return MarginDaily{
		TradeDate: formatDatacenterDate(r.DATE),
		Code:      r.SCODE,
		Name:      r.SECNAME,
		Market:    r.TRADE_MARKET,
		RZYE:      r.RZYE,
		RZMRE:     r.RZMRE,
		RZCHE:     r.RZCHE,
		RZJME:     r.RZJME,
		RQYE:      r.RQYE,
		RQMCL:     r.RQMCL,
		RQCHL:     r.RQCHL,
		RQJMG:     r.RQJMG,
		RZRQYE:    r.RZRQYE,
	}, nil
}

func (c *Client) getJSON(ctx context.Context, u string, out any) error {
	var lastErr error
	backoff := 200 * time.Millisecond

	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff *= 3
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; AOFCollector/1.0)")
		req.Header.Set("Accept", "application/json,text/plain,*/*")
		req.Header.Set("Connection", "close")

		resp, err := c.hc.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		var attemptErr error
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			attemptErr = fmt.Errorf("http %d: %s", resp.StatusCode, string(b))
		} else {
			dec := json.NewDecoder(resp.Body)
			dec.UseNumber()
			attemptErr = dec.Decode(out)
		}
		_ = resp.Body.Close()

		if attemptErr == nil {
			return nil
		}

		// Retry on common transient codes.
		if resp.StatusCode == 429 || resp.StatusCode == 502 || resp.StatusCode == 503 || resp.StatusCode == 504 {
			lastErr = attemptErr
			continue
		}
		// For other status codes, don't retry. For decode/network errors we do retry.
		if resp.StatusCode != http.StatusOK {
			return attemptErr
		}
		lastErr = attemptErr
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("unknown error")
	}
	// Some Eastmoney endpoints (notably clist/get) may terminate Go TLS handshakes (EOF).
	// On Windows, fall back to PowerShell Invoke-WebRequest which uses the system stack.
	if strings.Contains(u, "/api/qt/clist/get") {
		if err := getJSONViaPowerShell(ctx, u, out); err == nil {
			return nil
		}
	}
	return lastErr
}
