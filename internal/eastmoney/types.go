package eastmoney

import (
	"strings"
)

type NorthboundLeg struct {
	DayNetAmtIn float64
	NetBuyAmt   float64
	BuyAmt      float64
	SellAmt     float64
	DayAmtRemain    float64
	DayAmtThreshold float64
	BuySellAmt      float64
	BuySellAmtDate  int64
	UpdateTime  int64
}

type NorthboundRT struct {
	TradeDate string
	SH        NorthboundLeg
	SZ        NorthboundLeg
}

type FundflowRT struct {
	Code     string
	Name     string
	NetMain  float64
	NetXL    float64
	NetL     float64
	NetM     float64
	NetS     float64
	RawSecID string
}

type FundflowDaily struct {
	TradeDate string
	SecID     string
	Code      string
	Name      string
	NetMain   float64
	NetXL     float64
	NetL      float64
	NetM      float64
	NetS      float64
}

type TopItem struct {
	Rank  int
	Code  string
	Name  string
	Price float64
	Pct   float64
	Value float64
}

type MarginDaily struct {
	TradeDate string
	Code      string
	Name      string
	Market    string

	RZYE   float64
	RZMRE  float64
	RZCHE  float64
	RZJME  float64
	RQYE   float64
	RQMCL  float64
	RQCHL  float64
	RQJMG  float64
	RZRQYE float64
}

type kamtResp struct {
	RC   int `json:"rc"`
	Data *struct {
		HK2SH kamtLeg `json:"hk2sh"`
		HK2SZ kamtLeg `json:"hk2sz"`
	} `json:"data"`
}

type kamtLeg struct {
	Date2       string  `json:"date2"`
	DayNetAmtIn float64 `json:"dayNetAmtIn"`
	DayAmtRemain    float64 `json:"dayAmtRemain"`
	DayAmtThreshold float64 `json:"dayAmtThreshold"`
	BuyAmt      float64 `json:"buyAmt"`
	SellAmt     float64 `json:"sellAmt"`
	BuySellAmt  float64 `json:"buySellAmt"`
	NetBuyAmt   float64 `json:"netBuyAmt"`
	UpdateTime  int64   `json:"updateTime"`
	BuySellAmtDate int64 `json:"buySellAmtDate"`
}

type ulistResp struct {
	RC   int `json:"rc"`
	Data *struct {
		Diff []struct {
			F12 string  `json:"f12"`
			F14 string  `json:"f14"`
			F62 float64 `json:"f62"`
			F66 float64 `json:"f66"`
			F72 float64 `json:"f72"`
			F78 float64 `json:"f78"`
			F84 float64 `json:"f84"`
		} `json:"diff"`
	} `json:"data"`
}

type fflowKlineResp struct {
	RC   int `json:"rc"`
	Data *struct {
		Code   string   `json:"code"`
		Name   string   `json:"name"`
		Klines []string `json:"klines"`
	} `json:"data"`
}

type clistResp struct {
	RC   int `json:"rc"`
	Data *struct {
		Diff []struct {
			F12  string  `json:"f12"`
			F14  string  `json:"f14"`
			F2   float64 `json:"f2"`
			F3   float64 `json:"f3"`
			FAny float64 `json:"-"`
		} `json:"diff"`
	} `json:"data"`
}

// Custom unmarshal isn't needed if we post-process; we keep it simple by decoding into map later.
// But clist "fid" field is dynamic. We'll rely on json.RawMessage by decoding into map-like.
// To avoid complexity, we decode fid into FAny by re-marshalling each diff entry in TopList.

type datacenterResp[T any] struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Code    int    `json:"code"`
	Result  *struct {
		Data  []T `json:"data"`
		Count int `json:"count"`
	} `json:"result"`
}

type marginRow struct {
	DATE         string  `json:"DATE"`
	SCODE        string  `json:"SCODE"`
	SECNAME      string  `json:"SECNAME"`
	TRADE_MARKET string  `json:"TRADE_MARKET"`
	RZYE         float64 `json:"RZYE"`
	RZMRE        float64 `json:"RZMRE"`
	RZCHE        float64 `json:"RZCHE"`
	RZJME        float64 `json:"RZJME"`
	RQYE         float64 `json:"RQYE"`
	RQMCL        float64 `json:"RQMCL"`
	RQCHL        float64 `json:"RQCHL"`
	RQJMG        float64 `json:"RQJMG"`
	RZRQYE       float64 `json:"RZRQYE"`
}

func joinComma(ss []string) string {
	return strings.Join(ss, ",")
}

func splitComma(s string) []string {
	// Eastmoney uses commas, no quoting.
	return strings.Split(s, ",")
}

func formatDatacenterDate(s string) string {
	// "2024-06-07 00:00:00" -> "2024-06-07"
	if len(s) >= 10 {
		return s[:10]
	}
	return s
}
