package sqlite

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/pcdogyu/A-Stock-Order-Flow/internal/eastmoney"
)

type RTSnapshot struct {
	TSUTC string

	Northbound   *eastmoney.NorthboundRT
	Fundflow     []eastmoney.FundflowRT
	ToplistByFID map[string][]eastmoney.TopItem
	BoardsByKey  map[string][]eastmoney.TopItem
	AggByKey     map[string]float64
}

// LoadLatestRTSnapshot returns the most recent persisted realtime snapshot.
// ok is false when no rt tables have data.
func LoadLatestRTSnapshot(db *sql.DB) (snap RTSnapshot, ok bool, err error) {
	ts, err := latestRTTimestamp(db)
	if err != nil {
		return snap, false, err
	}
	if ts == "" {
		return snap, false, nil
	}
	snap.TSUTC = ts

	nb, err := QueryNorthboundRTAt(db, ts)
	if err != nil {
		return snap, false, err
	}
	snap.Northbound = nb

	ff, err := QueryFundflowRTAt(db, ts)
	if err != nil {
		return snap, false, err
	}
	snap.Fundflow = ff

	top, err := QueryToplistRTAt(db, ts)
	if err != nil {
		return snap, false, err
	}
	snap.ToplistByFID = top

	boards, err := QueryBoardsRTAt(db, ts)
	if err != nil {
		return snap, false, err
	}
	snap.BoardsByKey = boards

	agg, err := QueryMarketAggRTAt(db, ts)
	if err != nil {
		return snap, false, err
	}
	snap.AggByKey = agg

	return snap, true, nil
}

func latestRTTimestamp(db *sql.DB) (string, error) {
	tables := []string{
		"northbound_rt",
		"fundflow_rt",
		"toplist_rt",
		"board_rt",
		"market_agg_rt",
	}
	var maxTS string
	for _, tbl := range tables {
		var ts sql.NullString
		if err := db.QueryRow(fmt.Sprintf(`SELECT MAX(ts_utc) FROM %s`, tbl)).Scan(&ts); err != nil {
			return "", err
		}
		if ts.Valid && ts.String > maxTS {
			maxTS = ts.String
		}
	}
	return maxTS, nil
}

func QueryNorthboundRTAt(db *sql.DB, tsUTC string) (*eastmoney.NorthboundRT, error) {
	row := db.QueryRow(`
		SELECT trade_date,
			sh_day_net_amt_in, sh_net_buy_amt, sh_buy_amt, sh_sell_amt,
			sz_day_net_amt_in, sz_net_buy_amt, sz_buy_amt, sz_sell_amt
		FROM northbound_rt
		WHERE ts_utc = ?
		LIMIT 1
	`, tsUTC)

	var tradeDate string
	var shDay, shNet, shBuy, shSell float64
	var szDay, szNet, szBuy, szSell float64
	if err := row.Scan(&tradeDate, &shDay, &shNet, &shBuy, &shSell, &szDay, &szNet, &szBuy, &szSell); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &eastmoney.NorthboundRT{
		TradeDate: tradeDate,
		SH: eastmoney.NorthboundLeg{
			DayNetAmtIn: shDay,
			NetBuyAmt:   shNet,
			BuyAmt:      shBuy,
			SellAmt:     shSell,
		},
		SZ: eastmoney.NorthboundLeg{
			DayNetAmtIn: szDay,
			NetBuyAmt:   szNet,
			BuyAmt:      szBuy,
			SellAmt:     szSell,
		},
	}, nil
}

func QueryFundflowRTAt(db *sql.DB, tsUTC string) ([]eastmoney.FundflowRT, error) {
	rows, err := db.Query(`
		SELECT code, name, net_main, net_xl, net_l, net_m, net_s
		FROM fundflow_rt
		WHERE ts_utc = ?
	`, tsUTC)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]eastmoney.FundflowRT, 0, 64)
	for rows.Next() {
		var r eastmoney.FundflowRT
		if err := rows.Scan(&r.Code, &r.Name, &r.NetMain, &r.NetXL, &r.NetL, &r.NetM, &r.NetS); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func QueryToplistRTAt(db *sql.DB, tsUTC string) (map[string][]eastmoney.TopItem, error) {
	rows, err := db.Query(`
		SELECT fid, rank, code, name, price, pct, value
		FROM toplist_rt
		WHERE ts_utc = ?
		ORDER BY fid, rank
	`, tsUTC)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string][]eastmoney.TopItem)
	for rows.Next() {
		var fid string
		var it eastmoney.TopItem
		if err := rows.Scan(&fid, &it.Rank, &it.Code, &it.Name, &it.Price, &it.Pct, &it.Value); err != nil {
			return nil, err
		}
		out[fid] = append(out[fid], it)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func QueryBoardsRTAt(db *sql.DB, tsUTC string) (map[string][]eastmoney.TopItem, error) {
	rows, err := db.Query(`
		SELECT board_type, fid, code, name, price, pct, value
		FROM board_rt
		WHERE ts_utc = ?
		ORDER BY board_type, fid, value DESC
	`, tsUTC)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string][]eastmoney.TopItem)
	for rows.Next() {
		var bt, fid string
		var it eastmoney.TopItem
		if err := rows.Scan(&bt, &fid, &it.Code, &it.Name, &it.Price, &it.Pct, &it.Value); err != nil {
			return nil, err
		}
		key := bt + ":" + fid
		out[key] = append(out[key], it)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func QueryMarketAggRTAt(db *sql.DB, tsUTC string) (map[string]float64, error) {
	rows, err := db.Query(`
		SELECT source, fid, value
		FROM market_agg_rt
		WHERE ts_utc = ?
	`, tsUTC)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]float64)
	for rows.Next() {
		var source, fid string
		var v float64
		if err := rows.Scan(&source, &fid, &v); err != nil {
			return nil, err
		}
		out[source+":"+fid] = v
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// QueryBoardRTLatest returns the latest board snapshot for a board type + fid.
func QueryBoardRTLatest(db *sql.DB, boardType, fid string, limit int) (string, []eastmoney.TopItem, error) {
	if limit <= 0 {
		limit = 50
	}
	var ts sql.NullString
	if err := db.QueryRow(`
		SELECT MAX(ts_utc)
		FROM board_rt
		WHERE board_type = ? AND fid = ?
	`, boardType, fid).Scan(&ts); err != nil {
		return "", nil, err
	}
	if !ts.Valid || ts.String == "" {
		return "", nil, nil
	}

	rows, err := db.Query(`
		SELECT code, name, price, pct, value
		FROM board_rt
		WHERE board_type = ? AND fid = ? AND ts_utc = ?
		ORDER BY value DESC
		LIMIT ?
	`, boardType, fid, ts.String, limit)
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()

	out := make([]eastmoney.TopItem, 0, limit)
	rank := 1
	for rows.Next() {
		var it eastmoney.TopItem
		if err := rows.Scan(&it.Code, &it.Name, &it.Price, &it.Pct, &it.Value); err != nil {
			return "", nil, err
		}
		it.Rank = rank
		rank++
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return "", nil, err
	}
	return ts.String, out, nil
}
