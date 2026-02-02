package sqlite

import (
	"database/sql"
	"time"

	"github.com/pcdogyu/A-Stock-Order-Flow/internal/eastmoney"
)

func UpsertNorthboundRT(db *sql.DB, tsUTC time.Time, nb eastmoney.NorthboundRT) error {
	_, err := db.Exec(`
		INSERT INTO northbound_rt(
			ts_utc, trade_date,
			sh_day_net_amt_in, sh_net_buy_amt, sh_buy_amt, sh_sell_amt,
			sz_day_net_amt_in, sz_net_buy_amt, sz_buy_amt, sz_sell_amt
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(ts_utc) DO UPDATE SET
			trade_date=excluded.trade_date,
			sh_day_net_amt_in=excluded.sh_day_net_amt_in,
			sh_net_buy_amt=excluded.sh_net_buy_amt,
			sh_buy_amt=excluded.sh_buy_amt,
			sh_sell_amt=excluded.sh_sell_amt,
			sz_day_net_amt_in=excluded.sz_day_net_amt_in,
			sz_net_buy_amt=excluded.sz_net_buy_amt,
			sz_buy_amt=excluded.sz_buy_amt,
			sz_sell_amt=excluded.sz_sell_amt
	`, tsUTC.Format(time.RFC3339Nano), nb.TradeDate,
		nb.SH.DayNetAmtIn, nb.SH.NetBuyAmt, nb.SH.BuyAmt, nb.SH.SellAmt,
		nb.SZ.DayNetAmtIn, nb.SZ.NetBuyAmt, nb.SZ.BuyAmt, nb.SZ.SellAmt)
	return err
}

func UpsertNorthboundDaily(db *sql.DB, tradeDate string, nb eastmoney.NorthboundRT) error {
	_, err := db.Exec(`
		INSERT INTO northbound_daily(
			trade_date,
			sh_day_net_amt_in, sh_net_buy_amt, sh_buy_amt, sh_sell_amt,
			sz_day_net_amt_in, sz_net_buy_amt, sz_buy_amt, sz_sell_amt
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(trade_date) DO UPDATE SET
			sh_day_net_amt_in=excluded.sh_day_net_amt_in,
			sh_net_buy_amt=excluded.sh_net_buy_amt,
			sh_buy_amt=excluded.sh_buy_amt,
			sh_sell_amt=excluded.sh_sell_amt,
			sz_day_net_amt_in=excluded.sz_day_net_amt_in,
			sz_net_buy_amt=excluded.sz_net_buy_amt,
			sz_buy_amt=excluded.sz_buy_amt,
			sz_sell_amt=excluded.sz_sell_amt
	`, tradeDate,
		nb.SH.DayNetAmtIn, nb.SH.NetBuyAmt, nb.SH.BuyAmt, nb.SH.SellAmt,
		nb.SZ.DayNetAmtIn, nb.SZ.NetBuyAmt, nb.SZ.BuyAmt, nb.SZ.SellAmt)
	return err
}

func UpsertFundflowRT(db *sql.DB, tsUTC time.Time, rows []eastmoney.FundflowRT) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(`
		INSERT INTO fundflow_rt(ts_utc, code, name, net_main, net_xl, net_l, net_m, net_s)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(ts_utc, code) DO UPDATE SET
			name=excluded.name,
			net_main=excluded.net_main,
			net_xl=excluded.net_xl,
			net_l=excluded.net_l,
			net_m=excluded.net_m,
			net_s=excluded.net_s
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	ts := tsUTC.Format(time.RFC3339Nano)
	for _, r := range rows {
		if _, err := stmt.Exec(ts, r.Code, r.Name, r.NetMain, r.NetXL, r.NetL, r.NetM, r.NetS); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func UpsertFundflowDaily(db *sql.DB, tradeDate string, row eastmoney.FundflowDaily) error {
	_, err := db.Exec(`
		INSERT INTO fundflow_daily(trade_date, secid, code, name, net_main, net_xl, net_l, net_m, net_s)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(trade_date, secid) DO UPDATE SET
			code=excluded.code,
			name=excluded.name,
			net_main=excluded.net_main,
			net_xl=excluded.net_xl,
			net_l=excluded.net_l,
			net_m=excluded.net_m,
			net_s=excluded.net_s
	`, tradeDate, row.SecID, row.Code, row.Name, row.NetMain, row.NetXL, row.NetL, row.NetM, row.NetS)
	return err
}

func UpsertTopListRT(db *sql.DB, tsUTC time.Time, fid string, rows []eastmoney.TopItem) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(`
		INSERT INTO toplist_rt(ts_utc, fid, rank, code, name, price, pct, value)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(ts_utc, fid, rank) DO UPDATE SET
			code=excluded.code,
			name=excluded.name,
			price=excluded.price,
			pct=excluded.pct,
			value=excluded.value
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	ts := tsUTC.Format(time.RFC3339Nano)
	for _, r := range rows {
		if _, err := stmt.Exec(ts, fid, r.Rank, r.Code, r.Name, r.Price, r.Pct, r.Value); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func UpsertBoardRT(db *sql.DB, tsUTC time.Time, boardType, fid string, rows []eastmoney.TopItem) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(`
		INSERT INTO board_rt(ts_utc, board_type, fid, code, name, price, pct, value)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(ts_utc, board_type, fid, code) DO UPDATE SET
			name=excluded.name,
			price=excluded.price,
			pct=excluded.pct,
			value=excluded.value
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	ts := tsUTC.Format(time.RFC3339Nano)
	for _, r := range rows {
		if _, err := stmt.Exec(ts, boardType, fid, r.Code, r.Name, r.Price, r.Pct, r.Value); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func UpsertBoardDaily(db *sql.DB, tradeDate, boardType, fid string, rows []eastmoney.TopItem) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(`
		INSERT INTO board_daily(trade_date, board_type, fid, code, name, price, pct, value)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(trade_date, board_type, fid, code) DO UPDATE SET
			name=excluded.name,
			price=excluded.price,
			pct=excluded.pct,
			value=excluded.value
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, r := range rows {
		if _, err := stmt.Exec(tradeDate, boardType, fid, r.Code, r.Name, r.Price, r.Pct, r.Value); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func UpsertMarketAggRT(db *sql.DB, tsUTC time.Time, source, fid string, value float64) error {
	_, err := db.Exec(`
		INSERT INTO market_agg_rt(ts_utc, source, fid, value)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(ts_utc, source, fid) DO UPDATE SET
			value=excluded.value
	`, tsUTC.Format(time.RFC3339Nano), source, fid, value)
	return err
}

func UpsertMarketAggDaily(db *sql.DB, tradeDate, source, fid string, value float64) error {
	_, err := db.Exec(`
		INSERT INTO market_agg_daily(trade_date, source, fid, value)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(trade_date, source, fid) DO UPDATE SET
			value=excluded.value
	`, tradeDate, source, fid, value)
	return err
}

func UpsertMarginDaily(db *sql.DB, tradeDate string, row eastmoney.MarginDaily) error {
	_, err := db.Exec(`
		INSERT INTO margin_daily(
			trade_date, code, name, market,
			rzye, rzmre, rzche, rzjme, rqye, rqmcl, rqchl, rqjmg, rzrqye
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(trade_date, code) DO UPDATE SET
			name=excluded.name,
			market=excluded.market,
			rzye=excluded.rzye,
			rzmre=excluded.rzmre,
			rzche=excluded.rzche,
			rzjme=excluded.rzjme,
			rqye=excluded.rqye,
			rqmcl=excluded.rqmcl,
			rqchl=excluded.rqchl,
			rqjmg=excluded.rqjmg,
			rzrqye=excluded.rzrqye
	`, tradeDate, row.Code, row.Name, row.Market,
		row.RZYE, row.RZMRE, row.RZCHE, row.RZJME, row.RQYE, row.RQMCL, row.RQCHL, row.RQJMG, row.RZRQYE)
	return err
}
