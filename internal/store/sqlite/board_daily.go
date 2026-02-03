package sqlite

import "database/sql"

type BoardDailySeriesPoint struct {
	TradeDate string
	Code      string
	Name      string
	Value     float64
	Price     float64
	Pct       float64
}

type BoardDailyPoint struct {
	TradeDate string  `json:"trade_date"`
	Value     float64 `json:"value"`
}

func UpsertBoardDailySeries(db *sql.DB, boardType, fid string, points []BoardDailySeriesPoint) error {
	if len(points) == 0 {
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

	for _, p := range points {
		if _, err := stmt.Exec(p.TradeDate, boardType, fid, p.Code, p.Name, p.Price, p.Pct, p.Value); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func QueryBoardDailyByCode(db *sql.DB, boardType, fid, code string, limit int) ([]BoardDailyPoint, string, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := db.Query(`
		SELECT trade_date, value, name
		FROM board_daily
		WHERE board_type = ? AND fid = ? AND code = ?
		ORDER BY trade_date DESC
		LIMIT ?
	`, boardType, fid, code, limit)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	out := make([]BoardDailyPoint, 0, limit)
	name := ""
	for rows.Next() {
		var p BoardDailyPoint
		var nm sql.NullString
		if err := rows.Scan(&p.TradeDate, &p.Value, &nm); err != nil {
			return nil, "", err
		}
		if nm.Valid && name == "" {
			name = nm.String
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	reverse(out)
	return out, name, nil
}
