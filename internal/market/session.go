package market

import "time"

// IsCNTradingTime checks A-share continuous auction sessions in Asia/Shanghai.
// This ignores holidays; for a free MVP we at least gate by weekday and session hours.
func IsCNTradingTime(t time.Time) bool {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err == nil {
		t = t.In(loc)
	}

	wd := t.Weekday()
	if wd == time.Saturday || wd == time.Sunday {
		return false
	}

	hm := t.Hour()*60 + t.Minute()
	// 09:30-11:30 and 13:00-15:00
	if hm >= 9*60+30 && hm <= 11*60+30 {
		return true
	}
	if hm >= 13*60 && hm <= 15*60 {
		return true
	}
	return false
}

