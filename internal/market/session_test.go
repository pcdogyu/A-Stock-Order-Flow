package market

import (
	"testing"
	"time"
)

func TestIsCNTradingTime(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	// Monday 10:00 should be trading time.
	in := time.Date(2026, 2, 2, 10, 0, 0, 0, loc)
	if !IsCNTradingTime(in) {
		t.Fatalf("expected trading time")
	}
	// Lunch break.
	in = time.Date(2026, 2, 2, 12, 0, 0, 0, loc)
	if IsCNTradingTime(in) {
		t.Fatalf("expected not trading time")
	}
	// Weekend.
	in = time.Date(2026, 2, 1, 10, 0, 0, 0, loc)
	if IsCNTradingTime(in) {
		t.Fatalf("expected not trading time on weekend")
	}
}

