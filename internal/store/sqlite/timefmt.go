package sqlite

import "time"

// fixedRFC3339Nano is a fixed-width RFC3339 with 9-digit nanoseconds.
// This makes TEXT ordering match time ordering.
func fixedRFC3339Nano(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z07:00")
}

