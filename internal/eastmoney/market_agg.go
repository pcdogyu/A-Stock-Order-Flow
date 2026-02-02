package eastmoney

import (
	"context"
	"fmt"
	"sync"
)

// AllStocksSum pages through the A-share universe (fs) and returns sum(fid) across all stocks.
// This uses the same clist endpoint and is intentionally rate-limited by caller (interval_seconds).
func (c *Client) AllStocksSum(ctx context.Context, fs, fid string, concurrency int) (sum float64, total int, err error) {
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > 10 {
		concurrency = 10
	}

	const pageSize = 100 // API appears capped at 100 regardless of pz.

	total, items, err := c.clistPage(ctx, fs, fid, 1, pageSize)
	if err != nil {
		return 0, 0, err
	}
	var s float64
	for _, it := range items {
		s += it.Value
	}
	if total <= len(items) {
		return s, total, nil
	}

	pages := (total + pageSize - 1) / pageSize
	type res struct {
		sum float64
		err error
	}

	sem := make(chan struct{}, concurrency)
	out := make(chan res, pages-1)
	var wg sync.WaitGroup

	for pn := 2; pn <= pages; pn++ {
		pn := pn
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				out <- res{err: ctx.Err()}
				return
			}
			defer func() { <-sem }()

			_, items, err := c.clistPage(ctx, fs, fid, pn, pageSize)
			if err != nil {
				out <- res{err: err}
				return
			}
			var local float64
			for _, it := range items {
				local += it.Value
			}
			out <- res{sum: local}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	for r := range out {
		if r.err != nil {
			return 0, total, fmt.Errorf("allstocks page error: %w", r.err)
		}
		s += r.sum
	}
	return s, total, nil
}

