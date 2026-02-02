//go:build !windows

package eastmoney

import (
	"context"
	"fmt"
)

func getJSONViaPowerShell(ctx context.Context, u string, out any) error {
	_ = ctx
	_ = u
	_ = out
	return fmt.Errorf("powershell fallback only supported on windows")
}

