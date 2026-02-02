//go:build windows

package eastmoney

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

func getJSONViaPowerShell(ctx context.Context, u string, out any) error {
	u = strings.ReplaceAll(u, "'", "''")
	// Force UTF-8 stdout so we can json.Unmarshal the raw payload.
	script := fmt.Sprintf("[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false; $u='%s'; (Invoke-WebRequest -Uri $u -Headers @{ 'User-Agent'='Mozilla/5.0' } -TimeoutSec 20).Content", u)

	// Prefer pwsh (PowerShell 7), fall back to Windows PowerShell.
	for _, exe := range []string{"pwsh", "powershell"} {
		cmd := exec.CommandContext(ctx, exe, "-NoProfile", "-Command", script)
		b, err := cmd.Output()
		if err != nil {
			// If the executable isn't found, try next.
			var ee *exec.Error
			if strings.Contains(err.Error(), "executable file not found") || (errors.As(err, &ee) && errors.Is(ee.Err, exec.ErrNotFound)) {
				continue
			}
			// cmd.Output() loses stderr; re-run to get combined output for diagnostics.
			cmd2 := exec.CommandContext(ctx, exe, "-NoProfile", "-Command", script)
			combined, _ := cmd2.CombinedOutput()
			return fmt.Errorf("%s: %v: %s", exe, err, strings.TrimSpace(string(combined)))
		}
		if err := json.Unmarshal(b, out); err != nil {
			// If the shell still outputs UTF-16 for some reason, try to detect and convert.
			if len(b) >= 2 && b[0] == 0xFF && b[1] == 0xFE {
				// UTF-16LE BOM; conversion omitted to keep dependencies low.
				return fmt.Errorf("%s json decode (utf-16le BOM): %w", exe, err)
			}
			return fmt.Errorf("%s json decode: %w", exe, err)
		}
		return nil
	}
	return fmt.Errorf("pwsh/powershell not available for fallback request")
}
