package symbol

import (
	"fmt"
	"strings"
)

// CodeOnly extracts 6-digit-ish code from "600519.SH" => "600519".
func CodeOnly(sym string) (string, error) {
	sym = strings.TrimSpace(sym)
	if sym == "" {
		return "", fmt.Errorf("empty symbol")
	}
	parts := strings.Split(sym, ".")
	if len(parts) == 1 {
		return parts[0], nil
	}
	if len(parts) == 2 && parts[0] != "" {
		return parts[0], nil
	}
	return "", fmt.Errorf("invalid symbol: %q", sym)
}

// ToEastmoneySecID maps:
// - SH: "600519.SH" => "1.600519"
// - SZ: "000001.SZ" => "0.000001"
// - BJ: "920152.BJ" => "0.920152" (Eastmoney uses 0.* for many non-SH markets; this works for toplist and ulist in practice)
func ToEastmoneySecID(sym string) (string, error) {
	sym = strings.TrimSpace(sym)
	if sym == "" {
		return "", fmt.Errorf("empty symbol")
	}
	parts := strings.Split(sym, ".")
	if len(parts) != 2 {
		return "", fmt.Errorf("symbol must be like 600519.SH / 000001.SZ: %q", sym)
	}
	code := parts[0]
	sfx := strings.ToUpper(parts[1])
	switch sfx {
	case "SH":
		return "1." + code, nil
	case "SZ", "BJ":
		return "0." + code, nil
	default:
		return "", fmt.Errorf("unknown market suffix: %q", sfx)
	}
}

func ToEastmoneySecIDs(symbols []string) ([]string, error) {
	out := make([]string, 0, len(symbols))
	for _, s := range symbols {
		secid, err := ToEastmoneySecID(s)
		if err != nil {
			return nil, err
		}
		out = append(out, secid)
	}
	return out, nil
}

