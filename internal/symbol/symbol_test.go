package symbol

import "testing"

func TestToEastmoneySecIDFromCode(t *testing.T) {
	cases := []struct {
		code string
		want string
	}{
		{"600519", "1.600519"},
		{"000001", "0.000001"},
		{"300750", "0.300750"},
		{"920152", "0.920152"},
	}
	for _, tc := range cases {
		got, err := ToEastmoneySecIDFromCode(tc.code)
		if err != nil {
			t.Fatalf("code=%s: %v", tc.code, err)
		}
		if got != tc.want {
			t.Fatalf("code=%s: got=%s want=%s", tc.code, got, tc.want)
		}
	}
}

