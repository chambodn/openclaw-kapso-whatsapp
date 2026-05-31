package phone

import "testing"

func TestNormalize(t *testing.T) {
	tests := []struct{ in, want string }{
		{"+1 (234) 567-890", "1234567890"},
		{"1234567890", "1234567890"},
		{"+1234567890", "1234567890"},
		{"15551234567", "15551234567"},
		{"+15551234567", "15551234567"},
		{"", ""},
		{"+++", ""},
	}
	for _, tt := range tests {
		if got := Normalize(tt.in); got != tt.want {
			t.Errorf("Normalize(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
