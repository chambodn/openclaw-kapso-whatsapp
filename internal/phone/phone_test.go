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

func TestEnsurePlus(t *testing.T) {
	tests := []struct{ in, want string }{
		{"15551234567", "+15551234567"},
		{"+15551234567", "+15551234567"},
		{"", ""},
		{"+", "+"},
	}
	for _, tt := range tests {
		if got := EnsurePlus(tt.in); got != tt.want {
			t.Errorf("EnsurePlus(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
