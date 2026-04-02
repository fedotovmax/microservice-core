package network

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHostname(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid simple", "example.com", false},
		{"valid subdomain", "mail.example.com", false},
		{"uppercase domain", "EXAMPLE.COM", false},

		{"empty string", "", true},
		{"hostname too long", "a" + string(make([]byte, 253)), true},
		{"label empty", "example..com", true},
		{"label too long", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.com", true}, // 64 символа
		{"label starts with hyphen", "-abc.com", true},
		{"label ends with hyphen", "abc-.com", true},
		{"invalid char", "exa$mple.com", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Hostname(tt.input)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
