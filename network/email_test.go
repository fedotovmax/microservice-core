package network

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEmail(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// валидные email
		{"valid simple", "user@example.com", false},
		{"valid uppercase domain", "user@EXAMPLE.COM", false},
		{"valid with subdomain", "user@mail.example.com", false},
		{"valid with display name", "John Doe <john@example.com>", false},

		// ошибки формата
		{"missing at", "userexample.com", true},
		{"missing domain", "user@", true},
		{"empty string", "", true},

		// слишком длинные
		{"too long email", strings.Repeat("a", 245) + "@example.com", true},
		{"local part too long", strings.Repeat("a", 65) + "@example.com", true},

		// hostname ошибки
		{"hostname starts with dash", "user@-example.com", true},
		{"hostname empty label", "user@..com", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Email(tt.input)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
