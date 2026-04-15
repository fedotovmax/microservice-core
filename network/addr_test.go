package network

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddr(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		wantErr bool
	}{
		{
			name:    "Valid host and port",
			addr:    "localhost:6379",
			wantErr: false,
		},
		{
			name:    "Valid IP and port",
			addr:    "127.0.0.1:80",
			wantErr: false,
		},
		{
			name:    "Valid hostname only",
			addr:    "redis-server",
			wantErr: false,
		},
		{
			name:    "Invalid port (too high)",
			addr:    "localhost:70000",
			wantErr: true,
		},
		{
			name:    "Invalid port (not a number)",
			addr:    "localhost:abc",
			wantErr: true,
		},
		{
			name:    "Invalid hostname format",
			addr:    "invalid_host:6379", // если Hostname запрещает подчеркивания
			wantErr: true,
		},
		{
			name:    "Protocol not allowed (http)",
			addr:    "http://localhost:80",
			wantErr: true,
		},
		{
			name:    "Protocol not allowed (redis)",
			addr:    "redis://localhost:6379",
			wantErr: true,
		},
		{
			name:    "Empty address",
			addr:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Addr(tt.addr)
			if tt.wantErr {
				assert.Error(t, err, "Should return error for addr: %s", tt.addr)
			} else {
				assert.NoError(t, err, "Should not return error for addr: %s", tt.addr)
			}
		})
	}
}
