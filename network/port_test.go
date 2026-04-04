package network

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPort(t *testing.T) {
	tests := []struct {
		port    int
		wantErr bool
	}{
		{80, false},
		{6379, false},
		{65535, false},
		{0, true},
		{65536, true},
		{-1, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Port:%d", tt.port), func(t *testing.T) {
			err := Port(tt.port)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
