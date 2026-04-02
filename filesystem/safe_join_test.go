package filesystem

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSafeJoin(t *testing.T) {
	tests := []struct {
		name    string
		base    string
		parts   []string
		wantErr error
	}{
		{"safe join", "/app/data", []string{"file.txt"}, nil},
		{"safe nested", "/app/data", []string{"dir", "file.txt"}, nil},
		{"traversal attack", "/app/data", []string{"..", "secret.txt"}, ErrPathTraversal},
		{"exact base", "/app/data", []string{}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := SafeJoin(tt.base, tt.parts...)
			if tt.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}
