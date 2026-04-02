package filesystem

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilepath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{"valid simple", "file.txt", nil},
		{"valid nested", "dir/file.txt", nil},
		{"trim spaces", "  file.txt  ", nil},
		{"empty", "", ErrEmptyFilepath},
		{"dot only", ".", ErrInvalidFilepath},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Filepath(tt.input)
			if tt.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestFilepathAbsolute(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{"absolute", "/usr/bin", nil},
		{"not absolute", "file.txt", ErrFilepathNotAbsolute},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := FilepathAbsolute(tt.input)
			if tt.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestFilepathLocal(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{"local", "file.txt", nil},
		{"nested local", "dir/file.txt", nil},
		{"traversal", "../file.txt", ErrFilepathNotLocal},
		{"absolute", "/etc/passwd", ErrFilepathNotLocal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := FilepathLocal(tt.input)
			if tt.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}
