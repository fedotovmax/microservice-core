package filesystem

import (
	"errors"
	"path/filepath"
	"strings"
)

var (
	ErrEmptyFilepath       = errors.New("file path is empty")
	ErrInvalidFilepath     = errors.New("invalid file path")
	ErrFilepathNotAbsolute = errors.New("file path is not absolute")
	ErrFilepathNotLocal    = errors.New("file path is not local")
)

// Filepath нормализует и валидирует базовые вещи
func Filepath(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", ErrEmptyFilepath
	}

	p := filepath.Clean(value)

	// "." — это текущая директория, обычно не то, что хотят
	if p == "." {
		return "", ErrInvalidFilepath
	}

	return p, nil
}

// FilepathAbsolute — только абсолютные пути
func FilepathAbsolute(value string) (string, error) {
	p, err := Filepath(value)
	if err != nil {
		return "", err
	}

	if !filepath.IsAbs(p) {
		return "", ErrFilepathNotAbsolute
	}

	return p, nil
}

// FilepathLocal — только локальные (без выхода наружу)
func FilepathLocal(value string) (string, error) {
	p, err := Filepath(value)
	if err != nil {
		return "", err
	}

	if !filepath.IsLocal(p) {
		return "", ErrFilepathNotLocal
	}

	return p, nil
}
