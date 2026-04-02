package filesystem

import (
	"errors"
	"path/filepath"
	"strings"
)

var (
	ErrPathTraversal = errors.New("file path contains traversal")
)

// SafeJoin безопасно соединяет базовую директорию и пользовательские сегменты
// и защищает от выхода за пределы base
func SafeJoin(base string, parts ...string) (string, error) {
	base = filepath.Clean(base)

	all := append([]string{base}, parts...)
	full := filepath.Join(all...)
	full = filepath.Clean(full)

	// безопасная проверка выхода за пределы base
	if !strings.HasPrefix(full, base+string(filepath.Separator)) && full != base {
		return "", ErrPathTraversal
	}

	return full, nil
}
