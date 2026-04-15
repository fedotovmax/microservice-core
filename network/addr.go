package network

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func Addr(addr string) error {
	// Если в строке есть "://", это URL, а нам нужен чистый host:port
	if strings.Contains(addr, "://") {
		return fmt.Errorf("protocol (scheme) is not allowed: %s", addr)
	}

	// Пробуем разделить на хост и порт
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		// Если порта нет (например, просто "localhost"), валидируем только хост
		if err := Hostname(addr); err != nil {
			return fmt.Errorf("invalid hostname: %w", err)
		}
		return nil
	}

	// Если порт есть, валидируем его
	p, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid port format: %w", err)
	}
	if err := Port(p); err != nil {
		return err
	}

	// И валидируем сам хост
	if err := Hostname(host); err != nil {
		return fmt.Errorf("invalid hostname: %w", err)
	}

	return nil
}
