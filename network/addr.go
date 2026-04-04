package network

import (
	"net"
	"net/url"
	"strconv"
)

func Addr(addr string) error {
	// 1. Пытаемся распарсить как полноценный URL
	u, err := url.Parse(addr)

	hostToValidate := ""
	portToValidate := ""

	if err == nil && u.Scheme != "" {
		// Это URL (есть http:// или https://)
		hostToValidate = u.Hostname()
		portToValidate = u.Port()

		// Если порта в URL нет (например, http://google.com),
		// ставим дефолтный в зависимости от протокола (опционально)
		if portToValidate == "" {
			if u.Scheme == "http" {
				portToValidate = "80"
			}
			if u.Scheme == "https" {
				portToValidate = "443"
			}
		}
	} else {
		// Это не URL, пробуем как обычный host:port
		h, p, err := net.SplitHostPort(addr)
		if err != nil {
			// Возможно, это просто "localhost" без порта
			hostToValidate = addr
		} else {
			hostToValidate = h
			portToValidate = p
		}
	}

	// 2. Валидируем хост
	if err := Hostname(hostToValidate); err != nil {
		return err
	}

	// 3. Валидируем порт (если он есть)
	if portToValidate != "" {
		p, _ := strconv.Atoi(portToValidate)
		if err := Port(p); err != nil {
			return err
		}
	}

	return nil
}
