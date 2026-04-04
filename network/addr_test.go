package network

import (
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddr(t *testing.T) {
	// Тестируем логику, которая соединяет Hostname и Port
	tests := []struct {
		addr    string
		wantErr bool
	}{
		{"localhost:6379", false},
		{"google.com:443", false},
		{"127.0.0.1:80", false},
		{"redis:99999", true},         // Кривой порт
		{"my_host:6379", true},        // Кривой хост
		{"http://localhost:80", true}, // Наличие протокола
	}

	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			host, portStr, err := net.SplitHostPort(tt.addr)

			// Если SplitHostPort не справился (например, нет порта)
			if err != nil {
				err = Hostname(tt.addr)
			} else {
				// Если справился, проверяем оба компонента
				p, pErr := strconv.Atoi(portStr)
				if pErr != nil {
					err = pErr
				} else {
					err = Port(p)
					if err == nil {
						err = Hostname(host)
					}
				}
			}

			if tt.wantErr {
				assert.Error(t, err, "Должна быть ошибка для: %s", tt.addr)
			} else {
				assert.NoError(t, err, "Не должно быть ошибки для: %s", tt.addr)
			}
		})
	}
}
