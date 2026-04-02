package network

import (
	"fmt"
	"strings"
)

func Hostname(host string) error {
	normalized := strings.ToLower(strings.TrimSuffix(host, "."))

	if len(normalized) == 0 {
		return fmt.Errorf("hostname is empty")
	}

	if len(normalized) > 253 {
		return fmt.Errorf("hostname exceeds maximum length of 253 characters")
	}

	parts := strings.Split(normalized, ".")
	for _, part := range parts {
		l := len(part)

		if l == 0 {
			return fmt.Errorf("hostname contains empty label")
		}

		if l > 63 {
			return fmt.Errorf("hostname label exceeds maximum length of 63 characters")
		}

		if part[0] == '-' {
			return fmt.Errorf("hostname label starts with hyphen: %s", part)
		}

		if part[l-1] == '-' {
			return fmt.Errorf("hostname label ends with hyphen: %s", part)
		}

		for _, r := range part {
			if (r < 'a' || r > 'z') &&
				(r < '0' || r > '9') &&
				r != '-' {
				return fmt.Errorf("hostname label contains invalid character: %q", r)
			}
		}
	}

	return nil
}
