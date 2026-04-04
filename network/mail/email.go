package mail

import (
	"fmt"
	"net/mail"
	"strings"

	"github.com/fedotovmax/microservice-core/network"
)

func Email(addr string) error {
	a, err := mail.ParseAddress(addr)
	if err != nil {
		return fmt.Errorf("invalid email format: %w", err)
	}

	parts := strings.SplitN(a.Address, "@", 2)

	if len(parts[0]) > 64 {
		return fmt.Errorf("email local part too long: %s", parts[0])
	}

	if len(a.Address) > 254 {
		return fmt.Errorf("email too long: %s", a.Address)
	}

	if err := network.Hostname(parts[1]); err != nil {
		return fmt.Errorf("invalid hostname in email: %w", err)
	}

	return nil
}
