package mail

import (
	"fmt"
	"net/mail"
	"unicode/utf8"
)

type Config struct {
	Host        string
	Port        int
	Secret      string
	Sender      string
	DisplayName string
}

func (c Config) Validate() error {

	const op = "core.transport.smtp.mail.Config.Validate"

	if c.Host == "" {
		return fmt.Errorf("%s: hostname cannot be empty", op)
	}

	if c.Port <= 0 {
		return fmt.Errorf("%s: port must be greater than 0", op)
	}

	_, err := mail.ParseAddress(c.Sender)

	if err != nil {
		return fmt.Errorf("%s: invalid sender address: %w", op, err)
	}

	if utf8.RuneCountInString(c.Secret) < 5 {
		return fmt.Errorf("%s: smtp secret/password must be at least 5 characters", op)
	}

	if utf8.RuneCountInString(c.DisplayName) == 0 {
		return fmt.Errorf("%s: dispay name must be at least 1 character", op)
	}

	return nil

}
