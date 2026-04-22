package mail

import (
	"fmt"
	"net/mail"
	"unicode/utf8"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Host        string `envconfig:"SMTP_HOST" required:"true"`
	Port        int    `envconfig:"SMTP_PORT" required:"true"`
	Secret      string `envconfig:"SMTP_SECRET" required:"true"`
	Sender      string `envconfig:"SMTP_SENDER" required:"true"`
	DisplayName string `envconfig:"SMTP_DISPLAY_NAME" required:"true"`
}

func (c Config) Validate() error {

	const op = "core.network.mail.Config.Validate"

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

func NewConfig() (Config, error) {

	const op = "core.network.mail.NewConfig"

	var config Config

	err := envconfig.Process("SMTP_", &config)

	if err != nil {
		return Config{}, fmt.Errorf("%s: error when parse mail env variables: %w", op, err)
	}

	return config, nil

}

func NewConfigMust() Config {

	config, err := NewConfig()

	if err != nil {
		panic(err)
	}

	return config
}
