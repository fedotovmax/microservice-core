package jwt

import "fmt"

type Config struct {
	Secret string
	Issuer string
}

func NewConfig(s, i string) (Config, error) {

	if s == "" || i == "" {
		return Config{}, fmt.Errorf("secret key and issuer cannot be empty")
	}

	return Config{Secret: s, Issuer: i}, nil
}
