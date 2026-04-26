package password

import "golang.org/x/crypto/bcrypt"

const BcryptDefaultCost = bcrypt.DefaultCost

func Bcrypt(p string, cost int) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(p), cost)

	if err != nil {
		return "", err
	}

	return string(b), nil
}

func BcryptCompare(p, h string) error {
	err := bcrypt.CompareHashAndPassword([]byte(h), []byte(p))

	if err != nil {
		return ErrInvalidPassword
	}

	return nil
}
