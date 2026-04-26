package password

import "errors"

var (
	ErrArgonInvalidHash         = errors.New("the encoded hash is not in the correct format")
	ErrArgonIncompatibleVersion = errors.New("incompatible version of argon2")
	ErrInvalidPassword          = errors.New("invalid password")
)
