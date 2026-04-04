package jwt

import "errors"

var ErrInvalidToken = errors.New("invalid token")

var ErrParseClaims = errors.New("error when parse claims")
