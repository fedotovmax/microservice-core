package jwt

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

type tokenManager struct {
	config Config
}

func New(config Config) *tokenManager {
	return &tokenManager{config: config}
}

func (m *tokenManager) Create(p CreateParams) (token string, err error) {

	const op = "core.security.jwt.TokenManager.Create"

	accessClaims := jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(p.TokenExpiresAt),
		IssuedAt:  jwt.NewNumericDate(p.Now),
		Issuer:    m.config.Issuer,
		ID:        p.Sid,
		Subject:   p.Uid,
	}

	accessTokenObject := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims)

	token, err = accessTokenObject.SignedString([]byte(m.config.Secret))

	if err != nil {

		err = fmt.Errorf("%s: %w", op, err)

		return "", err
	}

	return token, nil
}

func (m *tokenManager) Verify(token string) (Verified, error) {

	const op = "core.security.jwt.TokenManager.Verify"

	opts := []jwt.ParserOption{
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithIssuer(m.config.Issuer),
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
	}

	result, err := jwt.ParseWithClaims(token, &jwt.RegisteredClaims{}, func(t *jwt.Token) (any, error) {
		return []byte(m.config.Secret), nil
	}, opts...)

	if err != nil {
		return Verified{}, fmt.Errorf("%s: %w: %v", op, ErrParseClaims, err)
	}

	if !result.Valid {
		return Verified{}, fmt.Errorf("%s: %w", op, ErrInvalidToken)
	}

	claims, ok := result.Claims.(*jwt.RegisteredClaims)

	if !ok {
		return Verified{}, fmt.Errorf("%s: %w", op, ErrParseClaims)
	}

	if claims.Subject == "" || claims.ID == "" {
		return Verified{}, fmt.Errorf("%s: %w", op, ErrInvalidToken)
	}

	return Verified{Uid: claims.Subject, Sid: claims.ID}, nil
}
