package jwt

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type tokenManager[T any] struct {
	config *Config
}

type claimsWrapper[T any] struct {
	jwt.RegisteredClaims
	Data T `json:"dat"`
}

func New[T any](config *Config) *tokenManager[T] {
	return &tokenManager[T]{config: config}
}

func (m *tokenManager[T]) Create(cl Claims, payload T, ttl time.Duration) (string, error) {
	const op = "core.security.jwt.tokenManager.Create"

	now := time.Now()

	claims := claimsWrapper[T]{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   cl.Subject,
			ID:        cl.ID,
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
			IssuedAt:  jwt.NewNumericDate(now),
			Issuer:    m.config.Issuer,
		},
		Data: payload,
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(m.config.Secret))

	if err != nil {
		return "", fmt.Errorf("%s: %w", op, err)
	}

	return token, nil
}

func (m *tokenManager[T]) Verify(tokenStr string) (Verified[T], error) {

	const op = "core.security.jwt.tokenManager.Verify"

	opts := []jwt.ParserOption{
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithIssuer(m.config.Issuer),
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
	}

	var wrapper claimsWrapper[T]

	token, err := jwt.ParseWithClaims(tokenStr, &wrapper, func(t *jwt.Token) (any, error) {
		return []byte(m.config.Secret), nil
	}, opts...)

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return Verified[T]{}, fmt.Errorf("%s: %w: %v", op, ErrTokenExpired, err)
		}

		if errors.Is(err, jwt.ErrSignatureInvalid) {
			return Verified[T]{}, fmt.Errorf("%s: %w: %v", op, ErrInvalidToken, err)
		}

		return Verified[T]{}, fmt.Errorf("%s: %w: %v", op, ErrParseClaims, err)
	}

	if !token.Valid {
		return Verified[T]{}, fmt.Errorf("%s: %w", op, ErrInvalidToken)
	}

	return Verified[T]{
		Payload:   wrapper.Data,
		Claims:    Claims{ID: wrapper.ID, Subject: wrapper.Subject},
		ExpiresAt: wrapper.ExpiresAt.Time,
		IssuedAt:  wrapper.IssuedAt.Time,
	}, nil
}
