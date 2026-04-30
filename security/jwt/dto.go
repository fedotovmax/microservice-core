package jwt

import "time"

type Verified[T any] struct {
	Payload   T
	Claims    Claims
	ExpiresAt time.Time
	IssuedAt  time.Time
}

type Claims struct {
	Subject string
	ID      string
}
