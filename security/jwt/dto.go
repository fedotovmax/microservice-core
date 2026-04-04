package jwt

import "time"

type CreateParams struct {
	Uid            string
	Sid            string
	TokenExpiresAt time.Time
	Now            time.Time
}

type Verified struct {
	Uid string
	Sid string
}
