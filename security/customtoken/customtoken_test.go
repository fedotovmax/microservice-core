package customtoken

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateToken(t *testing.T) {
	res, err := Create()
	require.NoError(t, err)
	require.NotEmpty(t, res.Nohashed, "token should not be empty")
	require.NotEmpty(t, res.Hashed, "hashed token should not be empty")

	require.Len(t, res.Hashed, 64)
}

func TestGenerateToken(t *testing.T) {
	tokenStr, err := Generate()
	require.NoError(t, err)
	require.NotEmpty(t, tokenStr, "generated token should not be empty")
}

func TestHashToken(t *testing.T) {
	input := "super-secret-token-123"
	hash := Hash(input)
	require.NotEmpty(t, hash)
	require.Len(t, hash, 64)
}
