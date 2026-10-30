package common

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

const (
	ADMIN    string = "admin"
	GUEST    string = "guest"
	ORDINARY string = "ordinary"

	DefaultAdminName     = "ckman"
	InternalOrdinaryName = "ordinary"
)

// GenerateInitialAdminPassword creates a one-time 24-character password with
// at least one character from each required category.
func GenerateInitialAdminPassword() (string, error) {
	const lower = "abcdefghjkmnpqrstuvwxyz"
	const upper = "ABCDEFGHJKMNPQRSTUVWXYZ"
	const digits = "23456789"
	const special = "!@#$%*-_+"
	const all = lower + upper + digits + special
	password := make([]byte, 24)
	for i, alphabet := range []string{lower, upper, digits, special} {
		index, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			return "", fmt.Errorf("generate initial admin password: %w", err)
		}
		password[i] = alphabet[index.Int64()]
	}
	for i := 4; i < len(password); i++ {
		index, err := rand.Int(rand.Reader, big.NewInt(int64(len(all))))
		if err != nil {
			return "", fmt.Errorf("generate initial admin password: %w", err)
		}
		password[i] = all[index.Int64()]
	}
	for i := len(password) - 1; i > 0; i-- {
		index, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			return "", fmt.Errorf("shuffle initial admin password: %w", err)
		}
		password[i], password[index.Int64()] = password[index.Int64()], password[i]
	}
	return string(password), nil
}
