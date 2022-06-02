package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	PasswordDecFake = "Ckman@123#?456!"
	PasswordEncFake = "8CBB2808F6E36F461DDED91F509A7FFB"
)

func TestAesDecryptECB(t *testing.T) {
	assert.Equal(t, AesDecryptECB(PasswordEncFake), PasswordDecFake)
}

func TestAesEncryptECB(t *testing.T) {
	assert.Equal(t, AesEncryptECB(PasswordDecFake), PasswordEncFake)
}
