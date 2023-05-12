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

func TestAes(t *testing.T) {
	assert.Equal(t, "9E0D1254D6C31AAFEEF413197471BC16", AesEncryptECB("Eoi123456!"))
	assert.Equal(t, "Eoi123456!", AesDecryptECB("9E0D1254D6C31AAFEEF413197471BC16"))
	assert.Equal(t, "", AesEncryptECB(""))
	assert.Equal(t, "", AesDecryptECB(""))
}
