package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	PasswordDecFake = "Ckman@123#?456!"
	PasswordEncFake = "815823a203f10827167ca76c558b94d2"
)

func TestDesDecrypt(t *testing.T) {
	assert.Equal(t, DesDecrypt(PasswordEncFake), PasswordDecFake)
}

func TestDesEncrypt(t *testing.T) {
	assert.Equal(t, DesEncrypt(PasswordDecFake), PasswordEncFake)
}
