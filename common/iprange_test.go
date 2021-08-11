package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseIPRange(t *testing.T) {
	ret1, err := ParseIPRange("192.168.1.15-192.168.1.22")
	assert.Nil(t, err)
	expect1 := []string{"192.168.1.15", "192.168.1.16","192.168.1.17","192.168.1.18","192.168.1.19","192.168.1.20", "192.168.1.21", "192.168.1.22"}
	assert.Equal(t, ret1, expect1)
	ret2, err := ParseIPRange("192.168.21.146")
	assert.Nil(t, err)
	assert.Equal(t, ret2, []string{"192.168.21.146"})
	ret3, err := ParseIPRange("192.168.1.0/31")
	assert.Nil(t, err)
	assert.Equal(t, ret3, []string{"192.168.1.0", "192.168.1.1"})
}
