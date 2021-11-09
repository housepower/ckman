package common

import (
	"github.com/housepower/ckman/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRemoteExecute(t *testing.T) {
	log.InitLoggerConsole()
	cmd := "grep lo /proc/net/if_inet6 >/dev/null 2>&1;echo $?"
	out, err := RemoteExecute("root", "123456", "192.168.21.73", 22, cmd)
	assert.Nil(t, err)
	assert.Equal(t, "0", out)
}
