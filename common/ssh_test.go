package common

// import (
// 	"regexp"
// 	"testing"

// 	"github.com/housepower/ckman/log"
// 	"github.com/housepower/ckman/model"
// 	"github.com/stretchr/testify/assert"
// )

// func TestRemoteExecute(t *testing.T) {
// 	log.InitLoggerConsole()
// 	cmd := "grep lo /proc/net/if_inet6 >/dev/null 2>&1;echo $?"
// 	sshOpts := SshOptions{
// 		User:             "root",
// 		Password:         "123456",
// 		Port:             22,
// 		Host:             "192.168.21.73",
// 		NeedSudo:         false,
// 		AuthenticateType: model.SshPasswordSave,
// 	}
// 	out, err := RemoteExecute(sshOpts, cmd)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "0", out)
// }

// func TestSSHRun(t *testing.T) {
// 	reg, err := regexp.Compile(".*@.*'s password:")
// 	assert.Nil(t, err)
// 	assert.Equal(t, reg.MatchString("eoi"), false)
// 	assert.Equal(t, reg.MatchString("eoi@192.168.110.48"), false)
// 	assert.Equal(t, reg.MatchString("eoi@192.168.110.48's"), false)
// 	assert.Equal(t, reg.MatchString("eoi@192.168.110.48's password"), false)
// 	assert.Equal(t, reg.MatchString("eoi@192.168.110.48's password:"), true)
// 	assert.Equal(t, reg.MatchString("eoi@192.168.110.48's password: "), true)
// }
