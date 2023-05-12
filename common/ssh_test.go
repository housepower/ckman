package common

import (
	"regexp"
	"testing"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
)

func TestRemoteExecute(t *testing.T) {
	log.InitLoggerConsole()
	cmd := "grep lo /proc/net/if_inet6 >/dev/null 2>&1;echo $?"
	sshOpts := SshOptions{
		User:             "eoi",
		Password:         "starcraft",
		Port:             22,
		Host:             "localhost",
		NeedSudo:         false,
		AuthenticateType: model.SshPasswordSave,
	}
	out, err := RemoteExecute(sshOpts, cmd)
	assert.Nil(t, err)
	assert.Equal(t, "0", out)
}

func TestSSHRun(t *testing.T) {
	reg, err := regexp.Compile(".*@.*'s password:")
	assert.Nil(t, err)
	assert.Equal(t, reg.MatchString("eoi"), false)
	assert.Equal(t, reg.MatchString("eoi@192.168.110.48"), false)
	assert.Equal(t, reg.MatchString("eoi@192.168.110.48's"), false)
	assert.Equal(t, reg.MatchString("eoi@192.168.110.48's password"), false)
	assert.Equal(t, reg.MatchString("eoi@192.168.110.48's password:"), true)
	assert.Equal(t, reg.MatchString("eoi@192.168.110.48's password: "), true)
}

func TestSftp(t *testing.T) {
	log.InitLoggerConsole()
	sshOpts := SshOptions{
		User:             "eoi",
		Password:         "starcraft",
		Port:             22,
		Host:             "localhost",
		NeedSudo:         false,
		AuthenticateType: model.SshPasswordSave,
	}
	c, sc, err := ScpConnect(sshOpts)
	assert.Nil(t, err)
	defer sc.Close()
	defer c.Close()
	err = ScpUploadFiles([]string{"ssh.go"}, "/home/eoi", sshOpts)
	assert.Nil(t, err)
	_, err = RemoteExecute(sshOpts, "rm -rf ~/ssh.go")
	assert.Nil(t, err)
}
