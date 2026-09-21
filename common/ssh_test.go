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

func TestGenFinalScriptSkipsEmptySegments(t *testing.T) {
	// 空片段不能拼成 "sudo "：sudo 不带参数只会打印 usage 并以 1 退出
	shell := genFinalScript("ckman", "ls;;mv a b;", true)
	assert.Equal(t, "echo 'i love china'; export LANG=en_US.UTF-8; sudo ls;sudo mv a b", shell)
}

func TestRemoteExecuteEmptyCmdIsNoop(t *testing.T) {
	// host 不可达，一旦真发起 SSH 就会返回错误
	out, err := RemoteExecute(SshOptions{
		User:             "ckman",
		Port:             22,
		Host:             "192.0.2.1",
		NeedSudo:         true,
		AuthenticateType: model.SshPasswordSave,
	}, "   ")
	assert.Nil(t, err)
	assert.Equal(t, "", out)
}
