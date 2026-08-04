package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPackTlsConf(t *testing.T) {
	origin := &TlsConf{Cert: "CERT-PEM", Key: "SECRET-KEY", SelfSigned: true}
	conf := CKManClickHouseConfig{
		Password: "123456",
		Secure:   true,
		TlsConf:  origin,
	}
	conf.Pack()

	// 私钥脱敏，证书原样返回（证书本就是公开信息）
	assert.Equal(t, "**********", conf.TlsConf.Key)
	assert.Equal(t, "CERT-PEM", conf.TlsConf.Cert)
	assert.True(t, conf.TlsConf.SelfSigned)

	// 关键：不得原地修改。local repository 下该指针与内存中的集群配置共享，
	// 原地改会把真实私钥抹成星号
	assert.Equal(t, "SECRET-KEY", origin.Key)
}

func TestPackTlsConfNil(t *testing.T) {
	conf := CKManClickHouseConfig{Password: "123456"}
	conf.Pack()
	assert.Nil(t, conf.TlsConf)
}

func TestUnPackTlsConf(t *testing.T) {
	stored := CKManClickHouseConfig{
		Password: "123456",
		TlsConf:  &TlsConf{Cert: "OLD-CERT", Key: "REAL-KEY", SelfSigned: true},
	}
	// 前端把脱敏后的星号原样提交回来
	incoming := CKManClickHouseConfig{
		Password: "******",
		TlsConf:  &TlsConf{Cert: "OLD-CERT", Key: "**********"},
	}
	incoming.UnPack(stored)
	assert.Equal(t, "REAL-KEY", incoming.TlsConf.Key)
}

func TestUnPackTlsConfNewKey(t *testing.T) {
	stored := CKManClickHouseConfig{
		TlsConf: &TlsConf{Cert: "OLD-CERT", Key: "REAL-KEY"},
	}
	// 用户传了新私钥，必须保留新值
	incoming := CKManClickHouseConfig{
		TlsConf: &TlsConf{Cert: "NEW-CERT", Key: "NEW-KEY"},
	}
	incoming.UnPack(stored)
	assert.Equal(t, "NEW-KEY", incoming.TlsConf.Key)
}

func TestUnPackTlsConfFromNil(t *testing.T) {
	// 明文集群首次切 TLS：库里没有 TlsConf，不能 panic
	stored := CKManClickHouseConfig{}
	incoming := CKManClickHouseConfig{
		TlsConf: &TlsConf{Cert: "NEW-CERT", Key: "NEW-KEY"},
	}
	incoming.UnPack(stored)
	assert.Equal(t, "NEW-KEY", incoming.TlsConf.Key)
}

// TestNormalizeSecurePorts_SecureEnabled_SwitchesDefaultPorts is the
// regression test for the port/protocol confusion bug: a real cluster was
// switched Secure=false -> true while HttpPort stayed pinned at the
// plaintext default 8123 (because the config page locked the field), so
// custom.xml ended up serving HTTPS on the port everyone assumes is
// plaintext. NormalizeSecurePorts is the backend-side safety net for this -
// it must fire regardless of whether the frontend's Secure-linked port
// switcher ran at all (direct API calls, scripted deploys, ...).
func TestNormalizeSecurePorts_SecureEnabled_SwitchesDefaultPorts(t *testing.T) {
	conf := CKManClickHouseConfig{
		Secure:   true,
		Port:     ClickHouseDefaultPort,
		HttpPort: ClickHouseDefaultHttpPort,
	}
	conf.NormalizeSecurePorts()
	assert.Equal(t, ClickHouseDefaultSecurePort, conf.Port)
	assert.Equal(t, ClickHouseDefaultSecureHttpPort, conf.HttpPort)
}

// TestNormalizeSecurePorts_SecureDisabled_SwitchesBackToPlaintextPorts is the
// mirror case: turning Secure off while the ports are still sitting at the
// TLS defaults must switch them back, so a plaintext cluster doesn't end up
// trying to speak plaintext on 9440/8443.
func TestNormalizeSecurePorts_SecureDisabled_SwitchesBackToPlaintextPorts(t *testing.T) {
	conf := CKManClickHouseConfig{
		Secure:   false,
		Port:     ClickHouseDefaultSecurePort,
		HttpPort: ClickHouseDefaultSecureHttpPort,
	}
	conf.NormalizeSecurePorts()
	assert.Equal(t, ClickHouseDefaultPort, conf.Port)
	assert.Equal(t, ClickHouseDefaultHttpPort, conf.HttpPort)
}

// TestNormalizeSecurePorts_CustomPorts_AreNeverOverwritten guards the "don't
// clobber a deliberate customization" half of the contract: a port that
// isn't sitting at either mode's well-known default (e.g. a user who put
// ClickHouse behind 9001) must survive the Secure flip untouched, in either
// direction.
func TestNormalizeSecurePorts_CustomPorts_AreNeverOverwritten(t *testing.T) {
	t.Run("secure enabled", func(t *testing.T) {
		conf := CKManClickHouseConfig{Secure: true, Port: 9001, HttpPort: 8001}
		conf.NormalizeSecurePorts()
		assert.Equal(t, 9001, conf.Port)
		assert.Equal(t, 8001, conf.HttpPort)
	})
	t.Run("secure disabled", func(t *testing.T) {
		conf := CKManClickHouseConfig{Secure: false, Port: 9001, HttpPort: 8001}
		conf.NormalizeSecurePorts()
		assert.Equal(t, 9001, conf.Port)
		assert.Equal(t, 8001, conf.HttpPort)
	})
}
