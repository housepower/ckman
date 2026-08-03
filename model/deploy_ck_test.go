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
