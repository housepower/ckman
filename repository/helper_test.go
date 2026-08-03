package repository

import (
	"testing"

	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
)

func TestEncodeDecodeTlsKey(t *testing.T) {
	plain := "-----BEGIN PRIVATE KEY-----\nMIIabc\n-----END PRIVATE KEY-----\n"
	conf := model.CKManClickHouseConfig{
		Password: "123456",
		Secure:   true,
		TlsConf:  &model.TlsConf{Cert: "CERT-PEM", Key: plain, SelfSigned: true},
	}

	EncodePasswd(&conf)
	assert.NotEqual(t, plain, conf.TlsConf.Key)
	// 证书是公开信息，不加密
	assert.Equal(t, "CERT-PEM", conf.TlsConf.Cert)

	DecodePasswd(&conf)
	assert.Equal(t, plain, conf.TlsConf.Key)
	assert.True(t, conf.TlsConf.SelfSigned)
}

func TestDecodeTlsKeyNotMutateShared(t *testing.T) {
	plain := "REAL-KEY"
	shared := &model.TlsConf{Cert: "CERT-PEM", Key: plain}
	conf := model.CKManClickHouseConfig{TlsConf: shared}

	EncodePasswd(&conf)
	// EncodePasswd 不得改到调用方持有的那个对象
	assert.Equal(t, plain, shared.Key)
}

func TestEncodeDecodeTlsConfNil(t *testing.T) {
	conf := model.CKManClickHouseConfig{Password: "123456"}
	EncodePasswd(&conf)
	assert.Nil(t, conf.TlsConf)
	DecodePasswd(&conf)
	assert.Nil(t, conf.TlsConf)
}
