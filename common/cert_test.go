package common

import (
	"crypto/tls"
	"crypto/x509"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGenSelfSignedCert(t *testing.T) {
	hosts := []string{"192.168.0.1", "192.168.0.2", "ck01"}
	certPEM, keyPEM, err := GenSelfSignedCert(hosts)
	assert.Nil(t, err)
	assert.True(t, strings.HasPrefix(certPEM, "-----BEGIN CERTIFICATE-----"))
	assert.Contains(t, keyPEM, "PRIVATE KEY")

	// 公私钥必须匹配
	_, err = tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	assert.Nil(t, err)

	cert, err := parseCert(certPEM)
	assert.Nil(t, err)

	// SAN 覆盖全部节点 IP + localhost + 127.0.0.1
	var ips []string
	for _, ip := range cert.IPAddresses {
		ips = append(ips, ip.String())
	}
	assert.Contains(t, ips, "192.168.0.1")
	assert.Contains(t, ips, "192.168.0.2")
	assert.Contains(t, ips, "127.0.0.1")
	assert.Contains(t, cert.DNSNames, "ck01")
	assert.Contains(t, cert.DNSNames, "localhost")

	// 分布式查询要求本节点也能作为客户端，两种用途都得有
	assert.Contains(t, cert.ExtKeyUsage, x509.ExtKeyUsageServerAuth)
	assert.Contains(t, cert.ExtKeyUsage, x509.ExtKeyUsageClientAuth)

	// NotBefore 提前 1 小时容忍时钟偏移，有效期 10 年
	assert.True(t, cert.NotBefore.Before(time.Now()))
	assert.True(t, cert.NotAfter.After(time.Now().AddDate(9, 11, 0)))
}

func TestGenSelfSignedCertNotMutateInput(t *testing.T) {
	// backing 长度 3、hosts 只取前 1 个元素，但底层数组容量足够容纳
	// 内部要追加的 "localhost"、"127.0.0.1" 两个元素（cap(hosts) == 3）。
	// 若实现用 append(hosts, "localhost", "127.0.0.1")，Go 会原地复用
	// 这段底层数组而不重新分配，从而静默覆盖 backing[1]/backing[2]
	// 处的哨兵值 —— 这正是调用方传入「有富余容量的切片」（例如
	// allNodes[:n]）时会被写穿的真实场景。
	backing := []string{"192.168.0.1", "SENTINEL-1", "SENTINEL-2"}
	hosts := backing[:1]
	assert.Equal(t, 3, cap(hosts))

	_, _, err := GenSelfSignedCert(hosts)
	assert.Nil(t, err)

	// hosts 变量本身（长度 1 的视图）内容不变
	assert.Equal(t, []string{"192.168.0.1"}, hosts)
	// 容量范围内、长度之外的哨兵值不得被覆盖
	assert.Equal(t, "SENTINEL-1", backing[1])
	assert.Equal(t, "SENTINEL-2", backing[2])
}

func TestValidateCertKeyPair(t *testing.T) {
	certPEM, keyPEM, err := GenSelfSignedCert([]string{"192.168.0.1"})
	assert.Nil(t, err)
	assert.Nil(t, ValidateCertKeyPair(certPEM, keyPEM))
}

func TestValidateCertKeyPairEmpty(t *testing.T) {
	err := ValidateCertKeyPair("", "")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "must not be empty")
}

func TestValidateCertKeyPairMismatch(t *testing.T) {
	certPEM, _, err := GenSelfSignedCert([]string{"192.168.0.1"})
	assert.Nil(t, err)
	_, otherKey, err := GenSelfSignedCert([]string{"192.168.0.2"})
	assert.Nil(t, err)

	err = ValidateCertKeyPair(certPEM, otherKey)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "do not match")
}

func TestValidateCertKeyPairGarbage(t *testing.T) {
	err := ValidateCertKeyPair("not a pem", "not a pem either")
	assert.NotNil(t, err)
}

func TestValidateCertKeyPairExpired(t *testing.T) {
	now := time.Now()
	certPEM, keyPEM, err := genSelfSignedCert([]string{"192.168.0.1"},
		now.AddDate(-2, 0, 0), now.AddDate(-1, 0, 0))
	assert.Nil(t, err)

	err = ValidateCertKeyPair(certPEM, keyPEM)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "expired")
}

func TestValidateCertKeyPairNotYetValid(t *testing.T) {
	now := time.Now()
	certPEM, keyPEM, err := genSelfSignedCert([]string{"192.168.0.1"},
		now.AddDate(1, 0, 0), now.AddDate(2, 0, 0))
	assert.Nil(t, err)

	err = ValidateCertKeyPair(certPEM, keyPEM)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "not valid until")
}

func TestCertNotAfter(t *testing.T) {
	now := time.Now()
	notAfter := now.AddDate(1, 0, 0)
	certPEM, _, err := genSelfSignedCert([]string{"192.168.0.1"}, now, notAfter)
	assert.Nil(t, err)

	got, err := CertNotAfter(certPEM)
	assert.Nil(t, err)
	assert.Equal(t, notAfter.Format("2006-01-02 15:04:05"), got)
}

func TestCertNotAfterInvalid(t *testing.T) {
	_, err := CertNotAfter("garbage")
	assert.NotNil(t, err)
}

func TestGenSelfSignedCertSkipEmptyHost(t *testing.T) {
	certPEM, _, err := GenSelfSignedCert([]string{"", "192.168.0.1"})
	assert.Nil(t, err)
	cert, err := parseCert(certPEM)
	assert.Nil(t, err)
	assert.NotContains(t, cert.DNSNames, "")
}
