package common

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"time"

	"github.com/pkg/errors"
)

const (
	// 自签名证书有效期。设计上不做到期告警与轮换，故取足够长的年限
	selfSignedValidYears = 10
	// 提前生效，容忍集群节点之间的时钟偏移
	selfSignedBackdate = time.Hour

	certTimeLayout = "2006-01-02 15:04:05"
)

// GenSelfSignedCert 生成一张自签名证书，SAN 覆盖 hosts 中的全部 IP/域名，
// 外加 localhost 与 127.0.0.1。
//
// 证书同时具备 ServerAuth 与 ClientAuth 用途：ClickHouse 执行分布式查询时，
// 本节点既是服务端也要作为客户端去连兄弟节点，缺少 ClientAuth 会导致握手失败。
func GenSelfSignedCert(hosts []string) (certPEM, keyPEM string, err error) {
	now := time.Now()
	return genSelfSignedCert(hosts, now.Add(-selfSignedBackdate), now.AddDate(selfSignedValidYears, 0, 0))
}

func genSelfSignedCert(hosts []string, notBefore, notAfter time.Time) (certPEM, keyPEM string, err error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", errors.Wrap(err, "generate rsa key")
	}

	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return "", "", errors.Wrap(err, "generate serial number")
	}

	tmpl := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "ckman-self-signed"},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	// 不用 append(hosts, ...)，那会写穿调用方切片的底层数组
	all := make([]string, 0, len(hosts)+2)
	all = append(all, hosts...)
	all = append(all, "localhost", "127.0.0.1")
	for _, h := range all {
		if h == "" {
			continue
		}
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		return "", "", errors.Wrap(err, "create certificate")
	}

	keyDer, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return "", "", errors.Wrap(err, "marshal private key")
	}

	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDer}))
	return certPEM, keyPEM, nil
}

// ValidateCertKeyPair 校验证书与私钥匹配，且当前时间落在有效期内。
// 拒绝过期证书是为了防止「下发死证书 -> 全停重启 -> 集群起不来」。
func ValidateCertKeyPair(certPEM, keyPEM string) error {
	if certPEM == "" || keyPEM == "" {
		return errors.New("certificate and private key must not be empty")
	}
	if _, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM)); err != nil {
		return errors.Wrap(err, "certificate and private key do not match")
	}
	_, err := ValidateCertValidity(certPEM)
	return err
}

// ValidateCertValidity 只校验证书本身：能否解析、当前时间是否落在
// NotBefore..NotAfter 有效期内。不涉及私钥，供「先导入证书、后导入私钥」
// 这种分步场景（例如证书检视接口）复用；私钥匹配仍只在 ValidateCertKeyPair
// 里做。返回的 *x509.Certificate 在解析失败时为 nil，解析成功但过期/未生效
// 时非 nil（调用方可能仍想拿 NotAfter 之类字段用于展示）。
func ValidateCertValidity(certPEM string) (*x509.Certificate, error) {
	cert, err := parseCert(certPEM)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	if now.Before(cert.NotBefore) {
		return cert, errors.Errorf("certificate is not valid until %s", cert.NotBefore.Local().Format(certTimeLayout))
	}
	if now.After(cert.NotAfter) {
		return cert, errors.Errorf("certificate expired at %s", cert.NotAfter.Local().Format(certTimeLayout))
	}
	return cert, nil
}

// CertNotAfter 解析证书到期时间，用于页面只读回显。不落库，每次现场解析。
func CertNotAfter(certPEM string) (string, error) {
	cert, err := parseCert(certPEM)
	if err != nil {
		return "", err
	}
	return cert.NotAfter.Local().Format(certTimeLayout), nil
}

func parseCert(certPEM string) (*x509.Certificate, error) {
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("failed to decode PEM block of certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "parse certificate")
	}
	return cert, nil
}
