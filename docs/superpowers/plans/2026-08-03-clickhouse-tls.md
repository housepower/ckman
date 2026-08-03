# ClickHouse TLS/HTTPS 支持 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 ckman 能够部署与运维启用 TLS 的 ClickHouse 集群 —— 生成服务端 TLS 配置、下发证书、支持明文集群在线切换为密文、支持证书到期更换。

**Architecture:** 证书内容作为 `TlsConf` 结构随集群配置存入 repository（私钥经 AES 加密），`ckconfig` 在生成 `custom.xml` 时按 `Secure` 开关写 `tcp_port_secure`/`https_port` + `openSSL` 节并对明文端口加 `remove` 属性，`metrika.xml` 的 replica 加 `<secure>1</secure>`，`deploy` 阶段把证书 scp 到节点的 `certs/` 目录。明文↔密文切换复用现有 `PUT /ck/config?force=true` 强制刷新配置链路。

**Tech Stack:** Go 1.24 / Gin / `crypto/x509` / gorm(repository) / Vue+TypeScript(ckman-fe)

## Global Constraints

- 设计依据：`docs/superpowers/specs/2026-08-03-clickhouse-tls-design.md`
- 加密范围**仅**客户端接入（`tcp_port_secure` 9440 / `https_port` 8443）与分布式查询（`remote_servers` 的 `<secure>`）。**不做** interserver 复制加密、ZK/Keeper 加密、双向认证、CA 体系、到期告警巡检、每节点独立证书、prometheus 9363 加密
- 自签名算法固定 **RSA 2048**，有效期 **10 年**，`NotBefore = now - 1h`
- 端口字段**复用** `Port`/`HttpPort`，**不新增**密文端口字段
- 路由只有 `router/v1.go`，**不存在 v2**，新接口一律注册到 `groupV1`
- `model` 包**不能** import `common`（`common` 已 import `model`，反向会循环依赖）
- 涉及 `TlsConf` 指针的读写路径（`Pack`、`EncodePasswd`、`DecodePasswd`）**必须新建对象**，不得原地修改 —— local repository 下该指针与内存中的集群配置共享，原地改会污染真实数据
- 提交信息用中文，结尾附 `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`
- 前端代码改在 `../ckman-fe/`，**绝不**在 `frontend/` 子模块内编辑或提交

---

## File Structure

| 文件 | 职责 | 动作 |
| --- | --- | --- |
| `common/cert.go` | 证书生成、校验、到期时间解析。纯标准库，不依赖 model | 创建 |
| `common/cert_test.go` | 上述三个函数的单测 | 创建 |
| `model/deploy_ck.go` | `TlsConf` 结构定义；`Pack`/`UnPack` 的私钥脱敏与回填 | 修改 |
| `repository/helper.go` | `EncodePasswd`/`DecodePasswd` 增加私钥加解密 | 修改 |
| `repository/helper_test.go` | 加解密往返与指针隔离验证 | 创建 |
| `ckconfig/custom.go` | `root()` 端口分支；新增 `openssl()` 节 | 修改 |
| `ckconfig/metrika.go` | `GenLocalMetrika`/`GenLogicMetrika` 写 `<secure>` | 修改 |
| `deploy/ck.go` | `Config()` 阶段下发/清理证书文件 | 修改 |
| `controller/clickhouse.go` | 参数校验、配置差异判定、`GetConfig` 回显、自签名接口 | 修改 |
| `controller/schema_ui.go` | `Secure`/`TlsConf` 的 schema 注册 | 修改 |
| `router/v1.go` | 注册自签名接口 | 修改 |
| `../ckman-fe/` | 证书表单、自签名按钮、端口联动、重启策略提示 | 修改 |

---

### Task 1: 证书工具函数

纯函数，无外部依赖，最适合先做。后续所有任务都消费这里的接口。

**Files:**
- Create: `common/cert.go`
- Test: `common/cert_test.go`

**Interfaces:**
- Consumes: 无
- Produces:
  - `func GenSelfSignedCert(hosts []string) (certPEM, keyPEM string, err error)`
  - `func ValidateCertKeyPair(certPEM, keyPEM string) error`
  - `func CertNotAfter(certPEM string) (string, error)` — 返回 `"2006-01-02 15:04:05"` 格式

- [ ] **Step 1: 写失败的测试**

创建 `common/cert_test.go`：

```go
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
	hosts := []string{"192.168.0.1"}
	_, _, err := GenSelfSignedCert(hosts)
	assert.Nil(t, err)
	// 内部追加 localhost/127.0.0.1 不得污染调用方切片
	assert.Equal(t, []string{"192.168.0.1"}, hosts)
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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./common/ -run 'TestGenSelfSignedCert|TestValidateCertKeyPair|TestCertNotAfter' -v`
Expected: 编译失败，`undefined: GenSelfSignedCert`

- [ ] **Step 3: 实现**

创建 `common/cert.go`：

```go
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
	cert, err := parseCert(certPEM)
	if err != nil {
		return err
	}
	now := time.Now()
	if now.Before(cert.NotBefore) {
		return errors.Errorf("certificate is not valid until %s", cert.NotBefore.Format(certTimeLayout))
	}
	if now.After(cert.NotAfter) {
		return errors.Errorf("certificate expired at %s", cert.NotAfter.Format(certTimeLayout))
	}
	return nil
}

// CertNotAfter 解析证书到期时间，用于页面只读回显。不落库，每次现场解析。
func CertNotAfter(certPEM string) (string, error) {
	cert, err := parseCert(certPEM)
	if err != nil {
		return "", err
	}
	return cert.NotAfter.Format(certTimeLayout), nil
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
```

- [ ] **Step 4: 运行测试确认通过**

Run: `go test ./common/ -run 'TestGenSelfSignedCert|TestValidateCertKeyPair|TestCertNotAfter' -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add common/cert.go common/cert_test.go
git commit -m "feat(tls): 新增自签名证书生成与校验工具

RSA 2048/10 年有效期,SAN 覆盖全部节点 IP 与 localhost;
ExtKeyUsage 同时含 ServerAuth 与 ClientAuth,供分布式查询握手;
校验拒绝过期与尚未生效的证书。

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: 数据模型 TlsConf

**Files:**
- Modify: `model/deploy_ck.go`（`CKManClickHouseConfig` 结构、`Pack()` 约 385 行、`UnPack()` 约 397 行）
- Test: `model/deploy_ck_test.go`（若不存在则创建）

**Interfaces:**
- Consumes: 无（`model` 不能 import `common`）
- Produces:
  - `type TlsConf struct { Cert, Key string; SelfSigned bool; NotAfter string }`
  - `CKManClickHouseConfig.TlsConf *TlsConf`
  - `Pack()` 将 `TlsConf.Key` 脱敏为等长星号，**并替换整个 TlsConf 指针**
  - `UnPack(conf)` 在 `Key` 为纯星号时从 `conf.TlsConf.Key` 回填

- [ ] **Step 1: 写失败的测试**

创建（或追加到）`model/deploy_ck_test.go`：

```go
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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./model/ -run TestPackTlsConf -v`
Expected: 编译失败，`undefined: TlsConf`

- [ ] **Step 3: 实现**

在 `model/deploy_ck.go` 的 `CKManClickHouseConfig` 结构里，紧跟 `Secure` 字段之后加：

```go
	Secure           bool      `json:"secure"`
	TlsConf          *TlsConf  `json:"tlsConf,omitempty"`
```

在 `CKManClickHouseConfig` 结构定义之后新增类型：

```go
// TlsConf 保存集群的服务端 TLS 证书。全集群共用一套，随集群配置一起落库。
// Key 在 repository.EncodePasswd 里做 AES 加密；NotAfter 只出不入——
// 由 GetConfig 从 Cert 现场解析填充，不落库，避免换证书后与实际值不一致。
type TlsConf struct {
	Cert       string `json:"cert"`
	Key        string `json:"key"`
	SelfSigned bool   `json:"selfSigned"`
	NotAfter   string `json:"notAfter" swaggerignore:"true"`
}
```

在 `Pack()` 末尾（`UsersConf.Users` 循环之后）加：

```go
	if config.TlsConf != nil {
		// 必须新建对象而非原地改：local repository 下 TlsConf 指针与内存中的
		// 集群配置共享，原地改会把真实私钥抹成星号
		config.TlsConf = &TlsConf{
			Cert:       config.TlsConf.Cert,
			Key:        strings.Repeat("*", len(config.TlsConf.Key)),
			SelfSigned: config.TlsConf.SelfSigned,
		}
	}
```

在 `UnPack()` 末尾（`UsersConf.Users` 循环之后）加：

```go
	if config.TlsConf != nil && conf.TlsConf != nil && reg.MatchString(config.TlsConf.Key) {
		config.TlsConf.Key = conf.TlsConf.Key
	}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `go test ./model/ -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add model/deploy_ck.go model/deploy_ck_test.go
git commit -m "feat(tls): 集群配置新增 TlsConf 结构

Pack 脱敏私钥时新建对象,避免污染 local repository 共享指针;
UnPack 在私钥为星号时从库中回填。

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: 私钥加密落库

**Files:**
- Modify: `repository/helper.go:17-31`（`EncodePasswd`）、`repository/helper.go:33-60`（`DecodePasswd`）
- Test: `repository/helper_test.go`

**Interfaces:**
- Consumes: `model.TlsConf`（Task 2）
- Produces: `EncodePasswd`/`DecodePasswd` 对 `conf.TlsConf.Key` 做 AES ECB 加解密，且不污染入参指向的原对象

- [ ] **Step 1: 写失败的测试**

创建 `repository/helper_test.go`：

```go
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
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./repository/ -run TestEncodeDecodeTls -v`
Expected: FAIL —— `EncodePasswd` 目前不碰 `TlsConf`，`conf.TlsConf.Key` 仍是明文

- [ ] **Step 3: 实现**

在 `repository/helper.go` 的 `EncodePasswd` 末尾（`Storage` 分支之后）加：

```go
	if conf.TlsConf != nil {
		// 新建对象：入参可能与内存中的集群配置共享同一 TlsConf 指针
		conf.TlsConf = &model.TlsConf{
			Cert:       conf.TlsConf.Cert,
			Key:        common.AesEncryptECB(conf.TlsConf.Key),
			SelfSigned: conf.TlsConf.SelfSigned,
		}
	}
```

在 `DecodePasswd` 末尾（`Storage` 分支之后）加：

```go
	if conf.TlsConf != nil {
		conf.TlsConf = &model.TlsConf{
			Cert:       conf.TlsConf.Cert,
			Key:        common.AesDecryptECB(conf.TlsConf.Key),
			SelfSigned: conf.TlsConf.SelfSigned,
		}
	}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `go test ./repository/ -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add repository/helper.go repository/helper_test.go
git commit -m "feat(tls): 私钥随集群配置加密落库

EncodePasswd/DecodePasswd 处理 TlsConf.Key,两处均新建对象
避免污染 local repository 的共享指针;证书本身不加密。

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: custom.xml 生成 TLS 配置

**Files:**
- Modify: `ckconfig/custom.go`（`root()` 约 21-22 行的端口写入；`GenerateCustomXML` 约 266 行）
- Test: `ckconfig/custom_test.go`

**Interfaces:**
- Consumes: `model.CKManClickHouseConfig.Secure`（Task 2）
- Produces: `Secure=true` 时 `custom.xml` 含 `tcp_port_secure`、`https_port`、`<tcp_port remove="remove"/>`、`<http_port remove="remove"/>`、完整 `openSSL` 节

**背景：** `remove` 属性沿用 `ckconfig/users.go:26` 抹 default 明文密码的现成写法 —— `common/xml.go:214` 的 `parseTags` 已支持 xpath 风格属性 key，`Merge()` 里 `WritewithAttr` 也已存在，**`common/xml.go` 无需改动**。

- [ ] **Step 1: 写失败的测试**

在 `ckconfig/custom_test.go` 末尾追加。注意现有测试只 assert `err == nil` 并把结果 dump 成 fixture 供人工查看，这里要加真实断言：

```go
func TestGenerateCustomXMLSecure(t *testing.T) {
	conf := &model.CKManClickHouseConfig{
		Cluster: "abc",
		Shards: []model.CkShard{
			{[]model.CkReplica{
				{Ip: "192.168.101.40", HostName: "vm10140"},
			}},
		},
		ZkNodes:   []string{"192.168.101.40"},
		ZkPort:    2181,
		Port:      9440,
		HttpPort:  8443,
		Secure:    true,
		IsReplica: true,
		Cwd:       "/home/eoi/clickhouse",
		NeedSudo:  false,
		Path:      "/data01/",
		Version:   "25.3.1.2703",
	}
	_, err := GenerateCustomXML("custom_secure_fake.xml", conf, model.CkDeployExt{
		NumCPU:     48,
		Ipv6Enable: false,
	})
	assert.Nil(t, err)

	data, err := os.ReadFile("custom_secure_fake.xml")
	assert.Nil(t, err)
	content := string(data)

	// 密文端口开启
	assert.Contains(t, content, "<tcp_port_secure>9440</tcp_port_secure>")
	assert.Contains(t, content, "<https_port>8443</https_port>")
	// 明文端口关闭
	assert.Contains(t, content, `<tcp_port remove="remove">`)
	assert.Contains(t, content, `<http_port remove="remove">`)
	// 不能再出现明文端口的实际值
	assert.NotContains(t, content, "<tcp_port>9440</tcp_port>")
	assert.NotContains(t, content, "<http_port>8443</http_port>")

	// 服务端证书
	assert.Contains(t, content, "<certificateFile>/home/eoi/clickhouse/etc/clickhouse-server/certs/server.crt</certificateFile>")
	assert.Contains(t, content, "<privateKeyFile>/home/eoi/clickhouse/etc/clickhouse-server/certs/server.key</privateKeyFile>")
	assert.Contains(t, content, "<verificationMode>none</verificationMode>")
	// client 段必须存在：分布式查询时本节点作为客户端连兄弟节点，
	// 自签证书下缺 AcceptCertificateHandler 会直接握手失败
	assert.Contains(t, content, "<name>AcceptCertificateHandler</name>")
}

func TestGenerateCustomXMLSecureNeedSudo(t *testing.T) {
	conf := &model.CKManClickHouseConfig{
		Cluster:   "abc",
		Shards:    []model.CkShard{{[]model.CkReplica{{Ip: "192.168.101.40"}}}},
		Port:      9440,
		HttpPort:  8443,
		Secure:    true,
		NeedSudo:  true,
		Path:      "/data01/",
		Version:   "25.3.1.2703",
	}
	_, err := GenerateCustomXML("custom_secure_sudo_fake.xml", conf, model.CkDeployExt{NumCPU: 48})
	assert.Nil(t, err)

	data, err := os.ReadFile("custom_secure_sudo_fake.xml")
	assert.Nil(t, err)
	assert.Contains(t, string(data), "<certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>")
}

func TestGenerateCustomXMLPlaintextUnchanged(t *testing.T) {
	conf := &model.CKManClickHouseConfig{
		Cluster:  "abc",
		Shards:   []model.CkShard{{[]model.CkReplica{{Ip: "192.168.101.40"}}}},
		Port:     9000,
		HttpPort: 8123,
		Secure:   false,
		NeedSudo: true,
		Path:     "/data01/",
		Version:  "25.3.1.2703",
	}
	_, err := GenerateCustomXML("custom_plain_fake.xml", conf, model.CkDeployExt{NumCPU: 48})
	assert.Nil(t, err)

	data, err := os.ReadFile("custom_plain_fake.xml")
	assert.Nil(t, err)
	content := string(data)
	assert.Contains(t, content, "<tcp_port>9000</tcp_port>")
	assert.Contains(t, content, "<http_port>8123</http_port>")
	assert.NotContains(t, content, "openSSL")
	assert.NotContains(t, content, "remove")
}
```

同时在该文件 import 块加入 `"os"`。

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./ckconfig/ -run TestGenerateCustomXML -v`
Expected: FAIL —— 生成的 XML 里没有 `tcp_port_secure` / `openSSL`

- [ ] **Step 3: 实现**

在 `ckconfig/custom.go` 的 `root()` 里，把现有这两行：

```go
	output["tcp_port"] = conf.Port
	output["http_port"] = conf.HttpPort
```

替换为：

```go
	if conf.Secure {
		output["tcp_port_secure"] = conf.Port
		output["https_port"] = conf.HttpPort
		// 关闭明文端口，避免与密文端口并存造成误用
		output["tcp_port[@remove=\"remove\"]"] = ""
		output["http_port[@remove=\"remove\"]"] = ""
	} else {
		output["tcp_port"] = conf.Port
		output["http_port"] = conf.HttpPort
	}
```

在 `merge_tree()` 函数之后新增：

```go
// CertsDir 返回节点上存放 TLS 证书的目录。
// 与 deploy 阶段的下发路径必须一致。
func CertsDir(conf *model.CKManClickHouseConfig) string {
	if conf.NeedSudo {
		return "/etc/clickhouse-server/certs"
	}
	return path.Join(conf.Cwd, "etc", "clickhouse-server", "certs")
}

func openssl(conf *model.CKManClickHouseConfig) map[string]interface{} {
	certsDir := CertsDir(conf)
	output := make(map[string]interface{})
	output["openSSL"] = map[string]interface{}{
		"server": map[string]interface{}{
			"certificateFile": path.Join(certsDir, "server.crt"),
			"privateKeyFile":  path.Join(certsDir, "server.key"),
			// 不做双向认证，不要求客户端证书
			"verificationMode":    "none",
			"cacheSessions":       true,
			"disableProtocols":    "sslv2,sslv3",
			"preferServerCiphers": true,
		},
		// client 段不可省略：分布式查询时本节点要作为客户端连兄弟节点，
		// 自签证书下缺少 AcceptCertificateHandler 会直接握手失败
		"client": map[string]interface{}{
			"loadDefaultCAFile":   true,
			"cacheSessions":       true,
			"disableProtocols":    "sslv2,sslv3",
			"preferServerCiphers": true,
			"invalidCertificateHandler": map[string]interface{}{
				"name": "AcceptCertificateHandler",
			},
		},
	}
	return output
}
```

在 `GenerateCustomXML` 里，`mergo.Merge(&custom, prometheus())` 之后加：

```go
	if conf.Secure {
		mergo.Merge(&custom, openssl(conf))
	}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `go test ./ckconfig/ -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add ckconfig/custom.go ckconfig/custom_test.go ckconfig/custom_secure_fake.xml ckconfig/custom_secure_sudo_fake.xml ckconfig/custom_plain_fake.xml
git commit -m "feat(tls): custom.xml 生成密文端口与 openSSL 配置

Secure 时写 tcp_port_secure/https_port 并对明文端口加 remove 属性;
openSSL 的 client 段带 AcceptCertificateHandler,保证自签证书下
分布式查询握手可通过。

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: metrika.xml 分布式查询加密

**Files:**
- Modify: `ckconfig/metrika.go:59-87`（`GenLocalMetrika`）、`ckconfig/metrika.go:89-115`（`GenLogicMetrika`）
- Test: `ckconfig/metrika_test.go`

**Interfaces:**
- Consumes: `model.CKManClickHouseConfig.Secure`（Task 2）
- Produces: `Secure=true` 的集群，其 replica 块含 `<secure>1</secure>`

**背景：** `GenLogicMetrika` 逐成员集群遍历，各写各的 `Secure`，因此逻辑集群「部分加密」的中间态能正确生成 XML —— 中间态下跨集群查询不通，属设计上接受的范围。

- [ ] **Step 1: 写失败的测试**

在 `ckconfig/metrika_test.go` 末尾追加：

```go
func TestMetrikaSecure(t *testing.T) {
	conf := model.CKManClickHouseConfig{
		Cluster: "test",
		Shards: []model.CkShard{
			{[]model.CkReplica{{HostName: "ck01", Ip: "192.168.0.1"}}},
		},
		Port:      9440,
		Secure:    true,
		ZkNodes:   []string{"192.168.0.1"},
		ZkPort:    2181,
		IsReplica: true,
		Version:   "22.8.8.3",
		User:      "default",
		Password:  "123456",
	}
	_, err := GenerateMetrikaXML("metrika_secure_fake.xml", &conf)
	assert.Nil(t, err)

	data, err := os.ReadFile("metrika_secure_fake.xml")
	assert.Nil(t, err)
	content := string(data)
	assert.Contains(t, content, "<secure>1</secure>")
	assert.Contains(t, content, "<port>9440</port>")
}

func TestMetrikaSecureOff(t *testing.T) {
	conf := model.CKManClickHouseConfig{
		Cluster:   "test",
		Shards:    []model.CkShard{{[]model.CkReplica{{Ip: "192.168.0.1"}}}},
		Port:      9000,
		Secure:    false,
		ZkNodes:   []string{"192.168.0.1"},
		ZkPort:    2181,
		IsReplica: true,
		Version:   "22.8.8.3",
	}
	_, err := GenerateMetrikaXML("metrika_plain_fake.xml", &conf)
	assert.Nil(t, err)

	data, err := os.ReadFile("metrika_plain_fake.xml")
	assert.Nil(t, err)
	assert.NotContains(t, string(data), "<secure>")
}

func TestGenLogicMetrikaMixedSecure(t *testing.T) {
	// 明文集群切 TLS 的中间态：逻辑集群内一个加密一个不加密，
	// XML 必须仍能正确生成，各成员按各自 Secure 写
	clusters := []model.CKManClickHouseConfig{
		{
			Cluster:   "secure_one",
			Shards:    []model.CkShard{{[]model.CkReplica{{Ip: "192.168.0.1"}}}},
			Port:      9440,
			Secure:    true,
			IsReplica: true,
		},
		{
			Cluster:   "plain_one",
			Shards:    []model.CkShard{{[]model.CkReplica{{Ip: "192.168.0.2"}}}},
			Port:      9000,
			Secure:    false,
			IsReplica: true,
		},
	}
	content := GenLogicMetrika("logic_test", clusters, true)
	assert.Contains(t, content, "<host>192.168.0.1</host>")
	assert.Contains(t, content, "<host>192.168.0.2</host>")
	// 加密成员有 secure 标记，明文成员没有
	assert.Equal(t, 1, strings.Count(content, "<secure>1</secure>"))
}
```

在该文件 import 块加入 `"os"` 与 `"strings"`。

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./ckconfig/ -run TestMetrika -v`
Expected: FAIL —— 生成的 XML 里没有 `<secure>`

- [ ] **Step 3: 实现**

在 `ckconfig/metrika.go` 的 `GenLocalMetrika` 里，replica 块内 `xml.Write("port", conf.Port)` 之后加：

```go
			if conf.Secure {
				xml.Write("secure", 1)
			}
```

在 `GenLogicMetrika` 里，replica 块内 `xml.Write("port", conf.Port)` 之后加同样三行（`conf` 是循环变量，指当前成员集群）：

```go
				if conf.Secure {
					xml.Write("secure", 1)
				}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `go test ./ckconfig/ -v`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add ckconfig/metrika.go ckconfig/metrika_test.go ckconfig/metrika_secure_fake.xml ckconfig/metrika_plain_fake.xml
git commit -m "feat(tls): remote_servers 按集群 Secure 写 secure 标记

逻辑集群逐成员判定,支持明文集群切 TLS 过程中的混合中间态。

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: 证书下发到节点

**Files:**
- Modify: `deploy/ck.go`（`Config()` 约 337-470 行）

**Interfaces:**
- Consumes: `ckconfig.CertsDir(conf)`（Task 4）、`model.TlsConf`（Task 2）
- Produces: 节点上 `{CertsDir}/server.crt`(644) 与 `{CertsDir}/server.key`(600)；`Secure=false` 时该目录被删除

**背景：**
- `Config()` 里 `remotePath` 已是 `/etc/clickhouse-server`（NeedSudo）或 `<cwd>/etc/clickhouse-server`（tgz），与 `ckconfig.CertsDir` 的取值规则一致
- owner 无需单独处理：`Config()` 结尾在 NeedSudo 分支已有 `chown -R clickhouse:clickhouse /etc/clickhouse-server`，递归覆盖 certs
- 现有清理命令 `find config.d -maxdepth 1 -name '*.xml' -delete` 不波及 certs 目录

- [ ] **Step 1: 实现证书下发**

在 `deploy/ck.go` 的 `Config()` 中，找到 `prepareCmds` 的构造处：

```go
			prepareCmds := []string{
				fmt.Sprintf("mkdir -p %s %s", path.Join(remotePath, "config.d"), path.Join(remotePath, "users.d")),
			}
```

改为：

```go
			certsDir := path.Join(remotePath, "certs")
			prepareCmds := []string{
				fmt.Sprintf("mkdir -p %s %s", path.Join(remotePath, "config.d"), path.Join(remotePath, "users.d")),
				// 无论开关如何都先清掉旧证书，保证回退明文时目录干净
				fmt.Sprintf("rm -rf %s", certsDir),
			}
			if d.Conf.Secure && d.Conf.TlsConf != nil {
				prepareCmds = append(prepareCmds, fmt.Sprintf("mkdir -p %s", certsDir))
			}
```

在 `common.ScpUploadFiles([]string{usersXml}, path.Join(remotePath, "users.d"), sshOpts)` 之后、`cmds := make([]string, 0)` 之前插入：

```go
			if d.Conf.Secure && d.Conf.TlsConf != nil {
				certFile, err := common.NewTempFile(path.Join(config.GetWorkDirectory(), "package"), "server_crt")
				if err != nil {
					lastError = err
					return
				}
				defer os.Remove(certFile.FullName)
				if err := os.WriteFile(certFile.FullName, []byte(d.Conf.TlsConf.Cert), 0644); err != nil {
					lastError = err
					return
				}

				keyFile, err := common.NewTempFile(path.Join(config.GetWorkDirectory(), "package"), "server_key")
				if err != nil {
					lastError = err
					return
				}
				defer os.Remove(keyFile.FullName)
				if err := os.WriteFile(keyFile.FullName, []byte(d.Conf.TlsConf.Key), 0600); err != nil {
					lastError = err
					return
				}

				if err := common.ScpUploadFile(certFile.FullName, path.Join(certsDir, "server.crt"), sshOpts); err != nil {
					lastError = err
					return
				}
				if err := common.ScpUploadFile(keyFile.FullName, path.Join(certsDir, "server.key"), sshOpts); err != nil {
					lastError = err
					return
				}
			}
```

在 `cmds` 的最后一条（NeedSudo 的 `chown -R` 之后）追加权限收紧。找到：

```go
			cmds = append(cmds, "rm -rf /tmp/host* /tmp/users*")
			if d.Conf.NeedSudo {
				cmds = append(cmds, fmt.Sprintf("chown -R clickhouse:clickhouse /etc/clickhouse-server"))
			}
```

在其后加：

```go
			if d.Conf.Secure && d.Conf.TlsConf != nil {
				// chown -R 不改 mode，私钥必须单独收紧到 600
				cmds = append(cmds,
					fmt.Sprintf("chmod 644 %s", path.Join(certsDir, "server.crt")),
					fmt.Sprintf("chmod 600 %s", path.Join(certsDir, "server.key")),
				)
			}
			cmds = append(cmds, "rm -rf /tmp/server_crt* /tmp/server_key*")
```

- [ ] **Step 2: 编译并跑全量单测**

Run: `go build ./... && go test ./deploy/ ./ckconfig/ ./common/ ./model/ ./repository/ -v`
Expected: 编译通过，测试全 PASS

（`deploy` 层依赖真实 SSH，无法单测覆盖，靠编译 + 后续人工联调验证。）

- [ ] **Step 3: 提交**

```bash
git add deploy/ck.go
git commit -m "feat(tls): 部署阶段下发证书到节点 certs 目录

每次下发前先删旧证书,保证回退明文时目录干净;
私钥落盘 600,证书 644;临时文件下发后清理。

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: 配置校验与变更判定

**Files:**
- Modify: `controller/clickhouse.go`（`checkConfigParams` 约 3046 行、`mergeClickhouseConfig` 约 3181 行、`GetConfig` 约 2890 行）

**Interfaces:**
- Consumes: `common.ValidateCertKeyPair`、`common.CertNotAfter`（Task 1）；`model.TlsConf`（Task 2）
- Produces: `Secure=true` 且证书缺失/非法时 `ConfigCluster` 返回参数错误；`Secure` 或 `TlsConf` 变化时触发重下配置 + 重启；`GetConfig` 返回带 `NotAfter`

**背景：** `GetConfig` 走 `cluster.Pack()` → `params.MarshalConfig(cluster)`。`Pack()`（Task 2）已把 `TlsConf` 换成新对象，所以在 `GetConfig` 里填 `NotAfter` 改的是副本，不会污染库中数据。`NotAfter` 的填充不能放进 `Pack()` —— `model` 包不能 import `common`。

- [ ] **Step 1: `checkConfigParams` 增加证书校验**

在 `controller/clickhouse.go` 的 `checkConfigParams` 里，`if conf.SshUser == ""` 那段校验之后插入：

```go
	if conf.Secure {
		if conf.TlsConf == nil || conf.TlsConf.Cert == "" || conf.TlsConf.Key == "" {
			return errors.Errorf("tls certificate and private key are required when secure is enabled")
		}
		if err := common.ValidateCertKeyPair(conf.TlsConf.Cert, conf.TlsConf.Key); err != nil {
			return errors.Wrap(err, "invalid tls certificate")
		}
	}
```

- [ ] **Step 2: `mergeClickhouseConfig` 纳入 TLS 变更判定**

在 `mergeClickhouseConfig` 里，`keeperChanged := ...` 那一行之后加：

```go
	secureChanged := cluster.Secure != conf.Secure || !reflect.DeepEqual(cluster.TlsConf, conf.TlsConf)
```

把 `noChangedFn` 里的 `!userconfChanged && !logicChaned && !zkChanged && !keeperChanged` 改为：

```go
			!userconfChanged && !logicChaned && !zkChanged && !keeperChanged && !secureChanged
```

把设置 `cc` 的条件里加上 `secureChanged`：

```go
	if force || cluster.Port != conf.Port ||
		cluster.Password != conf.Password || storageChanged || expertChanged ||
		cluster.ZkPort != conf.ZkPort ||
		userconfChanged || logicChaned || zkChanged || keeperChanged || secureChanged {
		cc = true
	}
```

把设置 `restart` 的条件里加上 `secureChanged`：

```go
	// need restart
	if force || cluster.Port != conf.Port || storageChanged || expertChanged || keeperChanged || secureChanged {
		restart = true
	}
```

在 merge 段（`cluster.Comment = conf.Comment` 那一串赋值）里加：

```go
	cluster.Secure = conf.Secure
	cluster.TlsConf = conf.TlsConf
```

- [ ] **Step 3: 逻辑集群加密状态不一致时记警告**

在 `mergeClickhouseConfig` 的 `secureChanged` 定义之后加：

```go
	if secureChanged && cluster.LogicCluster != nil {
		// 同一逻辑集群内应当要么全加密要么全不加密。切换过程中必然出现
		// 短暂的不一致（跨集群查询不通），这是设计上接受的中间态，
		// 只告警不阻断，避免把用户卡死在半程。
		if logics, e := repository.Ps.GetLogicClusterbyName(*cluster.LogicCluster); e == nil {
			for _, logic := range logics {
				if logic == cluster.Cluster {
					continue
				}
				if peer, e1 := repository.Ps.GetClusterbyName(logic); e1 == nil && peer.Secure != conf.Secure {
					log.Logger.Warnf("cluster %s secure=%v differs from logic peer %s secure=%v, cross-cluster query will be unavailable until all members are aligned",
						cluster.Cluster, conf.Secure, logic, peer.Secure)
				}
			}
		}
	}
```

- [ ] **Step 4: `GetConfig` 回显证书到期时间**

在 `controller/clickhouse.go` 的 `GetConfig` 里，`cluster.Pack()` 之后加：

```go
	if cluster.TlsConf != nil {
		// Pack 已把 TlsConf 换成新对象，这里改的是副本，不会污染库中数据。
		// 到期时间每次现场解析，不落库，避免换证书后与实际值不一致。
		cluster.TlsConf.NotAfter, _ = common.CertNotAfter(cluster.TlsConf.Cert)
	}
```

- [ ] **Step 5: 编译并运行测试**

Run: `go build ./... && go test ./controller/ ./model/ ./common/ -v`
Expected: 编译通过，测试全 PASS

- [ ] **Step 6: 提交**

```bash
git add controller/clickhouse.go
git commit -m "feat(tls): 配置校验与变更判定支持 TLS

Secure 开启时强制校验证书有效性(含过期拦截);
Secure/TlsConf 变化触发重下配置与重启;
逻辑集群成员加密状态不一致时记警告但不阻断;
GetConfig 现场解析证书到期时间回显。

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: 自签名证书生成接口

**Files:**
- Modify: `controller/clickhouse.go`（新增 handler）、`router/v1.go`、`model/deploy_ck.go`（请求/响应结构）

**Interfaces:**
- Consumes: `common.GenSelfSignedCert`（Task 1）
- Produces: `POST /api/v1/ck/cert/selfsigned`，请求体 `{"hosts": [...]}`，响应 `{"cert": "...", "key": "..."}`

**背景：** 接口只生成不落库。前端拿到后填回表单，随集群配置一起提交，使「生成自签名」与「上传」在后端走同一条入库与校验路径（Task 7 的 `checkConfigParams`）。**`router/` 下只有 `v1.go`，不存在 v2。**

- [ ] **Step 1: 定义请求/响应结构**

在 `model/deploy_ck.go` 末尾加：

```go
type SelfSignedCertReq struct {
	Hosts []string `json:"hosts" example:"192.168.0.1,192.168.0.2"`
}

type SelfSignedCertRsp struct {
	Cert string `json:"cert"`
	Key  string `json:"key"`
}
```

- [ ] **Step 2: 实现 handler**

在 `controller/clickhouse.go` 里 `GetConfig` 之后新增：

```go
// @Summary  生成自签名证书
// @Description 为指定节点列表生成一张自签名 TLS 证书，仅返回内容不落库
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param req body model.SelfSignedCertReq true "request body"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":{"cert":"","key":""}}"
// @Router /api/v1/ck/cert/selfsigned [post]
func (controller *ClickHouseController) GenSelfSignedCert(c *gin.Context) {
	var req model.SelfSignedCertReq
	if err := c.ShouldBindJSON(&req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	if len(req.Hosts) == 0 {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, errors.Errorf("hosts must not be empty"))
		return
	}
	cert, key, err := common.GenSelfSignedCert(req.Hosts)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, model.SelfSignedCertRsp{Cert: cert, Key: key})
}
```

**注意：** 这里用 `c.ShouldBindJSON`（与 `controller/rbac.go:27` 等 handler 一致）。
**不要**用 `controller.DecodeRequestBody` —— 那个函数签名是
`DecodeRequestBody(request *http.Request, conf *model.CKManClickHouseConfig, typo string) error`，
专用于 schema 驱动的集群配置解析，与本接口的请求体不匹配。

- [ ] **Step 3: 注册路由**

在 `router/v1.go` 里 `groupV1.GET(fmt.Sprintf("/ck/config/:%s", ...), ckController.GetConfig)` 附近加：

```go
	groupV1.POST("/ck/cert/selfsigned", ckController.GenSelfSignedCert)
```

- [ ] **Step 4: 编译并手工验证**

Run: `go build ./...`
Expected: 编译通过

启动 ckman 后用 curl 验证（`<TOKEN>` 换成登录接口拿到的 token）：

```bash
curl -s -X POST http://127.0.0.1:8808/api/v1/ck/cert/selfsigned \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"hosts":["192.168.0.1","192.168.0.2"]}' | head -c 400
```

Expected: 返回 `code: "0000"`，`data.cert` 以 `-----BEGIN CERTIFICATE-----` 开头

- [ ] **Step 5: 提交**

```bash
git add controller/clickhouse.go router/v1.go model/deploy_ck.go
git commit -m "feat(tls): 新增自签名证书生成接口

POST /api/v1/ck/cert/selfsigned 只生成不落库,
前端填回表单后随集群配置提交,与上传走同一条校验入库路径。

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: schema_ui 注册

**Files:**
- Modify: `controller/schema_ui.go`（部署页 `Secure` 约 181 行、配置页 `Secure` 约 1250 行）

**Interfaces:**
- Consumes: `model.TlsConf`（Task 2）
- Produces: 前端可见的 `Secure` 开关与 `TlsConf.Cert`/`TlsConf.Key`/`TlsConf.NotAfter` 字段

**背景：** 嵌套 struct 的注册模式见 `controller/schema_ui.go:198-250` 的 `KeeperConf` —— 先注册父字段，再 `var x model.Xxx` 后逐个注册子字段。`common.InputTextArea` 常量已存在（`common/config.go:48`）。

- [ ] **Step 1: 部署页（约 181 行）**

把现有的：

```go
	params.MustRegister(conf, "Secure", &common.Parameter{
		LabelZH:  "TLS安全认证",
		LabelEN:  "TLS Secure",
		Default:  "false",
		Required: "false",
		Editable: "false",
	})
```

替换为（部署页 `Editable` 保持原值，新建集群本就可填）：

```go
	params.MustRegister(conf, "Secure", &common.Parameter{
		LabelZH:       "TLS安全认证",
		LabelEN:       "TLS Secure",
		DescriptionZH: "开启后集群仅监听密文端口(默认9440/8443)，明文端口将被关闭",
		DescriptionEN: "Once enabled, only secure ports (9440/8443 by default) are exposed; plaintext ports are removed.",
		Default:       "false",
		Required:      "false",
		Editable:      "false",
	})

	params.MustRegister(conf, "TlsConf", &common.Parameter{
		LabelZH:       "TLS证书",
		LabelEN:       "TlsConf",
		DescriptionZH: "集群共用一套证书，需覆盖全部节点",
		Visiable:      "Secure == true",
		Required:      "false",
	})

	var tlsConf model.TlsConf
	params.MustRegister(tlsConf, "Cert", &common.Parameter{
		LabelZH:       "证书(PEM)",
		LabelEN:       "Certificate (PEM)",
		DescriptionZH: "PEM 格式的服务端证书内容",
		InputType:     common.InputTextArea,
	})
	params.MustRegister(tlsConf, "Key", &common.Parameter{
		LabelZH:       "私钥(PEM)",
		LabelEN:       "Private Key (PEM)",
		DescriptionZH: "PEM 格式的私钥内容，保存后不再回显明文",
		InputType:     common.InputTextArea,
	})
	params.MustRegister(tlsConf, "SelfSigned", &common.Parameter{
		LabelZH:  "自签名证书",
		LabelEN:  "Self Signed",
		Default:  "false",
		Required: "false",
		Editable: "false",
	})
	params.MustRegister(tlsConf, "NotAfter", &common.Parameter{
		LabelZH:  "证书有效期至",
		LabelEN:  "Valid Until",
		Required: "false",
		Editable: "false",
	})
```

- [ ] **Step 2: 配置页（约 1250 行）**

把配置页那处的 `Secure` 注册替换为 —— **注意 `Editable` 必须改成 `"true"`，这是明文集群在线切 TLS 的入口**：

```go
	params.MustRegister(conf, "Secure", &common.Parameter{
		LabelZH:       "TLS安全认证",
		LabelEN:       "TLS Secure",
		DescriptionZH: "开启后集群仅监听密文端口(默认9440/8443)，明文端口将被关闭。变更该项需要重启集群",
		DescriptionEN: "Once enabled, only secure ports (9440/8443 by default) are exposed; plaintext ports are removed. Changing this requires a cluster restart.",
		Default:       "false",
		Required:      "false",
		Editable:      "true",
	})
```

紧随其后，把 Step 1 里 `TlsConf` 及其四个子字段的注册代码**原样重复一遍**，包括 `var tlsConf model.TlsConf` 这行声明。

部署页在 `RegistCreateClusterSchema()`（65-963 行），配置页在 `RegistUpdateConfigSchema()`（964-1873 行），
是两个彼此独立的函数，各自声明局部变量不会冲突，也不能相互引用。

- [ ] **Step 3: 编译并验证 schema 输出**

Run: `go build ./... && go test ./controller/ -v`
Expected: 编译通过，测试 PASS

启动 ckman 后验证 schema 里出现了新字段：

```bash
curl -s "http://127.0.0.1:8808/api/v1/ck/config/<cluster>" \
  -H "Authorization: Bearer <TOKEN>" | grep -o "TlsConf"
```

Expected: 输出 `TlsConf`

- [ ] **Step 4: 提交**

```bash
git add controller/schema_ui.go
git commit -m "feat(tls): schema_ui 注册证书字段

配置页 Secure 改为可编辑,作为明文集群在线切 TLS 的入口;
新增 TlsConf 的证书/私钥/自签标记/有效期字段。

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: 前端

**Files:**
- Modify: `../ckman-fe/`（具体文件待探索确定）

**Interfaces:**
- Consumes: `GET /api/v1/ck/config/{cluster}` 返回的 schema（Task 9）、`POST /api/v1/ck/cert/selfsigned`（Task 8）
- Produces: 可用的证书录入交互

**前置检查（必做）：**

```bash
cd ../ckman-fe && git status && git branch -vv
```

确认工作区干净、当前在 `main` 分支。**绝不在 `frontend/` 子模块内编辑或提交** —— 那里 HEAD 常处于游离状态，提交会丢失。

- [ ] **Step 1: 探索现有 schema 表单渲染**

在 `../ckman-fe/` 中定位：schema 驱动的表单组件如何处理 `InputType`（`text`/`textarea`/`password`）、如何处理嵌套对象（参考 `KeeperConf` 的渲染）、如何解析 `Visiable` 表达式。

Run: `cd ../ckman-fe && grep -rn "textarea\|InputType\|Visiable" src/ | head -20`

产出：记录下需要修改的组件文件路径。

- [ ] **Step 2: 端口默认值联动**

行为规格：用户勾选 `Secure` 时，若 `Port` 当前值为 `9000` 则改为 `9440`，若 `HttpPort` 当前值为 `8123` 则改为 `8443`；取消勾选时反向。**仅在端口是默认值时联动**，用户手工改过的端口不覆盖。

- [ ] **Step 3: 证书区交互**

行为规格：
1. `Secure` 勾选时展开证书区（由 schema 的 `Visiable: "Secure == true"` 驱动，若通用渲染已支持则无需额外代码）
2. `Cert`/`Key` 两个 textarea
3. 「从文件导入」按钮：用浏览器 `FileReader` 本地读取 `.crt`/`.pem`/`.key` 文件内容填入对应 textarea，**不走后端上传接口**
4. 「生成自签名证书」按钮：调 `POST /api/v1/ck/cert/selfsigned`，请求体的 `hosts` 取表单中已填的全部节点 IP，把响应的 `cert`/`key` 填入两个 textarea
5. `NotAfter` 只读展示为「证书有效期至 xxx」，为空时不显示

- [ ] **Step 4: 重启策略提示**

行为规格：集群配置修改页，当 `Secure` 或证书内容相对初始值发生变化时，重启策略默认选中「全量停机」（`policy=full`），并给出提示文案：「切换 TLS 需要重启全部节点，期间集群不可用」。

- [ ] **Step 5: 构建验证**

Run: `cd ../ckman-fe && make build`
Expected: 构建成功无报错

- [ ] **Step 6: 提交前端并更新 submodule 指针**

```bash
cd ../ckman-fe
git add -A
git commit -m "feat(tls): 集群 TLS 证书录入与自签名生成

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
git push
```

拿到新 commit sha 后回到主仓：

```bash
cd ../ckman/frontend && git fetch && git checkout <new-sha>
cd .. && git add frontend
git commit -m "chore(frontend): 更新子模块指针至 TLS 证书录入

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: 收尾验证

**Files:**
- Modify: `docs/`（swagger 生成物）

- [ ] **Step 1: 重新生成 swagger 文档**

新增了 API 接口，必须重新生成。

Run: `swag init`
Expected: `docs/docs.go`、`docs/swagger.json`、`docs/swagger.yaml` 更新，其中包含 `/ck/cert/selfsigned`

验证：`grep -c "cert/selfsigned" docs/swagger.json`，Expected: 输出 ≥ 1

- [ ] **Step 2: 全量单测**

Run: `make test`
Expected: 全部 PASS

- [ ] **Step 3: 数据库后端回归**

模型发生了变更（新增 `TlsConf`），按项目约定必须跑真实数据库回归。

Run: `./repository/dbtest/pg-local-test.sh`
Expected: PASS

再按 `repository/dbtest/README.md` 配置 MySQL 与 DM8 的环境变量后：

Run: `go test -tags dbintegration -v ./repository/dbtest/ -run TestBackends`
Expected: 三个后端全 PASS

**验的是什么：** 带 `TlsConf` 的集群配置在三个后端上写入/读回/加解密往返一致。列容量已确认足够（MySQL `longtext`、Postgres `text`、DM8 `Clob` 1MB，证书入库约 3.5KB），不是本轮的风险点。

- [ ] **Step 4: 完整构建**

Run: `make build`
Expected: 构建成功

- [ ] **Step 5: 提交**

```bash
git add docs/
git commit -m "docs(swagger): 重新生成 API 文档

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 6: 真实集群联调（人工）**

单测覆盖不到 SSH 下发与 ClickHouse 实际启动，需要在真实集群上验证下列场景：

1. **新建 TLS 集群**：勾 Secure + 生成自签名 → 部署 → 节点上 `ss -lntp | grep 9440` 有监听、`grep 9000` 无监听
2. **ckman 连接**：集群列表能正常拉到表信息（验证 ckman 客户端 TLS 通路）
3. **分布式查询**：建分布式表并查询，验证 `openSSL` 的 `<client>` 段生效
4. **明文集群切 TLS**：改配置勾 Secure + 传证书 + policy=full → 重启后同样验证 1、2、3
5. **证书更换**：改 `TlsConf` 内容 → `force=true&policy=full` → 集群正常起来
6. **回退明文**：取消 Secure → 节点上 certs 目录消失、9000 恢复监听
7. **扩容**：TLS 集群加节点，新节点自动拿到证书并加入集群

---

## Self-Review

**1. Spec coverage**

| Spec 章节 | 对应任务 |
| --- | --- |
| 数据模型 `TlsConf` | Task 2 |
| 存储容量 | Task 11 Step 3 |
| 证书生命周期（生成/校验/过期拦截） | Task 1 |
| 私钥加密落库 | Task 3 |
| custom.xml 端口切换 | Task 4 |
| custom.xml openSSL 节 | Task 4 |
| metrika.xml remote_servers | Task 5 |
| 证书下发与权限 | Task 6 |
| 连接池失效 | 无需任务 —— `common/ck.go:84` 的 `orderId` 已含 TLS 配置与端口，自动失效重建（spec 已说明） |
| 自签名接口 | Task 8 |
| `checkConfigParams` / `mergeClickhouseConfig` / `GetConfig` | Task 7 |
| 逻辑集群警告 | Task 7 Step 3 |
| schema_ui | Task 9 |
| 前端 | Task 10 |
| 测试 | Task 1/2/3/4/5 内联 + Task 11 |

无遗漏。

**2. Placeholder scan**

Task 10（前端）没有给出具体代码 —— 这是因为 `../ckman-fe` 的组件结构尚未探索，硬编代码会是凭空捏造。改为给出**明确的行为规格 + 探索步骤 + 验证方式**，执行者按仓库现有模式实现。其余任务均有可直接落地的代码。

Task 8 Step 2 标注了「先确认现有请求体解码写法再照做」，而非直接假定 `DecodeRequestBody` 存在。

**3. Type consistency**

- `GenSelfSignedCert(hosts []string) (certPEM, keyPEM string, err error)` — Task 1 定义，Task 8 调用 ✓
- `ValidateCertKeyPair(certPEM, keyPEM string) error` — Task 1 定义，Task 7 调用 ✓
- `CertNotAfter(certPEM string) (string, error)` — Task 1 定义，Task 7 调用 ✓
- `genSelfSignedCert(hosts, notBefore, notAfter)` — Task 1 内部函数，仅测试调用 ✓
- `model.TlsConf{Cert, Key, SelfSigned, NotAfter}` — Task 2 定义，Task 3/6/7/9 使用，字段名一致 ✓
- `ckconfig.CertsDir(conf)` — Task 4 定义；Task 6 的下发路径 `path.Join(remotePath, "certs")` 与之等价（`remotePath` 取值规则相同）✓
- `model.SelfSignedCertReq/Rsp` — Task 8 定义并使用 ✓
