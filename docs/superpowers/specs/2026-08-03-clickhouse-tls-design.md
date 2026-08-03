# ClickHouse TLS/HTTPS 支持设计

日期：2026-08-03

## 背景与目标

ckman 目前的 `CKManClickHouseConfig.Secure` 只影响 **ckman 作为客户端**连接 ClickHouse 时是否走 TLS
（`common/ck.go:66`）。服务端侧完全没有 TLS 配置生成能力：`ckconfig/custom.go` 只写 `tcp_port`/`http_port`，
`ckconfig/metrika.go` 的 replica 固定写明文 `conf.Port`。

本次让 ckman 具备**部署与运维 TLS ClickHouse 集群**的能力：生成服务端 TLS 配置、下发证书、
支持明文集群在线切换为密文集群、支持证书到期后更换。

## 范围

**覆盖的通信面：**

- 客户端接入：`tcp_port_secure`(9440) / `https_port`(8443)
- 分布式查询：`remote_servers` 的 `<secure>1</secure>`

**明确不做：**

- 节点间复制加密（`interserver_https_port`），9009 保持明文
- ZooKeeper / clickhouse-keeper 连接加密
- 双向认证（客户端证书校验）
- CA 体系（自签名只签单张证书）
- 证书到期告警与定时巡检
- 每节点独立证书
- prometheus 指标端口（9363）加密

## 关键决策

| 决策点 | 结论 |
| --- | --- |
| 证书来源 | 用户上传；ckman 提供自签名证书生成 |
| 证书粒度 | 全集群共用一套 |
| 证书持久化 | 存进集群配置，随 cluster 落 repository；私钥加密 |
| 端口字段 | 复用 `Port`/`HttpPort`，不新增密文端口字段 |
| 明文端口 | 关闭，对 `tcp_port`/`http_port` 加 `remove` 属性 |
| 配置位置 | `custom.xml`（openSSL + 端口）、`metrika.xml`（remote_servers secure） |
| 证书到期 | 用户重传证书 → 走强制刷新配置逻辑，允许全集群停机 |
| 有效期管理 | 上传时拦截无效证书 + 页面只读回显到期时间；不做告警/巡检 |
| 逻辑集群一致性 | 约定要么全加密要么全不加密；ckman 只警告不阻断，容忍切换中间态 |
| 前端 | 本次一并实现 |

## 数据模型

`model/deploy_ck.go`：

```go
type CKManClickHouseConfig struct {
    ...
    Secure  bool     `json:"secure"`     // 语义扩展：集群整体是否为 TLS 集群
    TlsConf *TlsConf `json:"tlsConf,omitempty"`
}

type TlsConf struct {
    Cert       string `json:"cert"`       // server 证书 PEM
    Key        string `json:"key"`        // server 私钥 PEM，落库前 AesEncryptECB
    SelfSigned bool   `json:"selfSigned"` // 是否由 ckman 自签，仅用于回显提示
    NotAfter   string `json:"notAfter" swaggerignore:"true"` // 只出不入
}
```

- `Secure` 一个开关同时管服务端 TLS 与 ckman 客户端 TLS。导入的既有 TLS 集群靠
  `TlsConf == nil` 区分「只连不管证书」。
- `Key` 复用现有 `EncryptType` / `Pack` / `UnPack` 加解密路径，与 `Password`、`SshPassword` 同处理。
- `NotAfter` **不落库**，`GetConfig` 返回前从 `Cert` 现场解析填充，避免换证书后与实际值不一致。
- `Secure=false` 时 `TlsConf` 置 nil。

### 存储容量

`cluster` 表是单列 JSON blob，新增字段**不需要 DB schema 迁移**，且容量绰绰有余：

| 后端 | 列定义 | 容量 |
| --- | --- | --- |
| MySQL | `Config string`（无 size tag） | `longtext` |
| Postgres | `Config string` | `text` |
| DM8 | `Configuration dmSchema.Clob` + `size:1024000` | 1 MB CLOB |

实测证书体积（SAN 含 10 个节点 IP）：RSA 2048 证书 1245 B + 私钥 1704 B = 2.9 KB；
私钥经 AES + base64 后膨胀约 1.37 倍，实际入库约 **3.5 KB**。相对 1 MB 下限无压力。

## 证书生命周期

新文件 `common/cert.go`：

```go
func GenSelfSignedCert(hosts []string) (cert, key string, err error)
func ValidateCertKeyPair(cert, key string) error
```

**`GenSelfSignedCert`**：Go `crypto/x509` 生成单张自签证书。

- **RSA 2048**。ECDSA P-256 体积小三倍（0.9 KB vs 2.9 KB）也更快，
  但存储完全不是瓶颈（见下），故选兼容面最广的 RSA —— 老版本 clickhouse-client、
  JDBC driver、Poco SSL 对 RSA 支持最稳
- `IsCA = true`，`KeyUsage = CertSign | DigitalSignature | KeyEncipherment`
- `ExtKeyUsage = ServerAuth | ClientAuth` —— ClientAuth 必须有，分布式查询时本节点也作为客户端
- SAN = 全部节点 IP + hostname + `localhost` + `127.0.0.1`
- `NotBefore = now - 1h`（容忍节点时钟偏移），有效期 10 年

**`ValidateCertKeyPair`**：`tls.X509KeyPair` 解析并校验公私钥匹配；
再解析证书，`now < NotBefore` 或 `now > NotAfter` 直接报错拒绝，错误信息带上实际有效期区间。
防的是「传了过期证书 → 全停重启 → 集群起不来」。所有入库路径（上传、自签名回填）都必过此关。

**生命周期矩阵：**

| 场景 | 动作 |
| --- | --- |
| 新建集群勾 Secure | 上传或点「生成自签名」→ 校验 → 随 cluster 落库 → 部署时下发 |
| 明文集群改 TLS | 改配置页勾 Secure 并给证书 → `restart=true`，重启策略默认 full（全停） |
| 证书到期 | 只换 `TlsConf` 内容 → `PUT /ck/config?force=true&policy=full` |
| 扩容加节点 | 证书已在 cluster 配置里，新节点走同一下发路径自动获得 |
| 回退明文 | 取消 Secure → 删节点 certs 目录 + 配置恢复明文端口 |

## 配置生成

### custom.xml — 端口切换

`ckconfig/custom.go` 的 `root()`：

```go
if conf.Secure {
    output["tcp_port_secure"] = conf.Port      // 默认 9440
    output["https_port"]      = conf.HttpPort  // 默认 8443
    output["tcp_port[@remove=\"remove\"]"]  = ""
    output["http_port[@remove=\"remove\"]"] = ""
} else {
    output["tcp_port"]  = conf.Port
    output["http_port"] = conf.HttpPort
}
```

`remove` 属性沿用 `ckconfig/users.go:26` 抹 default 明文密码的现成写法。
`common/xml.go:214` 的 `parseTags` 已支持 xpath 风格属性 key，`Merge()` 里 `WritewithAttr` 也已存在，
**`common/xml.go` 不需要改动**。

### custom.xml — openSSL 节

`ckconfig/custom.go` 新增 `openssl(conf)`：

```xml
<openSSL>
  <server>
    <certificateFile>{certsDir}/server.crt</certificateFile>
    <privateKeyFile>{certsDir}/server.key</privateKeyFile>
    <verificationMode>none</verificationMode>
    <cacheSessions>true</cacheSessions>
    <disableProtocols>sslv2,sslv3</disableProtocols>
    <preferServerCiphers>true</preferServerCiphers>
  </server>
  <client>
    <loadDefaultCAFile>true</loadDefaultCAFile>
    <cacheSessions>true</cacheSessions>
    <disableProtocols>sslv2,sslv3</disableProtocols>
    <preferServerCiphers>true</preferServerCiphers>
    <invalidCertificateHandler>
      <name>AcceptCertificateHandler</name>
    </invalidCertificateHandler>
  </client>
</openSSL>
```

`<client>` 段不可省略：分布式查询时本节点要作为客户端连兄弟节点，
自签证书下没有 `AcceptCertificateHandler` 会直接握手失败。
`verificationMode: none` 表示不要求客户端证书（未做双向认证）。

### metrika.xml — remote_servers

`ckconfig/metrika.go` 的 `GenLocalMetrika` 与 `GenLogicMetrika`，在 replica 块内：
`conf.Secure` 为真时写 `<secure>1</secure>`，`port` 仍写 `conf.Port`（此时已是 9440）。

`GenLogicMetrika` 逐成员集群遍历，各写各的 `Secure`，
因此逻辑集群「部分加密」的中间态能正确生成 XML，不会导致配置错误 ——
只是中间态下跨集群查询不通，属于可接受范围。

## 证书下发

`deploy/ck.go` 的 `Config()` 阶段：

- 落盘目录 `path.Join(remotePath, "..", "certs")`
  —— rpm/deb 下为 `/etc/clickhouse-server/certs/`，tgz 下为 `<cwd>/etc/clickhouse-server/certs/`
- `server.crt` 权限 644，`server.key` 权限 **600**；`NeedSudo` 时 `chown clickhouse:clickhouse`
- 现有清理命令 `find config.d -maxdepth 1 -name '*.xml' -delete` 不波及 certs 目录
- `Secure=false` 时下发前 `rm -rf certs`，保证回退干净

**连接池无需额外处理**：`common/ck.go:84` 的 `orderId = sha256(opts)` 已包含 TLS 配置与端口，
`Secure` 变更后 orderId 随之改变，旧连接自动失效重建。

## API

### 新增

```
POST /api/v2/ck/cert/selfsigned
body: { "hosts": ["192.168.0.1", ...] }
resp: { "cert": "-----BEGIN CERTIFICATE-----...", "key": "-----BEGIN PRIVATE KEY-----..." }
```

只生成不落库。前端拿到后填回表单，随集群配置一起提交，
使「生成自签名」与「上传」在后端走同一条入库与校验路径。

**证书更新不新增接口**，走现有 `PUT /api/v1/ck/config/{clusterName}?force=true&policy=full`。

### controller/clickhouse.go

- `checkConfigParams`：`Secure=true` 时要求 `TlsConf` 非空，并调用 `ValidateCertKeyPair`
- `mergeClickhouseConfig`：新增
  `secureChanged := cluster.Secure != conf.Secure || !reflect.DeepEqual(cluster.TlsConf, conf.TlsConf)`，
  接入 `noChangedFn()`、`cc`（重下配置）、`restart`（重启）三处判断；
  merge 段补 `cluster.Secure` / `cluster.TlsConf` 赋值
- `GetConfig`：返回前从 `Cert` 解析 `NotAfter`；`Key` 按现有密码字段脱敏惯例返回掩码
- 逻辑集群提示：`Secure` 变更且该集群属于某 logic cluster 时，往 task 日志写警告，不阻断

### controller/schema_ui.go

两处 `Secure` 注册点（`:181` 部署页、`:1250` 配置页）：

- **配置页**（`:1250`）的 `Secure` 的 `Editable` 由 `"false"` 改为 `"true"`，
  这是明文集群在线切 TLS 的入口；部署页（`:181`）保持原样（新建时本就可填）
- `Port` / `HttpPort` 的 `Default` 保持 9000/8123，由前端在勾选 Secure 时联动为 9440/8443
- 新增 `TlsConf.Cert` / `TlsConf.Key`：`Visiable: "Secure == true"`，`InputType` 多行文本

## 前端

在 `../ckman-fe/` 修改（**不在 `frontend/` 子模块内改**），完成后更新父仓 submodule 指针。

1. Secure 勾选 → 展开证书区，端口默认值联动 9440/8443
2. 证书区：两个 textarea + 「从文件导入」（浏览器本地读文件填入 textarea，不走后端上传接口）
   + 「生成自签名证书」按钮（调 selfsigned 接口回填）
3. 集群配置页只读展示「证书有效期至 xxx」
4. 配置修改页：检测到 Secure 或证书变化时，重启策略默认选**全量停机**并给出提示文案

## 测试

- `common/cert_test.go`：自签名结果可被 `tls.X509KeyPair` 解析、SAN 含全部 host、
  cert/key 不匹配被拒、过期证书被拒、未生效证书被拒
- `ckconfig/custom_test.go` + 新 fixture `custom_secure_fake.xml`：
  验证 `tcp_port_secure` / `https_port` / `remove` 属性 / `openSSL` 完整节
- `ckconfig/metrika_test.go` + fixture：验证 `<secure>1</secure>`，
  以及逻辑集群下「一成员加密、一成员不加密」的混合场景能正确生成
- `repository/dbtest` 三后端回归（`go test -tags dbintegration ./repository/dbtest/`）：
  模型变更必跑。列容量已确认足够（见「存储容量」），此处验的是
  带 `TlsConf` 的集群配置在三个后端上的写入/读回/加解密往返一致
