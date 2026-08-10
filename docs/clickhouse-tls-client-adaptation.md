# ClickHouse TLS 集群的下游客户端适配

本文面向**已用 ckman 把集群切换为 TLS（Secure）之后**，需要继续正常读写该集群的下游系统，
覆盖两个最常见的接入方：clickhouse_sinker（Kafka → CH 写入）与 Java 应用（HTTP/JDBC 查询）。

服务端 TLS 的设计与实现见 `docs/superpowers/specs/2026-08-03-clickhouse-tls-design.md`，
本文不重复，只讲客户端要改什么。

---

## 一、开启 Secure 后服务端到底变了什么

这是所有适配动作的前提。ckman 在集群 `Secure=true` 时生成的配置：

| 项 | 明文集群 | TLS 集群 |
|------|---------|---------|
| Native 端口 | `tcp_port` 9000 | `tcp_port_secure` 9440，**`tcp_port` 被 `remove` 属性删除** |
| HTTP 端口 | `http_port` 8123 | `https_port` 8443，**`http_port` 被 `remove` 属性删除** |
| 分布式查询 | — | `remote_servers` 每个 replica 加 `<secure>1</secure>` |
| 证书落盘 | — | `<remotePath>/certs/server.crt`(644)、`server.key`(600) |
| interserver 复制 9009 | 明文 | **仍是明文** |
| ZooKeeper / Keeper 连接 | 明文 | **仍是明文** |
| prometheus 端口 9363 | 明文 | **仍是明文** |

`<remotePath>` 为 `/etc/clickhouse-server`（NeedSudo 部署）或 `<Cwd>/etc/clickhouse-server`（tgz 部署）。

**最关键的一条：明文端口是被摘掉的，不是与密文端口并存。**
这意味着切换没有灰度余地——所有客户端必须在同一个停机窗口内一起改完，
任何还在连 9000/8123 的进程在切换完成的瞬间就会 `connection refused`。

### 自签名证书的形状

若使用 ckman 的「生成自签名证书」（`POST /api/v1/ck/cert/selfsigned`，实现见 `common/cert.go:36`）：

| 属性 | 值 |
|------|-----|
| 算法 | RSA 2048 |
| Subject CN | `ckman-self-signed` |
| `IsCA` | `true`，且自签 —— **它自己就是根证书**，可直接导入信任库，不需要额外的 CA 文件 |
| ExtKeyUsage | `ServerAuth` + `ClientAuth`（分布式查询时本节点也作为客户端连兄弟节点） |
| SAN | **请求里传入的 hosts** + `localhost` + `127.0.0.1` |
| 有效期 | `now - 1h` 起，10 年 |

证书全集群共用一套，随 cluster 配置落库；私钥加密存储，`GetConfig` 返回时掩码，
**证书（`cert`）原文可以从集群配置页/接口取回**，下游要用的就是它。

### 两个容易踩的前提

1. **ckman 自身不校验证书。** `model/deploy_ck.go:579` 把 `InsecureSkipVerify` 硬编码为 `true`。
   所以「ckman 页面上集群节点全绿」**不能**推出「你的客户端能通过证书校验」。
2. **扩容新增的节点不会重新签发证书。** 新节点会通过同一下发路径拿到已有的 `server.crt`，
   但那张证书的 SAN 里没有新节点的 IP。做严格校验的客户端连新节点会直接失败。
   扩容前就要规划好 SAN，或在扩容后重新生成证书并走一次强制刷新配置。

---

## 二、clickhouse_sinker 写入适配

sinker 已原生支持，**不需要改代码**（`config/config.go:117`、`pool/conn.go:245`）。

### 配置改动

```hjson
clickhouse: {
    hosts: [
        ["192.168.0.1", "192.168.0.2"]
        ["192.168.0.3", "192.168.0.4"]
    ]
    port: 9440              // native 协议；若 protocol 为 http 则填 8443
    protocol: native
    db: default
    username: default
    password: "xxxxxx"

    secure: true
    insecureSkipVerify: true

    retryTimes: 0
    maxOpenConns: 8
}
```

必改的就是 `port` 和 `secure`，其余保持原样。

### 三个必须注意的点

**1. `insecureSkipVerify` 在自签场景下基本只能填 `true`。**

`pool/conn.go:245-248` 只构造了一个空的 `tls.Config` 然后设置 `InsecureSkipVerify`，
**没有暴露 `RootCAs` / 自定义 CA 路径的配置项**：

```go
if chCfg.Secure {
    tlsConfig := &tls.Config{}
    tlsConfig.InsecureSkipVerify = chCfg.InsecureSkipVerify
    sc.opts.TLS = tlsConfig
}
```

如果要求 `insecureSkipVerify: false` 做真实校验，唯一的办法是把 `server.crt` 装进
**sinker 所在机器的系统信任库**（Go 在 `RootCAs == nil` 时回落到系统根证书池）：

```bash
# RHEL / CentOS
cp server.crt /etc/pki/ca-trust/source/anchors/ckman-ch.crt
update-ca-trust

# Debian / Ubuntu
cp server.crt /usr/local/share/ca-certificates/ckman-ch.crt
update-ca-certificates
```

装完需要重启 sinker 进程才会生效（Go 的系统根池在进程内有缓存）。

**2. `hosts` 里写的名字必须落在证书 SAN 里。**

`pool/conn.go:218-222` 有个不显眼的分支：

```go
// Changing hostnames to IPs breaks TLS connections in many cases
if !chCfg.Secure {
    if ips2, err := util.GetIP4Byname(ip); err == nil {
        ip = ips2[0]
    }
}
```

`Secure=false` 时 sinker 会把 hostname 解析成 IP 再连；`Secure=true` 时保留原样直接用。
所以只要开了严格校验，`hosts` 里填的是什么（IP、hostname 还是 VIP），
证书 SAN 里就必须有什么。

ckman 自签证书的 SAN 只包含调用生成接口时传入的 hosts。
如果 sinker 走 VIP 或域名接入，要么在调 `/ck/cert/selfsigned` 时把 VIP/域名一并塞进 `hosts` 数组，
要么改用企业 CA 签发的证书上传。

**3. 每个 shard 的每个 replica 都要能连通。**

sinker 的 `InitClusterConn` 对 `hosts` 里的每台机器建连接，端口是配置里那唯一一个 `port`。
切换时不能漏掉任何一台节点，否则 `NextGoodReplica` 会直接报错、任务起不来。

---

## 三、Java 应用 HTTP 查询适配

访问地址从 `http://host:8123` 改为 `https://host:8443`。校验方式有两条路，**推荐 A**。

### 方案 A：严格校验（推荐）

自签证书本身即根证书，直接作为信任锚导入即可。

**方式 1 —— JDBC 参数直接指 PEM（最省事，不用建 keystore）：**

```
jdbc:clickhouse://192.168.0.1:8443/default?ssl=true&sslmode=strict&sslrootcert=/etc/ch/server.crt
```

**方式 2 —— 导入 JKS 信任库：**

```bash
keytool -importcert -alias ckman-ch -file server.crt \
        -keystore ch-truststore.jks -storepass changeit -noprompt

# 校验导入结果
keytool -list -v -keystore ch-truststore.jks -storepass changeit | head -20
```

```
-Djavax.net.ssl.trustStore=/etc/ch/ch-truststore.jks
-Djavax.net.ssl.trustStorePassword=changeit
```

> JVM 参数方式是**全局生效**的，会影响该 JVM 内所有 HTTPS 调用（包括调其他外部服务）。
> 如果应用还要访问公网 HTTPS，用这种方式需要把系统 cacerts 一起合并进去，
> 否则其他调用会集体失败。多数情况下方式 1 更安全。

**驱动版本差异：**

| 驱动 | 参数写法 |
|------|---------|
| `ru.yandex.clickhouse` 0.2.x | `ssl=true&sslmode=strict&sslrootcert=<path>` |
| `com.clickhouse.jdbc` 0.3.2 ~ 0.7.x | 同上（`sslmode` 取 `strict` / `none`） |
| clickhouse-java 0.8+ client-v2 | 代码里 `.setRootCertificate("/etc/ch/server.crt")` |

以你们实际引入的驱动版本为准，0.2.x ~ 0.7.x 之间这几个参数名是兼容的。

**严格模式下，连接串里的 host 必须在 SAN 内。**
Java 的 hostname verification 只看 SAN、不看 CN，而这张证书的 CN 是 `ckman-self-signed`，
完全靠 SAN 撑住。用 IP 直连就写生成证书时传入的那些 IP。

### 方案 B：跳过校验（仅限可信内网）

```
jdbc:clickhouse://192.168.0.1:8443/default?ssl=true&sslmode=none
```

只加密不认证，能挡住被动嗅探，挡不住中间人。用之前确认这个风险在你们的网络边界内可接受。

### 非 JDBC 的情况

如果应用是用 `RestTemplate` / `OkHttp` / `HttpClient` 直接打 ClickHouse 的 HTTP 接口，
改动就是 scheme 换 `https`、端口换 8443，再按方案 A 或 B 配置 `SSLContext` / `TrustManager`。
证书导入 JKS 的做法与上面一致。

---

## 四、其他下游与切换流程

### 别漏掉的组件

TLS 切换是**全停**级别的操作（`PUT /api/v1/ck/config/{clusterName}?force=true&policy=full`）：
端口和 `remote_servers` 的 `secure` 标记同时变化，滚动重启期间新老节点端口对不上会互相连不通。
请在同一个停机窗口内把下面这些一起改完：

- `clickhouse-client` → 加 `--secure --port 9440`
- Grafana 的 ClickHouse 数据源 → 端口 + TLS 开关 + 证书
- chproxy / 自研查询代理
- ckmigrate、ch2s3 等周边工具
- 任何硬编码了 8123 / 9000 的脚本、定时任务、健康检查

### 逻辑集群的约束

逻辑集群要求**要么全部成员加密、要么全部不加密**。
ckman 在检测到成员集群加密状态不一致时只写警告日志、不阻断，
但中间态下跨集群的分布式查询是不通的，不要长期停在这个状态。

---

## 五、验证与排错

### 切换后先做的几项验证

```bash
# 1. 密文端口在监听，且证书能取回
openssl s_client -connect 192.168.0.1:9440 -showcerts </dev/null 2>/dev/null | head -20

# 2. 看清楚 SAN 里到底有哪些名字 —— 排查校验失败第一步就查这个
openssl x509 -in server.crt -noout -text | grep -A1 "Subject Alternative Name"

# 3. 证书到期时间
openssl x509 -in server.crt -noout -enddate

# 4. HTTPS 端口连通（-k 跳过校验）
curl -k https://192.168.0.1:8443/ping

# 5. 带证书做严格校验，这一条过了 Java 侧基本就没问题
curl --cacert server.crt https://192.168.0.1:8443/ping

# 6. native 端口
clickhouse-client --secure --host 192.168.0.1 --port 9440 \
                  --user default --password 'xxx' --query "SELECT 1"

# 7. 分布式查询链路（验证 remote_servers 的 secure 标记生效）
clickhouse-client --secure --port 9440 --query \
  "SELECT hostName() FROM clusterAllReplicas('<cluster>', system.one)"
```

### 常见报错对照

| 报错 | 原因 | 处理 |
|------|------|------|
| `Connection refused` 到 9000 / 8123 | 明文端口已被 `remove`，客户端没改端口 | 改成 9440 / 8443 |
| Go：`tls: first record does not look like a TLS handshake` | 对端不是 TLS 端口，或 `secure` 配了但连的是明文口 | 核对端口与 `secure` 是否匹配 |
| Go：`x509: certificate signed by unknown authority` | 未装信任库且 `insecureSkipVerify=false` | 装系统信任库，或临时置 `true` |
| Go：`x509: cannot validate certificate for x.x.x.x because it doesn't contain any IP SANs` | 连接用的地址不在 SAN 里 | 重新签发证书并补全 SAN |
| Java：`PKIX path building failed: unable to find valid certification path` | 证书没导入信任库 | 见方案 A |
| Java：`No subject alternative names matching IP address x.x.x.x found` | 同上，SAN 不含该地址 | 重新签发证书补全 SAN，或临时 `sslmode=none` |
| Java：`Certificate for <host> doesn't match any of the subject alternative names` | 用域名连但 SAN 里只有 IP | 生成证书时把域名一并传入 `hosts` |
| CH 服务端日志：`Poco::Exception. Code: 1000, e.code() = 0, SSL Exception` | 证书/私钥不匹配或文件权限不对 | 检查节点上 `certs/` 下两个文件与权限（644 / 600） |
| 集群启动失败，日志报证书过期 | 下发了过期证书 | ckman 入库前有 `ValidateCertKeyPair` 拦截；若是自带证书过期，重新上传后走强制刷新 |

### 排查顺序建议

1. 先用 `curl -k` / `openssl s_client` 确认**端口通、TLS 握手成功** —— 这一步失败说明是服务端或端口问题，与客户端配置无关。
2. 再用 `curl --cacert server.crt` 确认**证书校验能过** —— 这一步失败基本都是 SAN 不匹配，直接看第 2 条命令的输出。
3. 最后才排查客户端自己的参数写法。

---

## 六、一个可选的改进项

sinker 的 `ClickHouseConfig` 缺少 CA 证书路径配置，导致自签名场景下只能二选一：
全局改系统信任库，或者 `insecureSkipVerify: true`。

如果对这条写入链路的加密强度有要求，可以在 `config/config.go` 的 `ClickHouseConfig`
加一个 `CaCertFile string` 字段，在 `pool/conn.go:246` 处填充 `tlsConfig.RootCAs`。
改动量很小，且与现有配置向后兼容（不填时行为不变）。
