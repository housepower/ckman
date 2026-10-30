# 配置总览

CKMAN 的主配置文件为 `conf/ckman.hjson`（HJSON 格式，对人类友好），也支持 YAML。

::: tip HJSON
[HJSON](https://hjson.github.io/) 是 JSON 的扩展：支持注释、省略引号、宽松逗号。
:::

## 配置文件位置

| 安装方式 | 路径 |
| --- | --- |
| tar.gz | `<工作目录>/conf/ckman.hjson` |
| rpm | `/etc/ckman/conf/ckman.hjson` |

启动时也可通过 `-c` 指定：

```bash
ckman -c /path/to/ckman.hjson
```

## 配置结构

```hjson
{
  "encrypt_algorithm": "AESWITHHEXANDBASE64", // 新写入密文的算法，可设为 SM4
  "server":            { ... },   // HTTP 服务、鉴权、metrics
  "clickhouse":        { ... },   // ClickHouse 连接池
  "log":               { ... },   // 日志级别与切割
  "cron":              { ... },   // 定时任务
  "persistent_config": { ... },   // 持久层（mysql/postgres/dm8/local）
  "registry":          { ... },   // 注册中心选择（多实例时启用）
  "config_center":     { ... },   // 配置中心选择
  "nacos":             { ... }    // Nacos 连接参数，另有 consul / apollo
}
```

各小节配置项详见左侧侧边栏对应页面。

## 密码加密

任何密码字段都可以使用密文：

```bash
ckman --encrypt 123456
# 输出 E310E892E56801CED9ED98AA177F18E6
```

写入配置时用 `ENC()` 包裹：

```hjson
password: ENC(E310E892E56801CED9ED98AA177F18E6)
```

未包裹则视为明文。

默认算法为 `AESWITHHEXANDBASE64`。需要使用 SM4 时，在主配置**顶层**设置 `encrypt_algorithm: SM4`：

```hjson
{
  "encrypt_algorithm": "SM4",
  "server": { ... }
}
```

CKMAN 默认从**所选主配置文件的同一目录**读取 `.ckman.env`。tar.gz 安装对应 `<工作目录>/conf/.ckman.env`；RPM 安装对应 `/etc/ckman/conf/.ckman.env`。首次配置可以用 CKMAN 自带的 `ckmanctl keygen sm4` 生成密钥，无需在客户机器上安装 OpenSSL。命令输出 `CKMAN_SM4_KEY=<Base64字符串>`；Base64 解码后是 16 字节密钥。

RPM 安装首次启用 SM4 时，可执行以下命令创建密钥文件（文件已存在时不会覆盖）：

```bash
(
  set -eC
  umask 077
  ckmanctl keygen sm4 > /etc/ckman/conf/.ckman.env
  chown ckman:ckman /etc/ckman/conf/.ckman.env
  chmod 600 /etc/ckman/conf/.ckman.env
)
```

已有 SM4 密文时，**不要重新生成或覆盖原密钥**，否则现有密文无法解密。平安提供的 key 如果是 16 字节密钥的 Base64 编码，可直接写入 `.ckman.env`，不必转成十六进制。根据已有密钥的实际表示方式，**选择一行**：

| 已有密钥形式 | `.ckman.env` 中的写法 |
| --- | --- |
| 16 字节密钥的 Base64 文本（推荐） | `CKMAN_SM4_KEY=<Base64文本>` |
| 恰好 16 字节的原始二进制文件 | `CKMAN_SM4_KEY_FILE=/绝对路径/密钥文件` |
| 旧版 16 字节 ASCII 原文 | `CKMAN_SM4_KEY=<原文>` |
| 旧版 32 位十六进制文本 | `CKMAN_SM4_KEY=<hex>` 或 `CKMAN_SM4_KEY=hex:<hex>` |

`CKMAN_SM4_KEY_FILE` 读取的是**恰好 16 字节的原始二进制文件**，不会把文件内容当作十六进制或 Base64 文本解析。若同时设置 `CKMAN_SM4_KEY` 和 `CKMAN_SM4_KEY_FILE`，优先使用非空的 `CKMAN_SM4_KEY`。旧版 hex 和原文仅保留读取兼容，新生成的 key 统一使用 Base64。已有 Base64 key 若解码后不是 16 字节，必须先向提供方确认密钥派生规则，不能仅靠改写编码得到 SM4 密钥。

`.ckman.env` 不存在时继续使用进程环境变量；如果两处都设置，**进程环境变量优先**。服务以 `ckman` 用户运行时，该用户需要能读取 `.ckman.env` 和原始密钥文件。建议文件归 `ckman` 用户所有，权限设为 `0600`。文件存在但无法读取或解析时，启动会报错。终端中的 `export CKMAN_SM4_KEY=...` 不会传给通过 `systemctl restart` 启动的服务；使用 `.ckman.env` 即可避免额外修改 systemd 单元。

生成配置文件中的 SM4 密文时，指定相同的主配置路径，让命令读取同目录的 `.ckman.env`：

```bash
ckman -c /etc/ckman/conf/ckman.hjson --encrypt-algorithm SM4 --encrypt '123456'
```

把输出写为 `ENC(密文)`。新 SM4 密文以 `sm4:gcm:v1:` 开头，使用随机 nonce，同一明文每次生成的密文不同。`encrypt_algorithm` 决定**新写入**的算法；读取时按密文识别算法。原有无前缀的 AES `ENC(...)` 和集群密码仍可读取，无需在切换配置项前全部替换。要把存量集群密码统一转为 SM4，请使用 [`ckmanctl upgrade encryption`](/reference/ckmanctl#upgrade-encryption)；配置文件中的旧 `ENC(...)` 需按需手动替换。密钥缺失、长度错误或 SM4 密文无法解密时，启动会报错。

已有 key 的**编码格式**与已有密文的**加密格式**是两回事。CKMAN 的新格式是 SM4-GCM，密文包含随机 nonce 和认证标签，并以 `sm4:gcm:v1:` 标记。若配置文件中的密文由别的程序生成，还需确认它的算法模式、nonce、认证标签和密文编码与 CKMAN 一致；仅有相同的 key 不能保证 CKMAN 可以解密。

## 完整示例

```hjson
// ckman 配置文件
// 密码可使用 ENC(密文)，密文通过 ./ckman --encrypt 明文 获取

{
  "encrypt_algorithm": "AESWITHHEXANDBASE64",
  "server": {
    "port": 8808,
    "https": false,
    // certfile:
    // keyfile:
    "pprof": true,
    "session_timeout": 3600,
    "persistent_policy": "local",
    "task_interval": 5
    // public_key:
  },

  "log": {
    "level": "INFO",
    "max_count": 5,
    "max_size": 10,    // MB
    "max_age": 10      // day
  },

  "clickhouse": {
    "max_open_conns": 10,
    "max_idle_conns": 2,
    "conn_max_idle_time": 10
  },

  "cron": {
    "sync_logic_schema":    "0 * * * * ?",
    "watch_cluster_status": "0 */3 * * * ?",
    "sync_dist_schema":     "30 */10 * * * ?"
  },

  // 单机部署用 local 即可；多实例参见持久化配置页
  // "persistent_config": {
  //   "mysql": {
  //     "host": "127.0.0.1",
  //     "port": 3306,
  //     "user": "root",
  //     "password": "ENC(E310E892E56801CED9ED98AA177F18E6)",
  //     "database": "ckman_db"
  //   }
  // },

  "registry": { "type": "none" },
  "config_center": { "type": "none" },

  "nacos": {
    "hosts": ["127.0.0.1"],
    "port": 8848,
    "user_name": "nacos",
    "password": "ENC(A7561228101CB07938FAFF00C4444546)"
  }
}
```

## 修改后是否要重启

| 修改项 | 是否需要重启 |
| --- | --- |
| `server.port`, `https`, `certfile`, `keyfile` | 是 |
| `log.level` | 否（动态生效） |
| `cron.*` | 是 |
| `persistent_config`, `persistent_policy` | 是 |
| `encrypt_algorithm`, `.ckman.env` | 是 |
| `registry`, `config_center`, `nacos.*`, `consul.*`, `apollo.*` | 是 |

::: tip 用户密码
登录用户的密码不在 `ckman.hjson` 中，而是以哈希形式存放在持久层数据库的用户表里。首次启动生成的 `ckman` 口令只输出一次。
:::
