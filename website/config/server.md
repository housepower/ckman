# server

HTTP 服务、鉴权与基础设施相关配置。

## 配置项

| 字段 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `ip` | string | 默认路由 IP | 服务端绑定 IP，未指定则取默认路由 |
| `port` | int | `8808` | 监听端口 |
| `https` | bool | `false` | 是否启用 HTTPS |
| `certfile` | string | `conf/server.crt` | HTTPS 证书路径，启用 HTTPS 必填 |
| `keyfile` | string | `conf/server.key` | HTTPS 私钥路径，启用 HTTPS 必填 |
| `pprof` | bool | `true` | 是否暴露 `/debug/pprof/*` |
| `session_timeout` | int | `3600` | 会话超时秒数，超时后 token 失效 |
| `public_key` | string | — | RSA 公钥，用于跳过 JWT 的统一门户接入 |
| `swagger_enable` | bool | `false` | 是否开启 Swagger 文档 `/swagger/*` |
| `task_interval` | int | `5` | 异步任务扫描间隔（秒） |
| `pkg_path` | string | 工作目录 | 安装包存放路径 |
| `metric` | bool | `true` | 是否暴露 Prometheus metrics |
| `metric_path` | string | `/metrics` | metrics 路径 |
| `persistent_policy` | string | `local` | 持久化策略：`local` / `mysql` / `postgres` / `dm8` |

## 关键字段说明

### `ip`

如果有多张网卡，建议显式指定 `ip`，避免 CKMAN 选错。可通过环境变量 `HOST_IP` 覆盖。

### `https`

启用 HTTPS 时确保：

- `certfile` 和 `keyfile` 路径对 CKMAN 进程**可读**
- 证书未过期、域名匹配
- 防火墙放开新端口（默认仍是 8808）

### `session_timeout`

- 用户在前端无任何操作的超时时间
- 超时后会被强制返回登录页
- 实际有效期：每次请求都会刷新过期时间（滑动过期）

### `public_key`

统一门户接入场景使用：

1. 客户端将用户信息（含时间戳、有效期）用**私钥**加密
2. 在 HTTP header 中传递 `userToken: <加密内容>`
3. CKMAN 使用配置的 `public_key` 解密
4. 优先级**高于** JWT token

适合企业内部统一身份认证集成。

### `task_interval`

异步任务（部署、升级、销毁、节点增删等）由后台 runner 轮询触发。
间隔越短响应越快，但 CPU 占用略高。一般保持默认 5 秒即可。

### `persistent_policy`

详见[持久化配置](/config/persistent)。多实例部署必须为 `mysql` / `postgres` / `dm8`。

## 示例

```hjson
"server": {
  "ip": "10.0.0.1",
  "port": 8808,
  "https": true,
  "certfile": "/etc/ckman/conf/server.crt",
  "keyfile": "/etc/ckman/conf/server.key",
  "pprof": false,
  "session_timeout": 7200,
  "swagger_enable": true,
  "task_interval": 5,
  "persistent_policy": "postgres",
  "metric": true,
  "metric_path": "/metrics"
}
```
