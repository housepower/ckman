# nacos

Nacos 连接参数。Nacos 可以同时做注册中心和配置中心，由 [registry / config_center](/config/discovery) 选择是否使用。

## 配置项

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `enabled` | `false` | 旧配置开关：未写 `registry` / `config_center` 段时，为 `true` 等价于两者都选 nacos。新配置无需填写 |
| `hosts` | — | Nacos 服务地址列表（数组） |
| `port` | `8848` | Nacos 端口 |
| `user_name` | — | 登录用户名 |
| `password` | — | 密码，支持 `ENC()` 加密 |
| `namespace_id` | — | 指定 namespace ID |
| `cfg_namespace_id` | — | 配置中心使用的 namespace ID，不填则与 `namespace_id` 相同；`public` 表示默认 namespace |
| `sync_config` | `true` | 旧配置开关：为 `false` 时只用 Nacos 做注册中心 |
| `beat_interval` | `5000` | 心跳间隔（毫秒） |
| `group` | `DEFAULT_GROUP` | 注册服务所在的分组 |
| `data_id` | `ckman` | 配置的 dataId；未设置 `registry.service_name` 时也作为注册的服务名 |

## 示例

```hjson
"registry":      { "type": "nacos" },
"config_center": { "type": "nacos" },
"nacos": {
  "hosts": ["10.0.0.20", "10.0.0.21", "10.0.0.22"],
  "port": 8848,
  "user_name": "nacos",
  "password": "ENC(A7561228101CB07938FAFF00C4444546)",
  "namespace_id": "public",
  "beat_interval": 5000,
  "group": "DEFAULT_GROUP",
  "data_id": "ckman"
}
```

## 环境变量覆盖

| 变量 | 作用 |
| --- | --- |
| `NACOS_HOST` | 逗号分隔的 `host:port` 列表，覆盖 `hosts` 与 `port` |

## 与高可用的关系

`registry.type` 为 nacos 时：

- 每个实例向 Nacos 注册，发送 `beat_interval` 心跳
- Nacos 协调 master 选举
- 只有 master 实例执行定时任务（cron）
- 普通运维操作（部署/升级/查询）所有实例都可处理

详见[高可用部署](/deploy/high-availability)。

## 单机部署

单机部署**不需要**注册中心，`registry.type` 保持 `none` 即可。
