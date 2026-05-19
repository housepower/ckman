# 错误码

CKMAN API 的统一错误码列表。在响应体的 `retCode` 字段返回。

::: tip 真相源
错误码定义在 `model/code.go`。若与代码不一致以代码为准。
:::

## 通用

| 错误码 | 常量 | 含义 |
| --- | --- | --- |
| `0000` | `E_SUCCESS` | 成功 |
| `9999` | `E_UNKNOWN` | 未知错误（一般是 panic） |

## 参数与数据

| 错误码 | 常量 | 含义 |
| --- | --- | --- |
| `5000` | `E_INVALID_PARAMS` | 参数非法 |
| `5001` | `E_INVALID_VARIABLE` | 变量非法 |
| `5002` | `E_DATA_MISMATCHED` | 数据不匹配 |
| `5003` | `E_DATA_CHECK_FAILED` | 数据校验失败 |
| `5004` | `E_DATA_NOT_EXIST` | 数据不存在 |
| `5005` | `E_DATA_DUPLICATED` | 数据重复 |
| `5006` | `E_DATA_EMPTY` | 数据为空 |

## 鉴权

| 错误码 | 常量 | 含义 |
| --- | --- | --- |
| `5020` | `E_JWT_TOKEN_EXPIRED` | JWT 已过期 |
| `5021` | `E_JWT_TOKEN_INVALID` | JWT 非法 |
| `5022` | `E_JWT_TOKEN_NONE` | 缺少 token |
| `5023` | `E_JWT_TOKEN_IP_MISMATCH` | token 与客户端 IP 不匹配 |
| `5024` | `E_CREAT_TOKEN_FAIL` | 创建 token 失败 |

## 用户

| 错误码 | 常量 | 含义 |
| --- | --- | --- |
| `5030` | `E_USER_VERIFY_FAIL` | 权限校验失败 |
| `5031` | `E_GET_USER_PASSWORD_FAIL` | 读取用户密码失败 |
| `5032` | `E_PASSWORD_VERIFY_FAIL` | 密码校验失败 |
| `5033` | `E_LOGIN_DISABLED` | 账号已禁用 |
| `5034` | `E_USER_ALREADY_EXISTS` | 用户名已存在 |
| `5035` | `E_INVALID_USERNAME` | 用户名不合法 |
| `5036` | `E_INVALID_POLICY` | 角色不合法 |
| `5037` | `E_FORBIDDEN_TARGET` | 目标用户受保护（如内置用户） |
| `5038` | `E_OLD_PASSWORD_MISMATCH` | 旧密码不匹配 |
| `5039` | `E_USER_NOT_FOUND` | 用户不存在 |

## 远程连接

| 错误码 | 常量 | 含义 |
| --- | --- | --- |
| `5100` | `E_SSH_CONNECT_FAILED` | SSH 连接失败 |
| `5101` | `E_SSH_EXECUTE_FAILED` | SSH 命令执行失败 |
| `5110` | `E_CH_CONNECT_FAILED` | ClickHouse 连接失败 |
| `5120` | `E_ZOOKEEPER_ERROR` | ZooKeeper 错误 |

## 配置与文件

| 错误码 | 常量 | 含义 |
| --- | --- | --- |
| `5200` | `E_CONFIG_FAILED` | 配置加载/写入失败 |
| `5201` | `E_FILE_NOT_EXIST` | 文件不存在 |
| `5202` | `E_UPLOAD_FAILED` | 上传失败 |
| `5203` | `E_DOWNLOAD_FAILED` | 下载失败 |

## 序列化

| 错误码 | 常量 | 含义 |
| --- | --- | --- |
| `5600` | `E_MARSHAL_FAILED` | 序列化失败 |
| `5601` | `E_UNMARSHAL_FAILED` | 反序列化失败 |

## 持久层

| 错误码 | 常量 | 含义 |
| --- | --- | --- |
| `5800` | `E_RECORD_NOT_FOUND` | 记录未找到（如集群不存在） |
| `5801` | `E_DATA_INSERT_FAILED` | 数据插入失败 |
| `5802` | `E_DATA_UPDATE_FAILED` | 数据更新失败 |
| `5803` | `E_DATA_DELETE_FAILED` | 数据删除失败 |
| `5804` | `E_DATA_SELECT_FAILED` | 数据查询失败 |
| `5805` | `E_TRANSACTION_DEGIN_FAILED` | 事务开启失败 |
| `5806` | `E_TRANSACTION_COMMIT_FAILED` | 事务提交失败 |
| `5807` | `E_TRANSACTION_ROLLBACK_FAILED` | 事务回滚失败 |

## ClickHouse 表

| 错误码 | 常量 | 含义 |
| --- | --- | --- |
| `5808` | `E_TBL_CREATE_FAILED` | 建表失败 |
| `5809` | `E_TBL_ALTER_FAILED` | 改表失败 |
| `5810` | `E_TBL_DROP_FAILED` | 删表失败 |
| `5811` | `E_TBL_EXISTS` | 表已存在 |
| `5812` | `E_TBL_NOT_EXISTS` | 表不存在 |
| `5813` | `E_TBL_BACKUP_FAILED` | 表备份失败 |
| `5814` | `E_TBL_RESTORE_FAILED` | 表恢复失败 |

## ClickHouse 异常码透传

当后端 ClickHouse 返回异常时，CKMAN 会**透传** ClickHouse 的异常码（4 位数字）到 `retCode`，并将异常消息附加到 `retMsg`。

例如：

```json
{
  "retCode": "0241",
  "retMsg": "执行失败: Memory limit (for query) exceeded: ...",
  "entity": null
}
```

ClickHouse 异常码完整列表参考 [ClickHouse 官方文档](https://clickhouse.com/docs/en/sql-reference/data-types/special-data-types/expression/)。

## 常见排查

| 错误码 | 可能原因 |
| --- | --- |
| `5022` | 请求未带 `token` header |
| `5023` | 反代时未透传客户端 IP，或换网络后未重新登录 |
| `5030` | 当前用户角色无权访问该接口 |
| `5100` | 节点 SSH 不通：检查端口、用户、密钥、网络 |
| `5110` | ClickHouse 服务挂了，或用户名密码错误 |
| `5800` | 操作的集群在 CKMAN 数据库中不存在（已被移除） |
