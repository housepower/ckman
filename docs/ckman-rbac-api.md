# CKMAN RBAC 用户 / Profile 管理接口文档

面向 ClickHouse 集群的用户（User）与设置档（Settings Profile）管理接口。所有操作通过 `ON CLUSTER` 下发到集群全部节点。

- 版本：`ckman v4.0.0-90-g05a52b0` 起
- 源码：`controller/rbac.go`、`service/clickhouse/rbac.go`、`model/rbac.go`、`router/v1.go`

---

## 1. 通用约定

### 1.1 Base URL

```
http://<ckman-host>:<port>/api/v1
```

路径中的 `{clusterName}` 为 ckman 中登记的集群名。

### 1.2 鉴权

除 `/api/login` 外所有接口都需在请求头携带 JWT `token`（登录接口返回，且与登录时的客户端 IP 绑定）：

```
token: <jwt-token>
Content-Type: application/json
```

获取 token 示例：

```bash
curl -s -H "Content-Type: application/json" \
  http://<host>:<port>/api/login \
  -d '{"username":"ckman","password":"<md5-password>"}' \
  | jq -r '.entity.token'
```

### 1.3 统一响应结构

所有接口返回 HTTP 200，业务状态由 `retCode` 表达：

```json
{
  "retCode": "0000",
  "retMsg": "success",
  "entity": null
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `retCode` | string | `0000` 表示成功；非 `0000` 为错误码 |
| `retMsg` | string | 提示信息，失败时会拼接底层错误/ClickHouse 异常详情 |
| `entity` | any | 业务数据；失败时为 `null` |

### 1.4 错误码

| retCode | 常量 | 含义 | 触发场景 |
|---------|------|------|----------|
| `0000` | E_SUCCESS | 成功 | — |
| `5000` | E_INVALID_PARAMS | 参数不合法 | 请求体 JSON 解析失败 |
| `5801` | E_DATA_INSERT_FAILED | 数据插入失败 | 创建用户 / profile 失败 |
| `5802` | E_DATA_UPDATE_FAILED | 数据更新失败 | 修改用户 / profile 失败 |
| `5803` | E_DATA_DELETE_FAILED | 数据删除失败 | 删除用户 / profile 失败 |
| `5804` | E_DATA_SELECT_FAILED | 数据查询失败 | 列表 / 查询失败 |

> ClickHouse 侧异常（如权限不足、名称已存在等）会被解码后拼进 `retMsg`，此时 `retCode` 可能是 ClickHouse 的错误码。

### 1.5 通用校验规则（service 层）

- **标识符**：用户名、profile 名、setting 键、name 过滤值必须匹配 `^[a-zA-Z0-9_]+$`，否则报错。
- **保留用户**：`default`（大小写不敏感）不可被修改或删除。
- **网络**：创建用户时 `networks` 为空则默认 `::/0`（允许所有来源）。

---

## 2. 数据模型

### RbacUserCreateReq（创建用户请求）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 用户名，`^[a-zA-Z0-9_]+$` |
| `password` | string | 是 | 明文密码，不可为空 |
| `access` | string | 是 | `readonly` 或 `readwrite` |
| `profile` | string | 否 | 绑定的 settings profile 名 |
| `networks` | []string | 否 | 允许来源 IP/网段，缺省 `["::/0"]` |

### RbacUserAlterReq（修改用户请求，字段均可选，仅修改传入项）

| 字段 | 类型 | 说明 |
|------|------|------|
| `password` | string | 传入则重置密码 |
| `access` | string | 传入则重置权限（`readonly`/`readwrite`） |
| `profile` | string | 传入则重新绑定 profile |

> 三个字段全空时报错 `nothing to alter`。

### RbacUserInfo（用户信息，List/Get 返回）

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | string | 用户名 |
| `access` | string | 由 `system.grants` 反推：含写权限（ALL/INSERT/ALTER/CREATE/DROP/TRUNCATE/OPTIMIZE/DELETE/UPDATE）则为 `readwrite`，否则 `readonly` |
| `profile` | string | 绑定的 profile，未绑定为 `""` |
| `networks` | []string | 允许来源（当前 List/Get 返回中该字段可能为空） |

### RbacProfileCreateReq（创建/修改 profile 请求）

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | profile 名，`^[a-zA-Z0-9_]+$` |
| `settings` | object | 创建可空 / 修改必填 | setting 键值对，键须合法标识符，值支持 string / bool / number |

> 值类型映射：`bool → 1/0`，整数 float 去小数，字符串加引号转义。修改（Alter）时 `settings` 为空报错 `nothing to alter: settings is empty`。

### RbacProfileInfo（profile 信息，List/Get 返回）

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | string | profile 名 |
| `settings` | object | 从 `system.settings_profile_elements` 查出的键值（值以字符串返回） |

---

## 3. 用户管理接口

### 3.1 创建用户

```
POST /api/v1/ck/rbac/user/{clusterName}
```

请求体：`RbacUserCreateReq`

```json
{
  "name": "alice",
  "password": "123456",
  "access": "readonly",
  "profile": "readonly_p",
  "networks": ["10.0.0.0/8", "192.168.1.1"]
}
```

行为：下发 `CREATE USER ... ON CLUSTER ... IDENTIFIED BY ... HOST IP ... [SETTINGS PROFILE ...]`，再按 `access` 授权：
- `readonly` → `GRANT SELECT ON *.*`
- `readwrite` → `GRANT CURRENT GRANTS ON *.*` 后 `REVOKE ACCESS MANAGEMENT`（授予读写但剥离账号管理权限）

> ⚠️ **版本要求**：`readwrite` 依赖 `GRANT CURRENT GRANTS` 语法，要求 ClickHouse **≥ 23.5**。低于该版本创建 readwrite 用户（或 3.4 中把用户改为 readwrite）会被 ckman 直接拒绝，返回 `access 'readwrite' requires ClickHouse >= 23.5 ...`。`readonly` 建用户与 profile 操作不受此限制（20.4+ 即可）。

响应：`entity` 为 `null`。

```bash
curl -X POST http://<host>:<port>/api/v1/ck/rbac/user/test_cluster \
  -H "token: <jwt>" -H "Content-Type: application/json" \
  -d '{"name":"alice","password":"123456","access":"readonly","profile":"readonly_p"}'
```

### 3.2 列出用户

```
GET /api/v1/ck/rbac/user/{clusterName}?name={filter}
```

Query 参数：`name`（可选），按 `LIKE %name%` 模糊过滤。

响应 `entity`：`RbacUserInfo` 数组。

```json
{
  "retCode": "0000",
  "retMsg": "success",
  "entity": [
    {"name": "alice", "access": "readonly", "profile": "readonly_p", "networks": null}
  ]
}
```

### 3.3 查询单个用户

```
GET /api/v1/ck/rbac/user/{clusterName}/{name}
```

响应 `entity`：单个 `RbacUserInfo`。用户不存在时报错 `user {name} not found`（retCode `5804`）。

### 3.4 修改用户

```
PUT /api/v1/ck/rbac/user/{clusterName}/{name}
```

请求体：`RbacUserAlterReq`（仅修改传入字段）

```json
{ "password": "newpass", "access": "readwrite" }
```

行为：`access` 变更会先 `REVOKE ALL` 再按新权限重新授权。保留用户 `default` 拒绝修改。

### 3.5 删除用户

```
DELETE /api/v1/ck/rbac/user/{clusterName}/{name}
```

行为：`DROP USER IF EXISTS ... ON CLUSTER ...`。保留用户 `default` 拒绝删除。

---

## 4. Settings Profile 管理接口

### 4.0 关于 settings 配置项（透传）

`settings` 是**透传**字段：ckman **不维护白名单**，仅做两层校验——

1. key 必须匹配 `^[a-zA-Z0-9_]+$`（防注入）；
2. value 类型限 string / bool / number（bool 自动转 `1/0`）。

其余合法性由 ClickHouse 在执行 `CREATE/ALTER SETTINGS PROFILE` 时校验：传入不存在的项会返回 `UNKNOWN_SETTING`（错误信息会拼进 `retMsg`）。

**完整可用项**以目标集群的 `system.settings` 表为准，可直接查询获取名称、当前值、类型与说明：

```sql
SELECT name, value, type, description FROM system.settings ORDER BY name
```

**常用配置项参考**——以下为 ckman 部署/管理集群时 profile 表单暴露的项（`ckconfig/users.go` + `model.Profile`），可作为最常用的推荐集（取值范围以 CH 版本为准）：

| setting | 类型 | 说明 |
|---------|------|------|
| `readonly` | number | 只读级别：`0` 读写 / `1` 只读 / `2` 只读但允许改 settings |
| `allow_ddl` | number | 是否允许 DDL：`0` 禁止 / `1` 允许 |
| `max_threads` | number | 单查询最大线程数 |
| `max_memory_usage` | number | 单查询最大内存（字节） |
| `max_memory_usage_for_all_queries` | number | 该用户所有查询的总内存上限（字节） |
| `max_execution_time` | number | 单查询最大执行时间（秒） |
| `max_bytes_to_read` | number | 单查询从表可读的最大未压缩字节数，`0` 不限 |
| `max_rows_to_read` | number | 单查询从表可读的最大行数，`0` 不限 |
| `max_partitions_to_read` | number | 单查询可访问的最大分区数，`-1` 不限 |
| `max_concurrent_queries_for_user` | number | 该用户最大并发查询数，`0` 不限 |
| `max_result_rows` | number | 返回结果最大行数，`0` 不限 |
| `max_result_bytes` | number | 返回结果最大字节数（未压缩） |
| `max_bytes_before_external_group_by` | number | GROUP BY spill 到磁盘的阈值（字节），`0` 禁用外部聚合 |
| `max_bytes_before_external_sort` | number | ORDER BY spill 到磁盘的阈值（字节），`0` 禁用外部排序 |
| `force_index_by_date` | number | 强制查询使用日期索引，未命中拒绝执行：`0`/`1` |
| `force_primary_key` | number | 强制查询使用主键，未命中拒绝执行：`0`/`1` |
| `s3_max_get_rps` | number | S3 GET 单查询限速（每秒请求数），`0` 不限 |

> 以上即 ckman 前端 profile 表单可调的字段；其余 CH 支持的 setting 仍可透传，完整列表以集群 `system.settings` 为准。

### 4.1 创建 profile

```
POST /api/v1/ck/rbac/profile/{clusterName}
```

请求体：`RbacProfileCreateReq`

```json
{
  "name": "readonly_p",
  "settings": {
    "max_memory_usage": 10000000000,
    "readonly": 1,
    "max_execution_time": 60
  }
}
```

行为：`CREATE SETTINGS PROFILE ... ON CLUSTER ... [SETTINGS k=v, ...]`。`settings` 可为空（创建一个无设置的空 profile）。

### 4.2 列出 profile

```
GET /api/v1/ck/rbac/profile/{clusterName}?name={filter}
```

响应 `entity`：`RbacProfileInfo` 数组（含各自 settings）。

### 4.3 查询单个 profile

```
GET /api/v1/ck/rbac/profile/{clusterName}/{name}
```

响应 `entity`：单个 `RbacProfileInfo`。不存在时报错 `profile {name} not found`。

```json
{
  "retCode": "0000",
  "retMsg": "success",
  "entity": {
    "name": "readonly_p",
    "settings": {"max_memory_usage": "10000000000", "readonly": "1"}
  }
}
```

### 4.4 修改 profile

```
PUT /api/v1/ck/rbac/profile/{clusterName}/{name}
```

请求体：`RbacProfileCreateReq`（`name` 以路径为准，`settings` 必填非空）

```json
{ "settings": { "max_memory_usage": 20000000000 } }
```

行为：`ALTER SETTINGS PROFILE ... ON CLUSTER ... SETTINGS ...`。`settings` 为空报错 `nothing to alter: settings is empty`。

### 4.5 删除 profile

```
DELETE /api/v1/ck/rbac/profile/{clusterName}/{name}
```

行为：`DROP SETTINGS PROFILE IF EXISTS ... ON CLUSTER ...`。

---

## 5. 接口一览

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/ck/rbac/user/{clusterName}` | 创建用户 |
| GET | `/api/v1/ck/rbac/user/{clusterName}` | 列出用户（`?name=` 过滤） |
| GET | `/api/v1/ck/rbac/user/{clusterName}/{name}` | 查询单个用户 |
| PUT | `/api/v1/ck/rbac/user/{clusterName}/{name}` | 修改用户 |
| DELETE | `/api/v1/ck/rbac/user/{clusterName}/{name}` | 删除用户 |
| POST | `/api/v1/ck/rbac/profile/{clusterName}` | 创建 profile |
| GET | `/api/v1/ck/rbac/profile/{clusterName}` | 列出 profile（`?name=` 过滤） |
| GET | `/api/v1/ck/rbac/profile/{clusterName}/{name}` | 查询单个 profile |
| PUT | `/api/v1/ck/rbac/profile/{clusterName}/{name}` | 修改 profile |
| DELETE | `/api/v1/ck/rbac/profile/{clusterName}/{name}` | 删除 profile |
