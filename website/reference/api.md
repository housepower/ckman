# API 接口

CKMAN 提供 RESTful API，所有接口的**唯一权威定义**是 Swagger，由代码注解自动生成。

<script setup>
import { withBase } from 'vitepress';
</script>

<div class="api-cta">
  <a class="api-cta__btn api-cta__btn--primary" :href="withBase('/reference/api-playground.html')">
    <span>📖</span> 浏览所有接口（API Playground）
  </a>
  <span class="api-cta__hint">
    或在自己的 ckman 实例上启用 <code>swagger_enable</code> 后访问
    <code>http://&lt;ckman-host&gt;:8808/swagger/index.html</code> 进行 try-it-out。
  </span>
</div>

<style scoped>
.api-cta {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 16px 20px;
  margin: 24px 0;
  border: 1px solid var(--vp-c-brand-3);
  border-radius: 10px;
  background: var(--vp-c-brand-soft);
}
.api-cta__btn {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  align-self: flex-start;
  padding: 8px 16px;
  border-radius: 8px;
  font-weight: 600;
  text-decoration: none !important;
  transition: filter 0.15s ease;
}
.api-cta__btn--primary {
  background: var(--vp-button-brand-bg);
  color: var(--vp-button-brand-text) !important;
}
.api-cta__btn--primary:hover {
  filter: brightness(0.92);
}
.api-cta__hint {
  font-size: 13px;
  color: var(--vp-c-text-2);
  line-height: 1.6;
}
.api-cta__hint code {
  font-size: 12px;
  padding: 1px 5px;
}
</style>

::: tip 不再手写 API 表
本页只描述接口设计原则、鉴权、响应结构与典型集成。具体每个接口的请求/响应字段以 Swagger 为准。
:::

## 接口前缀

所有 API 均位于 `/api/v1` 前缀下。

```
POST /api/login                       # 登录（不带版本前缀）
PUT  /api/logout                      # 登出
GET  /api/v1/<resource>               # 业务接口
```

## 鉴权

### 登录获取 Token

```http
POST /api/login
Content-Type: application/json

{
  "username": "ckman",
  "password": "<前端 hash 后的密码>"
}
```

返回：

```json
{
  "retCode": "0000",
  "retMsg": "ok",
  "entity": {
    "username": "ckman",
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
  }
}
```

::: warning 密码 hash
前端会先对密码做 hash 再传给服务端。直接用明文调用 `/api/login` 不会通过。集成调用方需要在自己一侧实现同样的 hash 算法，或使用统一门户 Token 方式（见下文）。
:::

### 后续请求带 Token

```http
GET /api/v1/ck/cluster
token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

Token 与**客户端 IP 绑定**，反代时务必设置 `X-Forwarded-For`。

### 统一门户 Token

适合企业内部系统集成：

```http
GET /api/v1/ck/cluster
userToken: <RSA 私钥加密后的内容>
```

CKMAN 用配置的 `server.public_key` 解密。**优先级高于** JWT。详见[server 配置](/config/server#public-key)。

### 登出

```http
PUT /api/logout
token: <你的 token>
```

调用后 token 立即失效。

## 统一响应结构

所有接口返回相同的 JSON 包裹：

```json
{
  "retCode": "0000",
  "retMsg": "ok",
  "entity": <实际数据>
}
```

| 字段 | 含义 |
| --- | --- |
| `retCode` | 业务返回码，`0000` 为成功，其他为失败 |
| `retMsg` | 返回消息，失败时会包含错误描述（含 ClickHouse 异常时会带具体错误码） |
| `entity` | 业务数据，失败时为 `null` |

错误码列表见[错误码](/reference/error-codes)。

## URL 路径占位

| 占位 | 含义 |
| --- | --- |
| `:clusterName` | 集群名 |
| `:taskId` | 任务 ID |
| `:policy_id` | 备份策略 ID |
| `:run_id` | 备份运行记录 ID |
| `:username` | 用户名 |

## 权限要求

不同接口对调用方有不同的角色要求，规则汇总在[用户与角色权限](/features/users/overview#详细权限矩阵)。

简要：

- **admin**：所有接口
- **ordinary**：表/分区/备份/DML 等运维操作 + guest 全部
- **guest**：所有 GET 查询 + 修改自己密码 + ping 节点

未授权访问返回 `5009 permission denied`。

## 完整接口列表

两种查阅方式：

- **文档站内浏览（只读）**：[API Playground](/reference/api-playground)，无需部署
- **本地 try-it-out**：在自己的 ckman 实例上启用后访问 `http://<ckman-host>:8808/swagger/index.html`

::: tip 启用本地 Swagger
默认 Swagger 未开启，在 `ckman.hjson` 中将 `server.swagger_enable` 设为 `true` 并重启即可。本地版本支持直接对接口发起请求。
:::

## 接口分类速览

代码注解中的接口分类（与 controller 对应）：

| Tag | 控制器 | 主要内容 |
| --- | --- | --- |
| `clickhouse` | `ClickHouseController` | 集群 CRUD、表、分区、查询、节点、rebalance 等 |
| `package` | `PackageController` | 安装包上传/查询/删除 |
| `deploy` | `DeployController` | 部署集群 |
| `metric` | `MetricController` | Prometheus 指标查询 |
| `zookeeper` | `ZookeeperController` | ZK 状态、复制队列 |
| `config` | `ConfigController` | CKMAN 版本、实例列表 |
| `task` | `TaskController` | 任务管理 |
| `data_manage` | `DataManageController` | 备份策略与运行记录 |
| `user` | `UserController` | 用户管理 |
| `node_override` | `NodeOverrideController` | 节点 override XML 配置 |
| `schema_ui` | `SchemaUIController` | 前端表单 JSON Schema |

## 集成示例

### Python

```python
import hashlib
import requests

# 登录
pwd_hashed = hashlib.md5("Ckman123456!".encode()).hexdigest()
r = requests.post("http://ckman:8808/api/login", json={
    "username": "ckman",
    "password": pwd_hashed,
})
token = r.json()["entity"]["token"]

# 调用业务接口
r = requests.get(
    "http://ckman:8808/api/v1/ck/cluster",
    headers={"token": token},
)
print(r.json()["entity"])
```

### curl

```bash
TOKEN=$(curl -s -X POST http://ckman:8808/api/login \
  -H "Content-Type: application/json" \
  -d '{"username":"ckman","password":"<hashed>"}' \
  | jq -r .entity.token)

curl http://ckman:8808/api/v1/ck/cluster -H "token: $TOKEN"
```
