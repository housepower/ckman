# ckman RBAC 用户/Profile 轻量管理接口 — 设计

- 日期:2026-07-06
- 状态:已评审,待实现
- 范围:给 ckman 增加一组后端 API,让调用方通过接口在 CH 集群上动态管理 **用户** 和 **settings profile**。只做后端接口,不涉及前端。

## 1. 背景

ckman 当前的 CH 用户是**部署时通过 xml 生成**的(`ckconfig/users.go` + `conf.UsersConf.Users`),运行时无法动态增删。`controller/user.go` 是 ckman 自身的登录用户(JWT),与 CH 用户无关。

本设计在 CH 的 SQL RBAC 之上包一层"只读/读写"的简化语义,提供动态管理 CH 用户与 profile 的 HTTP 接口。

## 2. 目标与非目标

### 目标
1. 创建用户,支持 **只读(readonly)** 或 **读写(readwrite)** 两种权限。
2. 创建 settings profile。
3. 给用户绑定/切换不同的 profile。
4. 两类对象(user / profile)均支持 Create / List / Get / Update / Delete。

### 非目标(YAGNI,本期不做)
- role / quota / grant 的独立对外接口(readwrite 的授权在建 user 时内部用 GRANT 完成,不对外暴露)。
- 按数据库/业务粒度的分权(本期权限范围固定为整实例 `ON *.*`)。
- 前端界面。
- 在 ckman 持久层镜像存储 RBAC 对象。

## 3. 关键决策(已评审确认)

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 对象范围 | 仅 user + profile | 覆盖全部实际需求,接口面最小 |
| 集群同步 | `ON CLUSTER '{cluster}'` | CH 官方机制,复用现有 DDL 队列(依赖 ZK/Keeper),与现有建表逻辑一致,无新部署前提 |
| 真相源 | CH 系统表为唯一真相源 | 零持久层改动,不触发多后端 dbtest;永不漂移 |
| 权限范围 | 整实例 `ON *.*` | 场景是"一个用户管全库",用一个 readonly 开关表达 |
| readwrite 权限 | `GRANT CURRENT GRANTS` 后 `REVOKE ACCESS MANAGEMENT`(GRANT ALL 会因授权账号缺 grant option 失败,故用 CURRENT GRANTS;仍 REVOKE ACCESS MANAGEMENT 兜底) | 覆盖除管理权限外的所有(含未来新增普通权限),挖掉 access management 防止读写用户自造管理员 |
| API 版本/路由 | 注册进现有 `router/v1.go`,前缀 `/api/v1/ck/rbac/...` | v2 router 尚无骨架,避免额外风险 |

## 4. 架构

新增文件(不改动现有逻辑):

- `service/clickhouse/rbac.go` —— 纯 CH 交互:拼 SQL、`ck.Conn.Exec` 执行 DDL、查系统表做 List/Get。
- `controller/rbac.go` —— HTTP handler:参数校验 → 调 service → `WrapMsg`。
- `model/rbac.go` —— 请求/响应结构体。
- `router/v1.go` 的 `InitRouterV1` —— 注册路由。

DDL 执行复用现有模式:`clickhouse.GetCkService(clusterName)` 拿连接,`ck.Conn.Exec(query)` 执行。ckman 连接身份是集群配置的管理员用户(`default`,`access_management=1`),有权执行这些 DDL。

## 5. 接口清单

```
# ---- User ----
POST   /api/v1/ck/rbac/user/:clusterName            创建用户
GET    /api/v1/ck/rbac/user/:clusterName[?name=]    列出用户(可按名称模糊筛选)
GET    /api/v1/ck/rbac/user/:clusterName/:name      查单个用户(含 profile/权限)
PUT    /api/v1/ck/rbac/user/:clusterName/:name      修改(改密码 / 换 profile / 改读写权限)
DELETE /api/v1/ck/rbac/user/:clusterName/:name      删除用户

# ---- Profile ----
POST   /api/v1/ck/rbac/profile/:clusterName         创建 profile
GET    /api/v1/ck/rbac/profile/:clusterName[?name=] 列出 profile(可按名称模糊筛选)
GET    /api/v1/ck/rbac/profile/:clusterName/:name   查单个 profile
PUT    /api/v1/ck/rbac/profile/:clusterName/:name   修改 profile
DELETE /api/v1/ck/rbac/profile/:clusterName/:name   删除 profile
```

**List 名称筛选**:两个 List 接口均支持可选 query 参数 `?name=<关键字>`,做模糊匹配(`WHERE name LIKE '%<关键字>%'`);不传则返回全部。`name` 关键字同样做 `[a-zA-Z0-9_]` 白名单校验防注入。

### 5.1 请求/响应体

**创建用户 `POST .../user/:clusterName`**
```json
{
  "name": "alice",
  "password": "xxxxxx",
  "access": "readonly",        // readonly | readwrite
  "profile": "readonly_p",     // 可选,绑定的 settings profile
  "networks": ["::/0"]         // 可选,默认 ["::/0"]
}
```

**修改用户 `PUT .../user/:clusterName/:name`**(字段均可选,只改传入项)
```json
{
  "password": "newpass",       // 可选
  "access": "readwrite",       // 可选,readonly | readwrite
  "profile": "another_p"       // 可选
}
```

**创建 profile `POST .../profile/:clusterName`**
```json
{
  "name": "readonly_p",
  "settings": {
    "max_memory_usage": 10000000000,
    "max_threads": 8,
    "readonly": 1
  }
}
```

**列用户响应(示例)**
```json
[
  { "name": "alice", "access": "readonly", "profile": "readonly_p", "networks": ["::/0"] }
]
```

## 6. SQL 映射(核心语义)

所有变更类 SQL 均带 `ON CLUSTER '{cluster}'`。

| 接口动作 | 生成的 SQL |
|---------|-----------|
| 创建 readonly 用户 | `CREATE USER <name> IDENTIFIED BY '<pwd>' HOST IP '<net>' [SETTINGS PROFILE '<p>']`,随后 `GRANT SELECT ON *.* TO <name>` |
| 创建 readwrite 用户 | `CREATE USER ...`,随后 `GRANT CURRENT GRANTS ON *.* TO <name>`,再 `REVOKE ACCESS MANAGEMENT ON *.* FROM <name>` |
| 改用户 profile | `ALTER USER <name> SETTINGS PROFILE '<p>'` |
| 改用户密码 | `ALTER USER <name> IDENTIFIED BY '<pwd>'` |
| 改读写权限 | `REVOKE ALL ON *.* FROM <name>`,随后按新 access 重新 GRANT(readwrite 时再 REVOKE ACCESS MANAGEMENT) |
| 删用户 | `DROP USER IF EXISTS <name>` |
| 列/查用户 | 查 `system.users`(List 传 name 时加 `WHERE name LIKE '%<kw>%'`);权限从 `system.grants` 聚合判定 readonly/readwrite;绑定 profile 来自 `system.settings_profile_elements.inherit_profile` |
| 建 profile | `CREATE SETTINGS PROFILE <name> SETTINGS <k=v...> [CONST/READONLY/WRITABLE]` |
| 改 profile | `ALTER SETTINGS PROFILE <name> SETTINGS ...` |
| 删 profile | `DROP SETTINGS PROFILE IF EXISTS <name>` |
| 列/查 profile | 查 `system.settings_profiles`(List 传 name 时加 `WHERE name LIKE '%<kw>%'`)+ `system.settings_profile_elements` |

**readonly/readwrite 判定(List/Get 时反向推断)**:查 `system.grants`,若某用户仅有 SELECT 类权限则判 readonly;含 INSERT/ALTER/CREATE/DROP 等写权限则判 readwrite;无法归类时如实返回原始权限列表。

## 7. 错误处理与边界

- **保留名保护**:禁止对 `default` 及配置中内置用户执行 Update/Delete,拒绝并返回明确错误,避免锁死集群。
- **幂等**:删除用 `IF EXISTS`;创建若对象已存在,透传 CH 原始错误(不静默吞掉)。
- **ON CLUSTER 部分失败**:透传 CH 返回的分布式 DDL 错误(复用 `WrapMsg` 的 CH 异常解码),不自行重试。
- **参数校验/防注入**:用户名、profile 名只允许 `[a-zA-Z0-9_]`,非法直接拒绝;密码非空。setting 的 key 同样做标识符白名单校验。
- **密码**:走 CH `IDENTIFIED BY`(CH 端 sha256),ckman 不落明文。

## 8. 测试

- **单元测试** `service/clickhouse/rbac_test.go`:主要测试面。给定请求结构 → 断言生成的 SQL 字符串;覆盖 readonly/readwrite、绑 profile、保留名保护、非法名校验。不连真库。
- **不触发持久层 dbtest**:本改动零持久层修改(CH 为真相源),CLAUDE.md 中"持久层改动必须跑真库"规则不适用。
- **手动验证**:连一个真实 CH 集群,curl 走一遍 user 与 profile 的 create/list/get/update/delete,确认 `ON CLUSTER` 在多节点均生效。

## 9. 实现顺序(概要)

1. `model/rbac.go`:请求/响应结构。
2. `service/clickhouse/rbac.go`:SQL 拼接函数(纯函数,便于单测)+ 执行/查询封装。
3. `service/clickhouse/rbac_test.go`:SQL 拼接单元测试。
4. `controller/rbac.go`:handler + 校验。
5. `router/v1.go`:注册 10 条路由。
6. `swag init` 重新生成 swagger(接口注释)。
7. 真实集群手动验证。
