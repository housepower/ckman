# 架构设计

CKMAN 由前端 Web 控制台、后端 API 服务和被管控的 ClickHouse 集群三部分组成。

## 整体架构

![CKMAN 架构图](/img/guide/architecture.png)

主要组件：

- **前端控制台**：Vue.js 单页应用，源码位于 `frontend/`（git submodule），构建后通过 `//go:embed` 嵌入到 Go 二进制中，无需独立 web 服务器
- **后端 API 服务**：Go + Gin，提供 `/api/v1` 业务接口与 `/api/login`、`/api/logout` 鉴权接口，封装 ClickHouse SSH、SQL、ZooKeeper 等操作
- **持久层**：抽象的 `repository` 接口，支持 `local`、`mysql`、`postgres`、`dm8` 四种后端，存放集群配置、任务记录、查询历史等
- **任务系统**：异步任务队列，由 `service/runner` 消费，部署/升级/销毁等耗时操作均走任务化处理
- **被管控对象**：ClickHouse 集群（含 ZooKeeper），CKMAN 通过 SSH 登录节点完成安装/配置/启停，通过 ClickHouse TCP 端口执行 SQL

## 分层结构

```
server/         HTTP 服务器、中间件、路由装配
  └─ enforce/   基于角色的访问控制
controller/     HTTP 请求处理（薄壁）
service/        业务逻辑（clickhouse / zookeeper / cron / runner / prometheus / backup）
repository/     持久化抽象（local / mysql / postgres / dm8）
model/          数据模型与请求/响应结构
config/         配置加载（HJSON / YAML）
common/         共享工具（日志、加密、连接池）
router/         API 路由定义
ckconfig/       ClickHouse 配置文件生成器（metrika.xml / users.xml / custom.xml / keeper.xml）
```

## 认证与权限

详见[核心概念 > 角色权限](/guide/concepts#角色权限)。

1. 登录接口签发 **JWT Token**，含用户名、客户端 IP、过期时间
2. 中间件校验 Token 合法性、是否过期、IP 是否匹配
3. 统一门户**用户 Token**（RSA 加密）优先级高于 JWT
4. Token 缓存使用 in-memory cache，会话超时自动清除

## 多实例与高可用

CKMAN 支持多实例部署，配合 Nacos 服务发现：

- 每个实例向 Nacos 注册并发送心跳
- 只有 master 节点执行定时任务（cron job），避免重复触发
- `config.IsMasterNode()` 判断当前是否 master
- 多实例需共用 mysql / postgres / dm8 持久层（不能用 `local`）

详见[高可用部署](/deploy/high-availability)。

## 任务系统

```
用户请求 → controller → 创建 Task → 立即返回 taskId
                            │
                            ↓
                  repository 写入 task 记录
                            │
                            ↓
             runner 后台轮询 → 执行任务 → 更新状态
```

任务支持：暂停、终止、删除（仅完成或停止状态可删）。
