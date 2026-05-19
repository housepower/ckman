# 什么是 CKMAN

CKMAN（**C**lic**K**house **MAN**ager）是一款面向 ClickHouse 的集群管理与监控平台。它通过 Web 界面提供集群部署、升级、监控、表管理、数据备份恢复等能力，免去运维同学手工 SSH 各节点编辑配置文件的繁琐过程。

## 为什么要有 CKMAN

ClickHouse 在生产环境通常以多节点集群方式部署，节点之间通过 ZooKeeper 协调副本同步与分布式 DDL。手工运维一套 ClickHouse 集群涉及：

- 在每台节点上安装相同版本的 RPM 包
- 编辑 `config.xml`、`users.xml`、`metrika.xml` 并保持各节点一致
- 在 ZooKeeper 上规划副本路径
- 滚动升级时控制重启顺序
- 监控集群整体健康状况、查询性能

把这些事情都交给 CKMAN，运维同学只需要在 Web 界面上点几下。

## CKMAN 能做什么

### 集群生命周期

- **部署集群**：选择安装包版本，填入节点 IP、副本数、分片数，CKMAN 自动完成全部安装和配置
- **导入集群**：将已经在运行的 ClickHouse 集群纳入管理
- **升级集群**：滚动升级整个集群到指定版本
- **销毁集群**：清理节点上的 ClickHouse 及配置
- **节点管理**：在线增加/删除节点、启停节点、查看节点日志

### 监控与可观测

- ClickHouse 数据库级、节点级、ZooKeeper 三层 KPI
- 慢查询、当前会话、表合并、后台线程池、分布式 DDL 队列
- 监控数据**直读 ClickHouse 系统表**，开箱即用，不依赖 Prometheus
- 可选：暴露 `/metrics` 端点接入 Prometheus / Grafana 做长期存储与告警

### 数据与表管理

- 创建/修改/删除表与分布式表，支持复杂的 MergeTree 配置
- 物化视图、TTL、ORDER BY 在线调整
- 分区操作、归档、purge
- 在线执行 SQL 查询与导出
- 定时备份策略、增量去重、本地或 S3 卷

### 高可用与权限

- 多实例部署 + Nacos 服务发现，避免单点
- 三级角色权限：**管理员 / 普通用户 / 游客**（详见[角色权限](/features/users/overview)）
- JWT token + 统一门户 token，token 与客户端 IP 绑定

## 适用场景

CKMAN 适合以下场景：

- 内网或私有云环境，需要一个统一的 ClickHouse 集群管控入口
- 多个 ClickHouse 集群并存，希望集中管理
- 团队中既有研发也有 DBA，需要细粒度权限
- 希望通过 API 把 ClickHouse 集群管理操作集成到自有平台

## 接下来

- [快速开始](/guide/quick-start)：5 分钟把 CKMAN 跑起来
- [架构设计](/guide/architecture)：了解 CKMAN 的内部结构
- [核心概念](/guide/concepts)：cluster / shard / replica / 逻辑集群等术语
