# 常见问题

本页收集 CKMAN 使用过程中的典型问题与排查路径。

## 部署 / 安装

### Q：浏览器访问 8808 端口返回空白页

排查顺序：

1. `systemctl status ckman` / `bin/start` 看进程是否真在跑
2. `ss -lntp | grep 8808` 看是否监听
3. `tail -100 /var/log/ckman/ckman.log` 看启动日志
4. 防火墙是否放开（`firewall-cmd --list-ports`）

### Q：默认密码 `Ckman123456!` 登录失败

可能原因：

- 历史版本残留的 `conf/password` 文件密码不是默认值
- 用 `ckmanpassword` 工具重置：`cd /etc/ckman/conf && ckmanpassword`

### Q：rpm 升级后服务起不来

- 检查 `ckman.hjson.rpmnew` 与现有配置的字段是否兼容
- 看 `/var/log/ckman/ckman.log` 是否有 schema 不匹配错误
- 持久层数据库连接是否正常

## 集群部署

### Q：部署集群时 SSH 失败（5100）

- 确认 SSH 用户的 `sudo` 权限（部署需要 root）
- 公钥认证：`.ssh/id_rsa` 复制到 ckman 工作目录 `conf/`，并保证文件可读
- 密码认证：密码是否正确，是否包含特殊字符未转义

### Q：节点 ClickHouse 启动失败

按以下顺序看：

1. 节点上 `journalctl -u clickhouse-server -n 200`
2. `/var/log/clickhouse-server/clickhouse-server.err.log`
3. 配置文件 `/etc/clickhouse-server/config.xml` 与 `metrika.xml` 是否生成
4. 数据目录权限是否正确（`clickhouse:clickhouse`）
5. 磁盘空间是否充足

## 集群运维

### Q：副本表读写不一致 / 数据落后

- 查看 [Table Replication Status](/features/tables/overview#table-replication-status) 看哪个副本落后
- 落后副本上执行 `SYSTEM RESTART REPLICA <database>.<table>`
- 严重情况下使用 [Restore Replica](/features/tables/overview) 接口

### Q：rebalance 执行失败

- 非副本模式 rebalance 要求所有节点安装了 `rsync` 且配置了 SSH 互信
- 普通用户执行需配置 `/etc/sudoers` 中 NOPASSWD
- 看任务详情中具体错误，常见为权限或 rsync 不存在

### Q：DDL 长时间不返回

按顺序检查：

1. [Distributed DDL Queue](/features/tables/overview#distributed-ddl-queue) 是否积压
2. ZooKeeper 集群是否健康
3. 某个节点是否离线
4. 是否有大事务（Mutation）占着锁

## 持久层

### Q：CKMAN 启动报 "table not exist"

- mysql / dm8 策略：CKMAN 启动时自动建表，看是否权限不足
- postgres 策略：**手工**执行 `dbscript/postgres.sql` 初始化

### Q：多实例数据不同步

- 确认所有实例的 `persistent_policy` 一致且连同一个数据库
- 确认所有实例的 `nacos.enabled = true` 且能连上 Nacos
- 查看 master 选举日志（`Nacos register success` 出现说明注册成功）

## 监控

### Q：Overview 面板没有数据

监控数据默认**直读 ClickHouse 系统表**，不依赖 Prometheus。按顺序排查：

1. 在 Query 管理里执行 `SELECT count() FROM system.metric_log WHERE event_time > now() - 600`
   - 返回 0 → 节点的 `system.metric_log` 没启用或被截断
   - 在 `/etc/clickhouse-server/config.xml` 确认 `<metric_log>` 段没被注释掉（CK 21.x+ 默认开启）
   - 集群刚部署的几分钟内 metric_log 可能还是空的，等几分钟后重试
2. `SELECT count() FROM system.asynchronous_metric_log WHERE event_time > now() - 600` 同上排查
3. ZK 监控仍依赖 ZK 自身 metrics 端口（与上面是两套），见下一题
4. 如果你**走的是 Prometheus 接入**：检查 Prometheus 是否能抓到 ClickHouse `9363` 与 CKMAN `8808` 的 `/metrics`

### Q：ZooKeeper 监控指标全空

ZK 监控基于 `mntr` 8080 端口，需要：

- ZooKeeper ≥ 3.5.0
- `zoo.cfg` 中配置 `admin.enableServer=true` 与 `admin.serverPort=8080`
- 配置 `metricsProvider.className` 与 `metricsProvider.httpPort=7000`

## API 集成

### Q：调用 API 返回 5022 (JWT_TOKEN_NONE)

- 请求未带 `token` header
- 或者使用统一门户但 `userToken` 未传

### Q：返回 5023 (JWT_TOKEN_IP_MISMATCH)

- 反向代理时未透传客户端 IP
- Nginx 中加 `proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;`
- 或者换网络后重新登录

### Q：返回 5030 (USER_VERIFY_FAIL)

- 当前用户角色无权访问该接口
- 检查权限矩阵：[用户与角色权限](/features/users/overview#详细权限矩阵)

## 备份与恢复

### Q：S3 备份失败

- 看任务详情中的具体错误码（多为 S3 错误透传）
- 确认 AccessKey / SecretKey 是否有 bucket 的 PutObject 权限
- Endpoint 是否带 http/https 协议头
- bucket 是否存在（或允许自动创建）

### Q：恢复时报"表已存在"

- 恢复目标表已存在，先手动 DROP TABLE 再恢复，或选择恢复到不同的表名

## 与 ClickHouse 配合

### Q：表 readonly 状态如何恢复

调用接口 `PUT /api/v1/ck/table/readonly/:cluster`，或在 Web 界面"修复只读"。

底层执行：

```sql
SYSTEM RESTORE REPLICA <database>.<table>
```

### Q：分区已删除但磁盘空间未释放

- ClickHouse 的 DROP PARTITION 是逻辑删除，物理删除发生在合并 / 后台清理
- 等待几分钟或执行 `OPTIMIZE TABLE ... FINAL`（注意性能开销）

## 仍然无法解决？

- 完整日志：`/var/log/ckman/ckman.log` + 节点 `clickhouse-server.err.log`
- 提交 [Issue](https://github.com/housepower/ckman/issues) 并附上：
  - CKMAN 版本（`/api/v1/version`）
  - ClickHouse 版本
  - 复现步骤
  - 完整错误日志（脱敏后）
