# 持久化后端真库集成测试

这套测试(build tag `dbintegration`,平时不参与构建)对 `mysql` / `dm8` / `postgres`
三个后端**连真库**跑一遍:连启 3 次(抓 AutoMigrate 非幂等 / 达梦大小写重启崩)、
零值时间 backup_run(抓达梦 -6118 / MySQL DATETIME 越界)、`TlsConf` 私钥加解密往返
(抓 PEM 换行符被 AES 去填充逻辑截断)、以及各实体 CRUD 往返。

它调用的是 `repository.Ps` 的真方法(和 ckman 生产同一路径),**不是裸写 SQL**——
这些 bug 恰恰藏在方法内部(哨兵时间包装、`ensureSchema`、JSON blob 等),mock 单测
一律抓不到。给哪个库的环境变量就测哪个,没给的自动 Skip。

## 一条命令:本地真库回归

不需要预备任何库,自动拉容器、跑测试、跑完删容器:

```bash
make dbtest          # mysql + postgres
make dbtest-mysql    # 只跑 mysql
make dbtest-pg       # 只跑 postgres
```

`make help` 能列出全部目标。背后是 `./repository/dbtest/local-test.sh [mysql|postgres]`,
直接调也行(`pg-local-test.sh` 保留为兼容入口,转发到 `local-test.sh postgres`)。

镜像可换:`DBTEST_MYSQL_IMAGE=... DBTEST_PG_IMAGE=... make dbtest`

达梦没有公开镜像,起不了容器,得自己指一台实例:

```bash
export DBTEST_DM_HOST=... DBTEST_DM_USER=SYSDBA DBTEST_DM_PASS=...
make dbtest-dm
```

## 手动指定各后端

```bash
# MySQL
export DBTEST_MYSQL_HOST=... DBTEST_MYSQL_PORT=3306 \
       DBTEST_MYSQL_USER=root DBTEST_MYSQL_PASS=... DBTEST_MYSQL_DB=ckman_db

# 达梦 DM8(Schema 用不到,可不填)
export DBTEST_DM_HOST=... DBTEST_DM_PORT=5236 DBTEST_DM_USER=sysdba DBTEST_DM_PASS=...

# PostgreSQL / openGauss(openGauss 用 user=gaussdb db=postgres,sslmode 已在 DSN 里 disable)
export DBTEST_PG_HOST=... DBTEST_PG_PORT=5432 \
       DBTEST_PG_USER=postgres DBTEST_PG_PASS=... DBTEST_PG_DB=ckman_db

make dbtest-env   # 等价于 go test -tags dbintegration -v ./repository/dbtest/ -run TestBackends
```

- 指向**空的测试库**,勿指生产:测试建表 + 写 `dbtest_` 前缀数据并自动清理,但连启会重跑迁移。
- 单跑某个后端:`-run TestBackends/mysql`(或 `/dm8` `/postgres`)。

除 `TestBackends` 外,MySQL 另有两条迁移专项(同样用 `DBTEST_MYSQL_*`,`-run TestMysqlMigrate`):

- `TestMysqlMigrateIsIdempotent` —— 表就绪后再连启必须**零 DDL**。用 `SHOW GLOBAL STATUS` 的
  `Com_alter_table` / `Com_create_table` / `Com_create_index` 计数器判定。
  gorm 的 AutoMigrate 曾在这里每次重启重发 6 条 `ALTER TABLE ... MODIFY COLUMN`,
  MySQL 5.7 磁盘一满就 `Error 1005 (errno: 28)`、Init 直接 FATAL(首次建库能过、重启必崩)。
- `TestMysqlMigrateUpgradesOldSchema` —— 老库缺列/缺表时该补的还得补,防止「为了幂等而什么都不做」。

  注意这条会 `DROP TABLE tbl_cluster / tbl_user` 重造老结构,更加不能指向生产库。

## 快速起各类库(参考)

```bash
# 真 PostgreSQL
docker run -d --name pgtest -e POSTGRES_PASSWORD=Test@1234 -e POSTGRES_DB=ckman_db -p 5433:5432 postgres:16

# openGauss(国产 PG 兼容,默认用户 gaussdb)
docker run -d --name opengauss --privileged=true -p 25432:5432 -e GS_PASSWORD=Euler@1234 enmotech/opengauss:latest
```
