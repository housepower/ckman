# 升级 CKMAN

升级方式与安装方式对应。

::: warning 操作前先做
- 备份 `conf/ckman.hjson`
- 如果使用 `local` 持久化策略，备份 `conf/clusters.db`（旧版本可能还是 `clusters.json` / `clusters.yaml`，账号也存在这里）
- 升级期间 Web 界面短暂不可用，运维任务请提前完成
:::

## RPM 升级

```bash
# 1. 下载新版本 rpm
wget https://github.com/housepower/ckman/releases/download/vx.x.x/ckman-x.x.x-x86_64.rpm

# 2. 停止服务
sudo systemctl stop ckman

# 3. 升级
sudo rpm -Uvh ckman-x.x.x-x86_64.rpm

# 4. 重启
sudo systemctl daemon-reload
sudo systemctl start ckman
```

::: tip 配置文件保留
rpm 升级会保留旧的 `ckman.hjson`。新版本默认配置写到 `ckman.hjson.rpmnew`，你可以对比差异后手动 merge。
:::

## tar.gz 升级

```bash
WORKDIR=/path/to/ckman

# 1. 停止服务
cd $WORKDIR && bin/stop

# 2. 备份配置
cp conf/ckman.hjson conf/ckman.hjson.last

# 3. 解压覆盖
tar -xzvf ckman-x.x.x-YYDDMM.Linux.x86_64.tar.gz -C $WORKDIR

# 4. 还原配置
cp conf/ckman.hjson.last conf/ckman.hjson

# 5. 启动
bin/start
```

::: tip 跨目录升级
如果新版本安装到与旧版本**不同**的工作目录，记得把 `conf/clusters.db` 也复制过去（旧版本可能是 `clusters.json` / `clusters.yaml`，新版本启动时会自动迁移到 `clusters.db`），否则集群信息无法加载。
:::

## Docker 升级

```bash
docker pull quay.io/housepower/ckman:latest
docker rm -f ckman
docker run -itd -p 8808:8808 --restart unless-stopped \
  --name ckman quay.io/housepower/ckman:latest
```

如果挂载了配置卷，新容器会自动接管旧配置。

## 数据库 schema 自动迁移

CKMAN 启动时会自动检查 `mysql` / `postgres` / `dm8` 后端的 schema 版本。**不需要手工执行 SQL 脚本**。

如启动失败提示 schema 错误，请查看 `ckman.log` 中的具体提示。

## 升级集群中的 ClickHouse

升级 ClickHouse 集群（而非 CKMAN 自身）请参见[升级集群](/features/cluster/upgrade)。
