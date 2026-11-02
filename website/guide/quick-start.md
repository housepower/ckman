# 快速开始

本节带你在 5 分钟内把 CKMAN 跑起来，并导入第一个 ClickHouse 集群。

## 前置准备

- 一台 Linux 服务器（CentOS 7+ / Ubuntu 18+ / RHEL 8+）
- 已有一套可访问的 ClickHouse 集群（IP 可达、知道 default 用户密码）
- 可选：ZooKeeper 集群（如果你要用分布式表）

## 安装 CKMAN

::: tip 安装方式
CKMAN 提供 `tar.gz`、`rpm`、`deb` 三种发行包，根据你的系统选一种。
:::

### 使用 tar.gz

```bash
wget https://github.com/housepower/ckman/releases/download/v3.x.x/ckman-3.x.x.Linux.x86_64.tar.gz
tar -xzvf ckman-3.x.x.Linux.x86_64.tar.gz
cd ckman-3.x.x.Linux.x86_64
./bin/start
```

### 使用 RPM

```bash
sudo rpm -ivh ckman-3.x.x.x86_64.rpm
sudo systemctl start ckman
sudo systemctl enable ckman
```

### 使用 DEB

```bash
sudo dpkg -i ckman_3.x.x_amd64.deb
sudo systemctl start ckman
```

## 访问 Web 界面

打开浏览器访问 `http://<ckman-host>:8808`，使用首次启动时生成的账号登录：

| 字段 | 默认值 |
| --- | --- |
| 用户名 | `ckman` |
| 密码 | CKMAN 首次启动创建账号时输出的随机口令 |

::: warning 首次登录必须修改密码
首次登录后，页面会自动跳转到修改密码页。请先配置 `persistent_policy`，再首次启动 CKMAN。RPM/DEB 的随机口令在首次启动时写入 systemd 日志，可用 `sudo journalctl -u ckman -b --no-pager | grep 'CKMAN initial login'` 查找；tar.gz 会在启动终端打印。目标持久层已有 `ckman` 账号时沿用其原密码。
:::

![首次登录后的修改密码页面](/img/deploy/first-login-password-change.png)

安装及查看初始口令的方法见[安装文档](/deploy/install)。

## 导入第一个集群

如果你已经有一套运行中的 ClickHouse 集群，使用**导入集群**最快：

1. 顶部导航点击「集群列表」→「导入集群」
2. 填写集群信息：
   - **集群名称**：在 CKMAN 中唯一标识
   - **CK 用户名 / 密码**：default 用户或自定义用户
   - **TCP 端口**：默认 9000
   - **节点 IP**：每个 shard 的副本列表
   - **ZooKeeper 节点**：如使用分布式表
3. 点击「确定」，CKMAN 会试连每个节点，全部连通即导入成功

## 下一步

集群导入后，你可以：

- 在「监控管理」查看集群整体健康度
- 在「表管理」创建分布式表
- 在「Query 管理」执行在线 SQL
- 在「用户权限管理」给团队成员分配只读 / 普通 / 管理员账号

更多细节参考[功能介绍](/features/cluster/deploy)章节。
