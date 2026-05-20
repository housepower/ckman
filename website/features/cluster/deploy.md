# 部署集群

通过 CKMAN 部署一套全新的 ClickHouse 集群。

::: tip 适用场景
- 全新搭建的集群
- 想让 CKMAN 完整托管的集群
- 需要后续做升级、销毁、节点增删等运维变更
:::

如果集群已经在运行了，请使用[导入集群](/features/cluster/import)。

## 节点准备清单

在部署集群之前，建议每台目标节点完成下面的准备：

- 所有 HDD 做一个大的 RAID 5 阵列
- 用 `hostnamectl` 设置 hostname
- 用 `timedatectl set-timezone` 设置 timezone
- 启动 `ntpd` 或 `chrony` 网络时间同步
- 永久关闭 swap
- 永久关闭防火墙 `firewalld`
- 安装 `tmux`、`mosh`、`emacs-nox` 等常用软件
- 创建一个普通账户并加入 `wheel` 组，允许其 `sudo` 切换到 root

## 操作步骤

主页点击 **Create a ClickHouse Cluster** 进入部署表单：

![部署集群表单](/img/features/cluster/deploy-form.png)

## 表单字段

### 基础

- **Cluster Name**：集群名，CKMAN 内唯一
- **Package Type**：安装包类型（区分 x86/arm 和 rpm/tgz），上传安装包后自动从下拉选取
- **ClickHouse Version**：从已上传的安装包列表中选择
- **Logic Name**：逻辑集群名称（可选，详见[核心概念](/guide/concepts#逻辑集群-logic-cluster)）
- **TCP Port**：ClickHouse TCP 端口，默认 `9000`

::: warning 安装包先上传
部署前需要先上传 ClickHouse 安装包到 CKMAN（顶部"安装包管理"页面）。每个 ckman 服务**只能看到自己上传的安装包**，多中心不互通。
:::

### 节点

- **ClickHouse Node List**：节点 IP 列表，支持 CIDR 与 Range 简写（例如 `192.168.0.[1-4]`）
- **Replica**：是否启用副本（默认关闭）
  - 启用：默认 1 个 shard 2 个副本；节点数为奇数时最后一个 shard 只有 1 个副本；最多 2 副本（更多副本通过"增加节点"补）
  - 不启用：每个节点独立一个 shard
- **是否启用副本一旦确定，后续不可更改**

### ZooKeeper

- **ZooKeeper Node List**：ZK 节点 IP 列表（CKMAN 不提供 ZK 部署能力，需要预先搭建好）
- **ZooKeeper Port**：默认 `2181`
- **ZK Status Port**：监控端口，默认 `8080`（需要 ZK ≥ 3.5.0）

### 数据路径

- **Data path**：ClickHouse 数据存放路径，例如 `/data01/`

### 集群用户

- **Cluster Username**：ClickHouse 用户名，**不能填 `default`**（保留用户）
- **Cluster Password**：ClickHouse 密码

### SSH

CKMAN 通过 SSH 登录每个节点安装 ClickHouse，需要以下字段：

- **SSH Username**：登录用户，要有 root 或 sudo 权限
- **SSH Port**：默认 `22`
- **AuthenticateType**：认证方式
  - `0` 密码认证（保存密码）
  - `1` 密码认证（不保存密码）—— 后续运维操作每次都要重输
  - `2` 公钥认证 —— **推荐**，需配置 `.ssh/id_rsa` 到 ckman 的 `conf` 目录并保证可读
- **SSH Password**：方式 0/1 时填写

### 存储策略（Storage）

- **disks**：磁盘列表
  - `local`、`hdfs`、`s3` 三种类型
  - ClickHouse 内置 `default` 策略
  - `hdfs` 需要 ClickHouse ≥ 21.9
- **policies**：策略，引用的磁盘必须存在于 `disks` 中

### 用户配置（User Config）

- **Users**：ClickHouse 用户名、密码、使用的 profile / quota 策略
- **Profiles**：资源使用、是否只读等策略
- **Quotas**：一段时间内查询、插入等的资源配额
- **User Custom Config**：自定义配置，最终写到 `users.xml`

### 自定义配置（Custom Config）

写入 `config.d/custom.xml`，ClickHouse 启动时与默认 `config.xml` merge。

`key` 使用类 XPath 语法（参考 [W3Schools XPath](https://www.w3schools.com/xml/xpath_syntax.asp)）：

- 不同 XML 层级以 `/` 分隔
- attribute 用 `[]` 包裹，每个 attr 以 `@` 开头

举例：

| key | value | 生成的 XML |
| --- | --- | --- |
| `title[@lang='en', @size=4]/header` | `header123` | `<title lang="en" size="4"><header>header123</header></title>` |

## 强制覆盖

若目标节点上已经存在 ClickHouse（可能属于其他集群或不受 CKMAN 纳管），正常情况下不允许部署。

勾选**强制覆盖**：销毁该节点已有 ClickHouse 后重新部署。

## 部署完成后

成功部署的集群：

- `mode` 为 `deploy`
- 可以执行修改、rebalance、启停、升级、节点增删等全部运维操作

接下来可以：

- [增删节点](/features/cluster/nodes)
- [升级集群](/features/cluster/upgrade)
- 在表管理中创建第一张分布式表
