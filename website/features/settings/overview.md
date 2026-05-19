# 配置管理

通过 Web 界面修改 ClickHouse 集群的运行时配置。

::: warning 仅限 deploy 模式
配置管理只能修改通过 CKMAN 部署的集群（`mode = deploy`）。`import` 模式的集群不能在 CKMAN 中改配置。
:::

## 可修改的配置项

- 存储策略（disks / policies）
- 用户配置（users / profiles / quotas）
- 端口配置
- 自定义 XML 配置（写入 `custom.xml`）
- 集群拓扑（仅启停，不含增删节点——见[节点管理](/features/cluster/nodes)）

## 操作步骤

集群详情 → **集群配置** → 修改 → **保存**：

![集群配置](/img/features/settings/cluster-config.png)
<!-- TODO(screenshot): 当前为占位图，请用实际截图替换 /img/features/settings/cluster-config.png（集群配置编辑页） -->

## 重启判断

CKMAN 会根据修改内容**自动判断**是否需要重启：

| 修改内容 | 是否重启 |
| --- | --- |
| 添加新的 disk / policy | 是 |
| 修改已有 policy 的 volume 顺序 | 是 |
| 修改 user 密码、profile 引用 | 否（reload 即可） |
| 修改端口 | 是 |
| 修改 custom.xml 内容 | 视具体 setting 而定 |

需要重启时，"保存"按钮会变为 **"保存 & 重启"**。

## 存储策略修改的限制

::: danger 已有数据的存储介质不可删除
如果某个 disk 上已经有 ClickHouse 数据，**不允许从配置中删除**。

正确流程：

1. 先把数据搬走（rebalance 到其他 shard / 重建表到新存储 policy）
2. 确认该 disk 上无数据
3. 再删除 disk / policy
:::

## 自定义配置语法

`key` 使用类 XPath：

| 元素 | 写法 |
| --- | --- |
| 层级 | `/` 分隔 |
| 属性 | `[@attr='value']` |
| 多属性 | `[@a='1', @b='2']` |

示例：

| key | value | 生成的 XML |
| --- | --- | --- |
| `title[@lang='en', @size=4]/header` | `header123` | `<title lang="en" size="4"><header>header123</header></title>` |

## 用户配置（User Config）

- **Users**：每个用户的名字、密码、profile、quota 引用
- **Profiles**：资源限制策略（max_memory_usage、readonly 等）
- **Quotas**：时间窗口内的配额（query 次数、写入字节等）
- **User Custom Config**：自定义 XML，最终写到 `users.xml`

::: tip 与 Cluster User 的区别
- **Cluster Username**（部署时填的）：CKMAN 用来连接 ClickHouse 的账号
- **User Config**：ClickHouse 集群内所有可用账号的定义（业务侧 SQL 客户端使用）
:::
