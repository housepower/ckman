# 销毁集群

::: danger 这是一个不可逆操作
"销毁集群"不是从 CKMAN 移除，而是**彻底销毁**：
- 停止所有节点的 ClickHouse 服务
- 卸载 ClickHouse 软件包（rpm/deb/tgz）
- 清空数据目录
- 集群在物理上不存在了

如果你只想让 CKMAN 不再纳管，而保留 ClickHouse 本身，请用[删除集群](#删除集群-vs-销毁集群)。
:::

仅 `mode = deploy` 的集群支持销毁。`import` 模式的集群无法销毁（CKMAN 不掌握其 SSH）。

## 操作步骤

集群详情 → 点击 **Destroy Cluster** → 输入集群名确认 → 等待任务完成。

## 删除集群 vs 销毁集群

| 操作 | 影响 |
| --- | --- |
| **删除集群**（Delete） | 仅从 CKMAN 的纳管列表移除，ClickHouse 服务与数据**保留** |
| **销毁集群**（Destroy） | 卸载 ClickHouse、清理数据、节点恢复"裸机"状态 |

import 模式仅支持"删除"，deploy 模式两种都支持。

## 销毁前请确认

1. 已经备份所有重要数据（可使用[数据备份](/features/backup/overview)）
2. 业务侧已经停止读写
3. 团队成员都知情，避免误操作

## 销毁后

CKMAN 中该集群消失。节点上：

- ClickHouse 软件被卸载
- 数据目录（部署时配置的 `Data path`）被清空
- ZooKeeper 中相关 znode 不会自动清理，可通过 `clear_znodes` 定时任务或手工清理

如果想在同样的节点上重新部署集群，**强制覆盖**选项可以加速准备过程。
