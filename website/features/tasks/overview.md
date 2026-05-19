# 任务管理

CKMAN 的耗时操作（部署、升级、销毁、节点增删、数据均衡、备份、归档等）以**异步任务**方式执行。

任务管理页面统一查看与管理这些任务。

![任务列表](/img/features/tasks/list.png)
<!-- TODO(screenshot): 当前为占位图，请用实际截图替换 /img/features/tasks/list.png（任务列表） -->

## 任务状态

| 状态 | 含义 |
| --- | --- |
| `pending` | 等待中（未开始） |
| `running` | 执行中 |
| `success` | 已成功 |
| `failed` | 已失败 |
| `stopped` | 已停止 |

## 任务列表

显示字段：

- 任务 ID
- 任务类型（部署 / 升级 / 销毁 / rebalance / 备份 等）
- 关联集群
- 状态
- 开始时间 / 结束时间 / 耗时
- 当前 step（执行到哪一步）

## 任务详情

点击某条任务进入详情，可看到：

- 各节点的执行进度（任务通常按节点并行/串行展开）
- 每一步的执行结果
- 失败时的错误日志与堆栈
- 重试次数

![任务详情](/img/features/tasks/detail.png)
<!-- TODO(screenshot): 当前为占位图，请用实际截图替换 /img/features/tasks/detail.png（任务详情页） -->

## 停止任务

- 仅 `pending` / `running` 状态可停止
- 停止信号会传递到 runner，正在执行的步骤完成后停下来
- 已经完成的步骤**不会**被回滚

::: warning 停止 ≠ 回滚
比如升级集群时停止任务，已经升级的节点保持新版本，未升级的保持旧版本。集群处于**混合版本**状态，需要再次触发升级让所有节点对齐。
:::

## 删除任务

- 仅 `success` / `failed` / `stopped` 状态可删除
- `pending` / `running` 状态需要先停止再删除
- 删除任务**不会**回退任务的副作用

## 运行中任务计数

界面右上角显示当前 `running` 任务数。如果任务长期堆积，请检查 runner 日志或 master 实例状态。

## 任务相关接口

| 接口 | 功能 |
| --- | --- |
| `GET /api/v1/task/:taskId` | 任务详情 |
| `GET /api/v1/task/lists` | 任务列表 |
| `GET /api/v1/task/running` | 运行中任务计数 |
| `PUT /api/v1/task/:taskId` | 停止任务 |
| `DELETE /api/v1/task/:taskId` | 删除任务记录 |

::: tip 权限
任务停止与删除目前仅 **admin** 可操作。普通用户和游客只能查看。
:::
