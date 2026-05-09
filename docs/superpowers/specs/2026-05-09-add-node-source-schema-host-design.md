# 增加节点：指定 Schema 同步源节点

- 日期：2026-05-09
- 仓库：`housepower/ckman`（后端）+ `housepower/ckman-fe`（前端）

## 背景

新增 ClickHouse 节点时，ckman 需要从已有节点拉取建表语句到新节点上执行。当前实现写死了 schema 同步源：

| 场景 | 代码位置 | 当前策略 | 问题 |
|---|---|---|---|
| AddNode | `service/runner/handle.go:237,244` | 写死 `conf.Hosts[0]` | 用户无法指定；首节点离线即整个任务失败 |
| StartNode | `controller/clickhouse.go:1944-1961` | 取第一个 `ip != self` 的 host | 无可用性检查；选中的节点离线时失败 |
| DeployCk + LogicCluster | `service/clickhouse/clickhouse.go:1290` `SyncLogicTable` | src 端用 `GetShardAvaliableHosts`；dst 端写死 `dst.Hosts[0]` | dst 端无可用性检查 |

## 目标

1. **AddNode**：前端给用户一个下拉框来选 schema 同步源节点；后端按用户指定的节点执行同步。
2. **StartNode / DeployCk(SyncLogicTable)**：自动找第一个**可用**节点（不再盲目取 `Hosts[0]`），允许集群首节点离线。

## 非目标

- 不持久化 SourceSchemaHost 到 `CKManClickHouseConfig`（仅本次 AddNode 操作生效）。
- 不修改 cron 中的列级 schema 同步（`service/cron/clickhouse.go` 的 `syncSchema`），那是不同概念。
- 不改 StartNode 的 API 入参形式（沿用 query string）。

## 设计

### 1. 数据模型

`model/manage_ck.go` 的 `AddNodeReq` 新增字段：

```go
type AddNodeReq struct {
    Ips              []string `json:"ips" example:"192.168.0.1,192.168.0.2"`
    Shard            int      `json:"shard" example:"3"`
    SourceSchemaHost string   `json:"sourceSchemaHost" example:"192.168.0.10"`
}
```

字段约定：
- 必填。
- 必须是当前 `conf.Hosts` 中已存在的 IP（不能等于本次新增的任意一个 IP）。
- 后端在创建任务前用 `common.ConnectClickHouse` 试连一次，连不上直接返回错误让用户重选。

### 2. 后端逻辑

#### 2.1 工具函数（新增）

在 `common/ck.go` 新增：

```go
// pickAvailableSchemaSource 在 conf.Hosts 中按顺序挑选第一个能连上 ClickHouse 的节点，
// 跳过 exclude 中列出的 IP。若全部不可用返回 ""。
func PickAvailableSchemaSource(conf *model.CKManClickHouseConfig, exclude ...string) string
```

实现要点：
- 遍历 `conf.Hosts`，跳过 `exclude` 集合。
- 用 `ConnectClickHouse(ip, ClickHouseDefaultDB, conf.GetConnOption())` 试连，第一个成功返回。
- 全失败返回 `""`，由调用方决定是否跳过同步。

#### 2.2 AddNode 流程

**Controller**（`controller/clickhouse.go:1715` 的 `AddNode`）：
- 解码后校验 `req.SourceSchemaHost`：
  - 非空。
  - 在 `conf.Hosts` 中且不等于 `req.Ips` 中任一项。
  - `ConnectClickHouse` 试连成功。
- 把 `req.SourceSchemaHost` 写入 `d.Ext.SourceSchemaHost`（在 `deploy.CKDeployExt` 上加该字段），避免污染会持久化的 `CKManClickHouseConfig`。

**Handler**（`service/runner/handle.go:202` 的 `CKAddNodeHandle`）：
- 把 `service.FetchSchemerFromOtherNode(conf.Hosts[0])` 改为 `service.FetchSchemerFromOtherNode(d.Ext.SourceSchemaHost)`。
- 同一行 244 的重试同样用 `d.Ext.SourceSchemaHost`。
- 不做 fallback：用户既然选了，连不上就报错。

#### 2.3 StartNode 流程

`controller/clickhouse.go:1925` 的 `StartNode`：
- 删除现有循环（行 1944-1949）找 host 的逻辑，替换为：
  ```go
  host := common.PickAvailableSchemaSource(&conf, ip)
  ```
- 后续 `if host != ""` 判空逻辑保持不变。

#### 2.4 DeployCk + LogicCluster

`service/clickhouse/clickhouse.go:1290` 的 `SyncLogicTable`：
- src 端不动（已有 `GetShardAvaliableHosts`）。
- dst 端 `dst.Hosts[0]` 改为 `common.PickAvailableSchemaSource(&dst)`，为空时打 warn 并返回 `false`。

### 3. 前端（`ckman-fe`）

#### 3.1 父组件

`src/views/manage/manage.vue:62-67` 的 `<AddNodeDialog>`：
- 新增 prop：`:nodes="list.nodes"`。

#### 3.2 AddNodeDialog

`src/views/manage/modal/addNode.vue`：
- props 增加 `nodes: { type: Array, default: () => [] }`。
- `formModel` 增加 `sourceSchemaHost: ''`。
- 表单新增一项（放 IP / Shard 之后）：
  ```vue
  <el-form-item :label="$t('manage.Source Schema Host') + ':'" prop="sourceSchemaHost">
    <el-select v-model="formModel.sourceSchemaHost" class="width-350">
      <el-option
        v-for="n in nodes"
        :key="n.ip"
        :label="`${n.ip} (${n.status})`"
        :value="n.ip"
        :disabled="n.status === 'red'" />
    </el-select>
  </el-form-item>
  ```
- `mounted`（或 `watch nodes`）设默认值：第一个 `status === 'green'` 的节点；没有则第一个 `'yellow'`；都没则空。
- `onOk` 提交时附带 `sourceSchemaHost`。

#### 3.3 i18n

`src/services/i18n.ts` 的 `manage` 段下新增：
- `Source Schema Host`（中文：「同步 Schema 节点」；英文：`Source Schema Host`）

## 数据流

```
[Frontend AddNodeDialog]
  formModel.sourceSchemaHost
        |
        v
POST /api/v1/ck/node/{cluster}  body: {ips, shard, sourceSchemaHost}
        |
        v
[controller.AddNode]
  - validate sourceSchemaHost ∈ conf.Hosts ∧ ∉ req.Ips
  - ConnectClickHouse 试连
  - d.Ext.SourceSchemaHost = req.SourceSchemaHost
        |
        v
[runner.CKAddNodeHandle]
  - service.FetchSchemerFromOtherNode(d.Ext.SourceSchemaHost)
```

## 错误处理

| 情形 | 行为 |
|---|---|
| `SourceSchemaHost` 为空 | 400 `E_INVALID_PARAMS` |
| `SourceSchemaHost` 不在 `conf.Hosts` 或在 `req.Ips` 里 | 400 `E_INVALID_VARIABLE` |
| `SourceSchemaHost` 连不上 ClickHouse | 400 `E_DATA_CHECK_FAILED`，提示用户换一个节点 |
| StartNode 时所有其他节点都不可用 | `host == ""`，跳过 schema 同步（保留现有行为，不阻塞节点上线） |
| DeployCk(SyncLogicTable) dst 端无可用节点 | 打 warn 返回 false，不阻塞主流程（保留现有行为） |

## 测试要点

- 单元测试 `PickAvailableSchemaSource`：
  - 全部可用 → 返回首个；
  - 部分不可用 → 跳过；
  - 全不可用 → 返回 `""`；
  - 含 `exclude` → 排除指定 IP。
- AddNode 校验失败的几种 case 走 controller 层。
- 手动验证：StartNode 时把 `Hosts[0]` 停掉，仍能成功同步 schema。

## 影响范围

- 后端：`model/manage_ck.go`、`controller/clickhouse.go`、`service/runner/handle.go`、`service/clickhouse/clickhouse.go`、`common/ck.go`、`deploy/deploy.go`（`d.Ext` 字段）。
- 前端：`ckman-fe` 仓 `src/views/manage/manage.vue`、`src/views/manage/modal/addNode.vue`、`src/services/i18n.ts`。
- API：`/api/v1/ck/node/{clusterName}` POST，body 新增字段；swagger 注解需重新 `swag init`。
- 兼容性：旧客户端不传 `sourceSchemaHost` 会被 400 拒绝。属于破坏性变更，但仅影响 ckman-fe，没有外部 API 用户。
