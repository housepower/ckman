# 增加节点指定 Schema 同步源节点 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** AddNode 接口允许用户指定 schema 同步源节点；StartNode 与 SyncLogicTable 自动选可用节点，避免首节点离线导致失败。

**Architecture:** 后端在 `common` 包新增 `PickAvailableSchemaSource(conf, exclude...)` 工具；`AddNodeReq` 增加 `SourceSchemaHost` 字段并在 controller 校验后透传到 `CkDeployExt`，runner handler 替换写死的 `Hosts[0]`。StartNode 与 SyncLogicTable dst 端切换到工具函数。前端 `ckman-fe` 在 AddNode 对话框新增下拉框，默认选第一个 green 节点。

**Tech Stack:** Go 1.24 / Gin / clickhouse-go；Vue.js 2 + Element-UI（ckman-fe）；Swagger（swag init）。

**仓库：**
- 后端：`/data/root/go/src/github.com/housepower/ckman`
- 前端：`/data/root/go/src/github.com/housepower/ckman-fe`（独立 git 仓）

---

## 文件结构

### 新建
- 无新文件。

### 修改（后端 ckman）
- `common/ck.go` — 新增 `PickAvailableSchemaSource`
- `common/ck_test.go` — 新增对应单测
- `model/manage_ck.go:29-32` — `AddNodeReq` 加 `SourceSchemaHost` 字段
- `model/deploy_ck.go:45-53` — `CkDeployExt` 加 `SourceSchemaHost` 字段
- `controller/clickhouse.go:1715-1812` — `AddNode` 加校验 + 透传
- `service/runner/handle.go:202-263` — `CKAddNodeHandle` 改用 `d.Ext.SourceSchemaHost`
- `controller/clickhouse.go:1925-1993` — `StartNode` 改用 `PickAvailableSchemaSource`
- `service/clickhouse/clickhouse.go:1290-1328` — `SyncLogicTable` dst 端改用 `PickAvailableSchemaSource`
- `docs/api/swagger.json` 等 swag 生成物 — `swag init` 重新生成

### 修改（前端 ckman-fe）
- `src/views/manage/manage.vue` — `<AddNodeDialog>` 传 `nodes` prop
- `src/views/manage/modal/addNode.vue` — 加下拉框 + 默认值 + 提交字段
- `src/services/i18n.ts` — 中英文两段都加 `Source Schema Host`

---

## Task 1: 后端 — PickAvailableSchemaSource 工具函数

**Files:**
- Modify: `common/ck.go`（在 `GetShardAvaliableHosts` 下方追加）
- Test: `common/ck_test.go`

为可测性，在 `common/ck.go` 引入包级可替换变量 `pickConnFn`，默认指向 `ConnectClickHouse`，测试中替换为 fake 实现。

- [ ] **Step 1: 写失败的测试**

在 `common/ck_test.go` 文件末尾追加：

```go
package common

import (
	"errors"
	"testing"

	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
)

func TestPickAvailableSchemaSource(t *testing.T) {
	conf := &model.CKManClickHouseConfig{
		Hosts: []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"},
	}

	// 保存并恢复原 fn
	orig := pickConnFn
	defer func() { pickConnFn = orig }()

	t.Run("全部可用返回首个", func(t *testing.T) {
		pickConnFn = func(host string, _ model.ConnetOption) error { return nil }
		got := PickAvailableSchemaSource(conf)
		assert.Equal(t, "10.0.0.1", got)
	})

	t.Run("首个不可用跳到下一个", func(t *testing.T) {
		pickConnFn = func(host string, _ model.ConnetOption) error {
			if host == "10.0.0.1" {
				return errors.New("connection refused")
			}
			return nil
		}
		got := PickAvailableSchemaSource(conf)
		assert.Equal(t, "10.0.0.2", got)
	})

	t.Run("全不可用返回空", func(t *testing.T) {
		pickConnFn = func(host string, _ model.ConnetOption) error { return errors.New("dead") }
		got := PickAvailableSchemaSource(conf)
		assert.Equal(t, "", got)
	})

	t.Run("exclude 跳过指定 IP", func(t *testing.T) {
		pickConnFn = func(host string, _ model.ConnetOption) error { return nil }
		got := PickAvailableSchemaSource(conf, "10.0.0.1")
		assert.Equal(t, "10.0.0.2", got)
	})

	t.Run("exclude 含空字符串不影响", func(t *testing.T) {
		pickConnFn = func(host string, _ model.ConnetOption) error { return nil }
		got := PickAvailableSchemaSource(conf, "")
		assert.Equal(t, "10.0.0.1", got)
	})
}
```

- [ ] **Step 2: 跑测试确认 FAIL**

```bash
cd /data/root/go/src/github.com/housepower/ckman
go test ./common -run TestPickAvailableSchemaSource -v
```

预期：编译错误 `undefined: pickConnFn` / `undefined: PickAvailableSchemaSource`。

- [ ] **Step 3: 实现 PickAvailableSchemaSource**

打开 `common/ck.go`，在 `GetShardAvaliableHosts` 函数（行 197-218）下方追加：

```go
// pickConnFn 默认指向真实 ClickHouse 连接，测试中可替换。
var pickConnFn = func(host string, opt model.ConnetOption) error {
	_, err := ConnectClickHouse(host, model.ClickHouseDefaultDB, opt)
	return err
}

// PickAvailableSchemaSource 在 conf.Hosts 中按顺序挑第一个能连上 ClickHouse 的节点，
// 跳过 exclude 中列出的 IP。全部不可用返回空串。
func PickAvailableSchemaSource(conf *model.CKManClickHouseConfig, exclude ...string) string {
	skip := make(map[string]struct{}, len(exclude))
	for _, e := range exclude {
		if e != "" {
			skip[e] = struct{}{}
		}
	}
	opt := conf.GetConnOption()
	for _, host := range conf.Hosts {
		if _, ok := skip[host]; ok {
			continue
		}
		if err := pickConnFn(host, opt); err == nil {
			return host
		}
	}
	return ""
}
```

- [ ] **Step 4: 跑测试确认 PASS**

```bash
go test ./common -run TestPickAvailableSchemaSource -v
```

预期：5 个子测试全部 `--- PASS`。

- [ ] **Step 5: 跑整个 common 包测试避免回归**

```bash
go test ./common -v
```

预期：无失败用例。

- [ ] **Step 6: 提交**

```bash
git add common/ck.go common/ck_test.go
git commit -m "feat(common): 新增 PickAvailableSchemaSource 工具函数

按顺序挑第一个能连上 ClickHouse 的节点，支持 exclude，覆盖
StartNode/SyncLogicTable/AddNode 的可用节点选择需求。"
```

---

## Task 2: 后端 — AddNodeReq 增加 SourceSchemaHost 字段

**Files:**
- Modify: `model/manage_ck.go:29-32`

- [ ] **Step 1: 修改结构体**

将 `model/manage_ck.go` 行 29-32 替换为：

```go
type AddNodeReq struct {
	Ips              []string `json:"ips" example:"192.168.0.1,192.168.0.2"`
	Shard            int      `json:"shard" example:"3"`
	SourceSchemaHost string   `json:"sourceSchemaHost" example:"192.168.0.10"`
}
```

- [ ] **Step 2: 编译检查**

```bash
go build ./...
```

预期：编译通过（此时尚无 controller 引用，所以不会报错）。

- [ ] **Step 3: 提交**

```bash
git add model/manage_ck.go
git commit -m "feat(model): AddNodeReq 增加 SourceSchemaHost 字段"
```

---

## Task 3: 后端 — CkDeployExt 增加 SourceSchemaHost 字段

**Files:**
- Modify: `model/deploy_ck.go:45-53`

`CkDeployExt` 是运行时载体，不入库，适合放本次操作的临时参数。

- [ ] **Step 1: 修改结构体**

将 `model/deploy_ck.go` 行 45-53 替换为：

```go
type CkDeployExt struct {
	Policy           string
	Ipv6Enable       bool
	Restart          bool
	ChangeCk         bool
	CurClusterOnly   bool //仅修改当前集群的配置
	NumCPU           int
	SkipKeeper       bool
	SourceSchemaHost string //AddNode 时由用户指定的 schema 同步源
}
```

- [ ] **Step 2: 编译**

```bash
go build ./...
```

预期：通过。

- [ ] **Step 3: 提交**

```bash
git add model/deploy_ck.go
git commit -m "feat(model): CkDeployExt 增加 SourceSchemaHost"
```

---

## Task 4: 后端 — Controller AddNode 校验并透传

**Files:**
- Modify: `controller/clickhouse.go:1715-1812`

校验顺序：非空 → 不在新增 IP 中 → 在已有 Hosts 中 → 试连成功。

- [ ] **Step 1: 在节点重复检查后追加校验逻辑**

在 `controller/clickhouse.go` 行 1758（`for _, ip := range req.Ips { ... }` 循环结束后、`shards := make(...)` 之前）插入：

```go
	// 校验 SourceSchemaHost
	if req.SourceSchemaHost == "" {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, errors.New("sourceSchemaHost is required"))
		return
	}
	for _, ip := range req.Ips {
		if req.SourceSchemaHost == ip {
			err := errors.Errorf("sourceSchemaHost %s cannot be one of the new nodes", req.SourceSchemaHost)
			controller.wrapfunc(c, model.E_INVALID_VARIABLE, err)
			return
		}
	}
	if !common.ArraySearch(req.SourceSchemaHost, conf.Hosts) {
		err := errors.Errorf("sourceSchemaHost %s is not in cluster %s", req.SourceSchemaHost, clusterName)
		controller.wrapfunc(c, model.E_INVALID_VARIABLE, err)
		return
	}
	if _, err := common.ConnectClickHouse(req.SourceSchemaHost, model.ClickHouseDefaultDB, conf.GetConnOption()); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED,
			errors.Wrapf(err, "sourceSchemaHost %s is not reachable", req.SourceSchemaHost))
		return
	}
```

- [ ] **Step 2: 把 SourceSchemaHost 写入 d.Ext**

在行 1791（`d.Packages = deploy.BuildPackages(...)` 之前）插入一行：

```go
	d.Ext.SourceSchemaHost = req.SourceSchemaHost
```

- [ ] **Step 3: 编译**

```bash
go build ./...
```

预期：通过。如果 `common.ArraySearch` 不存在，先用 `grep "func ArraySearch" /data/root/go/src/github.com/housepower/ckman/common/*.go` 确认；项目里此函数已存在（`common/util.go`）。

- [ ] **Step 4: 提交**

```bash
git add controller/clickhouse.go
git commit -m "feat(controller): AddNode 校验 SourceSchemaHost 并写入 d.Ext"
```

---

## Task 5: 后端 — Runner CKAddNodeHandle 用指定 host 同步

**Files:**
- Modify: `service/runner/handle.go:237,244`

- [ ] **Step 1: 替换 schema 源**

将 `service/runner/handle.go` 行 237 的：

```go
		if err := service.FetchSchemerFromOtherNode(conf.Hosts[0]); err != nil {
```

改为：

```go
		if err := service.FetchSchemerFromOtherNode(d.Ext.SourceSchemaHost); err != nil {
```

将行 242 的：

```go
					err = zkService.CleanZoopath(conf, conf.Cluster, conf.Hosts[0], false)
```

保持不变（这个是清理新节点对应的 zk path，应该是 `host`，让我再看下原意）。

**注意**：行 242 实际上传给 `CleanZoopath` 的是要清理的副本路径上的 host。原代码用 `conf.Hosts[0]` 看上去是 bug —— 应该是新增节点 `host`。但本次不改这行（避免范围蔓延），仅替换行 244：

```go
						if err = service.FetchSchemerFromOtherNode(d.Ext.SourceSchemaHost); err != nil {
```

- [ ] **Step 2: 编译**

```bash
go build ./...
```

预期：通过。

- [ ] **Step 3: 跑相关单测（不引入新测试，但确保不破坏）**

```bash
go test ./service/runner/... ./service/clickhouse/... -v
```

预期：现有测试全过（如果没测试就只看编译结果）。

- [ ] **Step 4: 提交**

```bash
git add service/runner/handle.go
git commit -m "feat(runner): AddNode 使用 d.Ext.SourceSchemaHost 同步 schema"
```

---

## Task 6: 后端 — StartNode 用 PickAvailableSchemaSource

**Files:**
- Modify: `controller/clickhouse.go:1942-1949`

- [ ] **Step 1: 替换循环**

将 `controller/clickhouse.go` 行 1942-1949 的：

```go
	con := conf
	//copy the original hosts
	var host string
	for _, h := range conf.Hosts {
		if h != ip {
			host = h
			break
		}
	}
	con.Hosts = []string{ip}
```

替换为：

```go
	con := conf
	host := common.PickAvailableSchemaSource(&conf, ip)
	con.Hosts = []string{ip}
```

- [ ] **Step 2: 编译**

```bash
go build ./...
```

预期：通过。

- [ ] **Step 3: 提交**

```bash
git add controller/clickhouse.go
git commit -m "feat(controller): StartNode 切换到 PickAvailableSchemaSource

避免集群首节点离线时无法上线其他节点。"
```

---

## Task 7: 后端 — SyncLogicTable dst 端用 PickAvailableSchemaSource

**Files:**
- Modify: `service/clickhouse/clickhouse.go:1313-1316`

- [ ] **Step 1: 替换 dst 连接 host**

将 `service/clickhouse/clickhouse.go` 行 1313-1316 的：

```go
	dstConn, err := common.ConnectClickHouse(dst.Hosts[0], model.ClickHouseDefaultDB, dst.GetConnOption())
	if err != nil {
		log.Logger.Warnf("can't connect %s", dst.Hosts[0])
		return false
	}
```

替换为：

```go
	dstHost := common.PickAvailableSchemaSource(&dst)
	if dstHost == "" {
		log.Logger.Warnf("no available host in cluster %s for schema sync", dst.Cluster)
		return false
	}
	dstConn, err := common.ConnectClickHouse(dstHost, model.ClickHouseDefaultDB, dst.GetConnOption())
	if err != nil {
		log.Logger.Warnf("can't connect %s", dstHost)
		return false
	}
```

- [ ] **Step 2: 编译**

```bash
go build ./...
```

预期：通过。

- [ ] **Step 3: 提交**

```bash
git add service/clickhouse/clickhouse.go
git commit -m "feat(clickhouse): SyncLogicTable dst 端切换到 PickAvailableSchemaSource"
```

---

## Task 8: 后端 — 重新生成 Swagger 文档

**Files:**
- Modify: 由 `swag init` 自动生成的 `docs/` 下文件

- [ ] **Step 1: 执行 swag init**

```bash
cd /data/root/go/src/github.com/housepower/ckman
swag init
```

预期：`docs/docs.go`、`docs/swagger.json`、`docs/swagger.yaml` 被更新，包含 `sourceSchemaHost` 字段。

如果 `swag` 未安装：`go install github.com/swaggo/swag/cmd/swag@latest`，再 `export PATH=$PATH:$(go env GOPATH)/bin` 后重试。

- [ ] **Step 2: 校验生成产物里出现新字段**

```bash
grep -n "sourceSchemaHost" docs/swagger.json
```

预期：至少匹配 1 行。

- [ ] **Step 3: 编译完整工程**

```bash
go build ./...
```

预期：通过。

- [ ] **Step 4: 提交**

```bash
git add docs/
git commit -m "docs(swagger): 重新生成 swagger 包含 sourceSchemaHost"
```

---

## Task 9: 前端（ckman-fe）— manage.vue 传 nodes 给 AddNodeDialog

**Files:**
- Modify: `/data/root/go/src/github.com/housepower/ckman-fe/src/views/manage/manage.vue:62-67`

- [ ] **Step 1: 修改 AddNodeDialog 用法**

将 `manage.vue` 行 62-67 的：

```vue
        <AddNodeDialog
          :visible.sync="addNodeDialogVisible"
          @close="addNodeDialogVisible = false"
          @onOk="onAddNodeSuccess"
          :numberRange="numberRange"
          :password="password" />
```

替换为：

```vue
        <AddNodeDialog
          :visible.sync="addNodeDialogVisible"
          :nodes="list.nodes"
          @close="addNodeDialogVisible = false"
          @onOk="onAddNodeSuccess"
          :numberRange="numberRange"
          :password="password" />
```

- [ ] **Step 2: 提交（前端仓）**

```bash
cd /data/root/go/src/github.com/housepower/ckman-fe
git add src/views/manage/manage.vue
git commit -m "feat(manage): AddNodeDialog 传入节点列表用于源选择"
```

---

## Task 10: 前端（ckman-fe）— addNode.vue 加下拉框

**Files:**
- Modify: `/data/root/go/src/github.com/housepower/ckman-fe/src/views/manage/modal/addNode.vue`

- [ ] **Step 1: 整个文件重写**

把 `src/views/manage/modal/addNode.vue` 整体替换为：

```vue
<template>
  <el-dialog
    v-bind="$attrs"
    :title="$t('manage.Add Node')"
    @close="close"
    >
    <section class="add-node">
      <el-form ref="Form"
              :model="formModel"
              label-width="150px">
        <el-form-item :label="$t('manage.New Node IP') + ':'"
                      prop="ips">
          <el-input type="textarea" v-model="formModel.ips"
                    :placeholder="$t('common.placeholderIp')"
                    class="width-350" />
        </el-form-item>
        <el-form-item :label="$t('manage.Node Shard') + ':'"
                      prop="shard">
          <el-input-number v-model="formModel.shard"
                          :step="1"
                          :min="numberRange[0]"
                          :max="numberRange[1]"></el-input-number>
        </el-form-item>
        <el-form-item :label="$t('manage.Source Schema Host') + ':'"
                      prop="sourceSchemaHost">
          <el-select v-model="formModel.sourceSchemaHost" class="width-350">
            <el-option
              v-for="n in nodes"
              :key="n.ip"
              :label="`${n.ip} (${n.status})`"
              :value="n.ip"
              :disabled="n.status === 'red'" />
          </el-select>
        </el-form-item>
      </el-form>
    </section>
    <span slot="footer" class="dialog-footer">
      <el-checkbox v-model="force" class="mr-20">{{ $t('common.Force Override') }}</el-checkbox>
      <el-button @click="close">{{$t("common.Cancel")}}</el-button>
      <el-button type="primary" @click="onOk">{{$t("common.Save")}}</el-button>
    </span>
  </el-dialog>
</template>
<script>
import { ClusterApi } from "@/apis";
import { lineFeed, getCirdOrRangeIps } from "@/helpers";
export default {
  props: {
    "numberRange": Array,
    "password": String,
    "nodes": { type: Array, default: () => [] },
  },
  data() {
    return {
      formModel: {
        ips: "",
        shard: 1,
        sourceSchemaHost: "",
      },
      force: false,
    };
  },
  watch: {
    nodes: {
      immediate: true,
      handler(list) {
        if (this.formModel.sourceSchemaHost) return;
        if (!list || list.length === 0) return;
        const green = list.find(n => n.status === 'green');
        const yellow = list.find(n => n.status === 'yellow');
        this.formModel.sourceSchemaHost = (green && green.ip) || (yellow && yellow.ip) || '';
      },
    },
  },
  methods: {
    close() {
      this.$emit('close');
    },
    async onOk() {
      const { ips, shard, sourceSchemaHost } = this.formModel;
      if (!sourceSchemaHost) {
        this.$message.error(this.$t('manage.Source Schema Host') + ' is required');
        return;
      }
      const { force, password } = this;
      const { data: { entity: taskId } } = await ClusterApi.addClusterNode(this.$route.params.id, {
        ips: getCirdOrRangeIps(lineFeed(ips)),
        shard: +shard,
        sourceSchemaHost,
      }, force, password);
      this.$emit('onOk', taskId);
      return taskId;
    }
  },
};
</script>

<style></style>
```

- [ ] **Step 2: 提交**

```bash
git add src/views/manage/modal/addNode.vue
git commit -m "feat(addNode): 增加同步 Schema 节点下拉框

默认选第一个 green 节点，无 green 则选 yellow，红色节点禁用。"
```

---

## Task 11: 前端（ckman-fe）— i18n 加新词条

**Files:**
- Modify: `/data/root/go/src/github.com/housepower/ckman-fe/src/services/i18n.ts`

i18n.ts 有两段 `manage:`：英文段（行 131 起）、中文段（行 682 起）。

- [ ] **Step 1: 在英文 manage 段插入新条目**

定位 `i18n.ts` 行 152 的 `'New Node IP': 'New Node IP',` 一行下方，插入：

```ts
      'Source Schema Host': 'Source Schema Host',
```

- [ ] **Step 2: 在中文 manage 段插入新条目**

定位行 703 的 `'New Node IP': '节点IP',` 一行下方（或同段对应位置），插入：

```ts
      'Source Schema Host': '同步Schema节点',
```

- [ ] **Step 3: 在 ckman-fe 仓内构建一次确认无 TS 错误**

```bash
cd /data/root/go/src/github.com/housepower/ckman-fe
yarn build
```

预期：构建成功。如果环境无 yarn/网络受限，可跳过此步，让后端 `make build` 重新打包时一并验证。

- [ ] **Step 4: 提交**

```bash
git add src/services/i18n.ts
git commit -m "i18n: 新增 Source Schema Host 词条"
```

---

## Task 12: 集成 — 重建前端到 ckman 仓并验证

**Files:**
- Modify: `static/dist/`（由前端构建输出，ckman 仓内 `frontend/` submodule 指针更新）

- [ ] **Step 1: 在 ckman 仓更新 frontend submodule 指针**

```bash
cd /data/root/go/src/github.com/housepower/ckman/frontend
git fetch
git checkout <ckman-fe 上一步生成的 commit hash>
cd ..
git add frontend
```

如果 `frontend` 是普通目录而非 submodule（视实际仓库配置），跳过本步，直接通过下面的 `make frontend` 构建。

- [ ] **Step 2: 构建后端 + 前端**

```bash
cd /data/root/go/src/github.com/housepower/ckman
make build
```

预期：构建成功，产物 `ckman` 二进制可启动。

- [ ] **Step 3: 手动验收清单（开发者人工执行）**

- [ ] 启动 ckman + 一个 ClickHouse 测试集群
- [ ] 访问集群管理页 → 「增加节点」按钮 → 弹窗里能看到「同步Schema节点」下拉框，包含集群所有节点，状态为 green 的默认选中
- [ ] 提交不传该字段（手动构造请求或前端置空）→ 后端返回 `sourceSchemaHost is required`
- [ ] 把 `Hosts[0]` 节点 stop 掉 → 选其他节点作 schema 源 → AddNode 任务能成功执行
- [ ] 把要选的源节点 stop 掉 → 提交 → 后端返回「is not reachable」
- [ ] 单独验证 StartNode：把 `Hosts[0]` stop，offline 一个其他节点再 online → schema 同步走 `PickAvailableSchemaSource` 选到的可用节点

- [ ] **Step 4: 提交（如更新了 frontend submodule 指针 / static/dist）**

```bash
cd /data/root/go/src/github.com/housepower/ckman
git add frontend static/dist
git commit -m "build: 同步前端构建产物（同步Schema节点下拉框）"
```

---

## Self-Review

- 后端规格全部覆盖：AddNodeReq 字段（Task 2）、CkDeployExt（Task 3）、Controller 校验（Task 4）、Runner 替换（Task 5）、StartNode（Task 6）、SyncLogicTable（Task 7）、Swagger（Task 8）。
- 前端规格全部覆盖：父组件传 nodes（Task 9）、对话框下拉框 + 默认值 + 提交字段（Task 10）、i18n（Task 11）、构建验证（Task 12）。
- TDD 范围限定在 `PickAvailableSchemaSource`（Task 1），其他后端任务因依赖真实 ClickHouse 不写新单测，仅 `go build ./...` + 手动验收。
- 字段名一致性：后端 `SourceSchemaHost` / JSON `sourceSchemaHost` / 前端 `formModel.sourceSchemaHost`。
- 中文 i18n 用「同步Schema节点」（spec 写的是「同步 Schema 节点」，无空格更紧凑符合现有词条风格如「升级集群」）。
