# 节点级个性化配置（Per-Node Override）

- 日期：2026-05-19
- 仓库：`housepower/ckman`（后端）+ `housepower/ckman-fe`（前端）

## 背景

ckman 当前的 ClickHouse 配置全部是集群级：

- 集群 `Expert map[string]string`（dotted-path）合并进 `custom.xml`，所有节点共享。
- `deploy/ck.go` 的 `Config()` 流程在每个 host 下发完全相同的 `config.d/*.xml`（仅 `host.xml` 因 IP 差异）。
- 每次部署/升级时 `deploy/ck.go:381` 执行 `rm -rf /etc/clickhouse-server/config.d/*.xml` 后重新生成全部配置。

实际运维中存在"只想给某一个节点加一项配置"的需求（例如某节点磁盘配置不同、某节点临时调优），目前无法表达。

## 目标

允许用户给单个 ClickHouse 节点添加 / 修改 / 删除一份**只对该节点生效**的 XML 配置：

1. 该配置覆盖集群级 `custom.xml` 中的同名 key。
2. 集群级部署 / 升级 / 重启不会误删该配置。
3. 可随时清空；清空 = 物理删除该文件，而不是留空 XML。
4. 保存后立即下发并 `SYSTEM RELOAD CONFIG`，无需手动重启（不能 reload 的字段由 UI 提示需重启）。
5. 前端提供双形态编辑：dotted-path 表格 + 原生 XML 编辑器。

## 非目标

- 不支持 `users.d/` 的 per-node 个性化（用户体系按集群统一管理已经够用）。
- 不做 "需重启字段" 白名单 / 自动检测（维护成本高，让用户读 toast 提示）。
- 不提供"把 A 节点的 override 复制到 B 节点"批量操作（点几下就够）。
- 不在集群 redeploy 时主动幂等下发 ckman 数据库里的 override（接受 OS 重装等边界场景需用户在 UI "重新保存" 触发下发）。

## 设计

### 1. 落盘文件 & 覆盖关系

- 文件：`/etc/clickhouse-server/config.d/node_override.xml`
- 根节点：`<clickhouse>` 或 `<yandex>`，由 `ckconfig.GetRootTag(conf.Version)` 决定。
- 字典序加载：`custom.xml < host.xml < keeper_config.xml < metrika.xml < node_override.xml`。ClickHouse 后加载的覆盖前面同名 key —— 所以 `node_override.xml` 的同名节点天然覆盖集群级 `custom.xml`。

### 2. 不被集群 deploy 误删

修改 `deploy/ck.go:381` 的清理命令，**排除** `node_override.xml`：

```go
// 原：
cmd := "rm -rf /etc/clickhouse-server/config.d/*.xml /etc/clickhouse-server/users.d/*.xml"
// 改为：
cmd := "find /etc/clickhouse-server/config.d -maxdepth 1 -name '*.xml' ! -name 'node_override.xml' -delete; rm -rf /etc/clickhouse-server/users.d/*.xml"
```

这样集群级 `deploy.Config()` 无论跑多少次，`node_override.xml` 都被保留。`deploy.Config()` 流程**不再生成 / 下发** `node_override.xml`，它完全由独立的 per-node API 管理。

注：`d.Conf.NeedSudo` 为 false 时（自定义路径部署），rm 走的是 `path.Join(d.Conf.Cwd, "etc/clickhouse-server")`，需要同样改造。具体路径以现有 deploy 流程的 `remotePath` 为准。

### 3. 数据模型

`model/deploy_ck.go` 的 `CKManClickHouseConfig` 新增字段：

```go
type CKManClickHouseConfig struct {
    ...
    Expert        map[string]string
    NodeOverrides map[string]string `json:"nodeOverrides,omitempty"` // key = host IP, value = 完整 XML 字符串（已格式化）
}
```

- 空字符串 / 不存在 = 该节点无 override。
- value 始终是后端 `PrettyXML` 处理后的格式化字符串。

### 4. XML 校验 + 格式化

后端新增工具函数 `common/xml.go`（或独立 `xmlpretty.go`）：

```go
// PrettyXML parses raw XML, validates syntax, and re-emits with 4-space indent.
// Empty / whitespace-only input is treated as empty (returns "", nil).
func PrettyXML(raw string) (string, error) {
    if strings.TrimSpace(raw) == "" {
        return "", nil
    }
    dec := xml.NewDecoder(strings.NewReader(raw))
    var buf bytes.Buffer
    enc := xml.NewEncoder(&buf)
    enc.Indent("", "    ")
    for {
        tok, err := dec.Token()
        if err == io.EOF {
            break
        }
        if err != nil {
            return "", err
        }
        if err := enc.EncodeToken(tok); err != nil {
            return "", err
        }
    }
    if err := enc.Flush(); err != nil {
        return "", err
    }
    return buf.String(), nil
}
```

PUT 接口收到 XML 后先过 `PrettyXML`，失败返回 422；成功则用格式化后的字符串入库 + scp，保证无论前端表格自动生成、XML 模式手写、curl 直调，落盘内容完全一致。

### 5. API（v2）

新增三个端点，挂在 `router/v2.go`：

#### 5.1 GET 取节点 override

```
GET /api/v2/ck/node/override/:clusterName?ip=<host>

200:
{ "code": "0000", "msg": "ok", "data": { "ip": "192.168.1.10", "xml": "<clickhouse>...</clickhouse>" } }
```

`xml` 为空字符串表示该节点无 override。

#### 5.2 PUT 保存

```
PUT /api/v2/ck/node/override/:clusterName?ip=<host>
body: { "xml": "..." }

200: { "code": "0000", "data": { "reloaded": true, "warning": "" } }
422: XML 语法非法
500: scp / reload 失败
```

后端流程（统一封装为内部函数 `applyNodeOverride(cluster, ip, xml)`）：

1. `pretty, err = PrettyXML(req.Xml)`，失败 → 422。
2. `pretty == ""` → 走 DELETE 路径（见 5.3）。
3. 否则：scp `pretty` 到该 host 的 `config.d/node_override.xml`；失败 → 500，**不更新 repository**。
4. scp 成功 → 更新 `conf.NodeOverrides[ip] = pretty` + `repository.Ps.UpdateCluster(conf)`。
5. 在该 host 执行 `SYSTEM RELOAD CONFIG`；失败 → 仍返回 200 但 `warning: "reload failed, please restart node manually"`。

#### 5.3 DELETE 清空

```
DELETE /api/v2/ck/node/override/:clusterName?ip=<host>

200: { "code": "0000", "data": { "reloaded": true } }
```

流程：

1. ssh `rm -f /etc/clickhouse-server/config.d/node_override.xml`；失败 → 500，不更新 repository。
2. 成功 → `delete(conf.NodeOverrides, ip)` + `UpdateCluster`。
3. `SYSTEM RELOAD CONFIG`；失败 → 200 + warning。

#### 5.4 权限

复用 `controller/clickhouse.go` 现有的认证中间件；本功能视为集群运维操作，需要集群编辑权限。

### 6. 前端

#### 6.1 入口

`ckman-fe/src/views/manage/manage.vue`，节点行 `...` 下拉菜单新增一项 `"个性化配置"`：

```vue
<el-dropdown-item command="override">
  <i class="el-icon-setting"></i>
  {{ $t('manage.Node Override') }}
</el-dropdown-item>
```

`onNodeCommand` 触发 `openOverrideDialog(row)`。

#### 6.2 弹窗组件

新建 `src/views/manage/modal/nodeOverride.vue`：

```
┌──────────────────────────────────────────────────┐
│ 个性化配置 · 192.168.1.10                        │
├──────────────────────────────────────────────────┤
│ [ 表格 ] [ XML 高级 ]    ⓘ 部分配置需重启节点    │
├──────────────────────────────────────────────────┤
│ 表格模式：                                        │
│   + 新增                                          │
│   key                       | value | actions    │
│   -------------------------- | ----- | -------   │
│   merge_tree.parts_to_throw  | 300   | 🗑        │
│   <无法扁平化项 标灰只读>     | ...   | 🗑        │
│                                                   │
│ XML 模式：                                        │
│   CodeMirror / Monaco XML editor                  │
├──────────────────────────────────────────────────┤
│            [清空] [取消] [保存]                   │
└──────────────────────────────────────────────────┘
```

#### 6.3 双形态切换逻辑

- 组件内 single source of truth：**当前 XML 字符串** `currentXml`。
- 打开弹窗时 GET API 拿到的 XML 直接灌进 `currentXml`。
- **表格模式**渲染：
  - 调用 `parseXmlToRows(currentXml)` 把 XML 扁平化为 `{ path: 'a.b.c', value: '300', editable: true }[]`。
  - 不可扁平化的节点（含属性、重复同名兄弟、混合文本+子元素）→ `editable: false`，UI 标灰，仅允许删除整项。
  - 表格内编辑 → `rowsToXml(rows)` 同步回写 `currentXml`。
- **XML 模式**渲染：
  - 直接绑定 `currentXml` 到编辑器。
  - 失焦或保存前用前端 XML 解析做一次语法预检查，错误下方红字提示，禁用保存按钮。
- 切换标签时单向投影：
  - XML → 表格：重新 `parseXmlToRows`，已存在的不可扁平化结构标灰。
  - 表格 → XML：用 `rowsToXml` 重建 XML。
- 保存按钮：始终 PUT `currentXml`（无论当前哪种模式）。
- 清空按钮：调 DELETE 接口。

#### 6.4 编辑器选型

- 优先 CodeMirror 6 + lang-xml（体积小）。
- 若项目已有 Monaco 依赖（待确认 `package.json`），复用 Monaco。

#### 6.5 i18n

新增 key（中英文），放入 `src/services/i18n.ts` 的 `manage` 命名空间：

- `Node Override` / 个性化配置
- `Node Override Tip` / 部分配置项需重启节点才能生效
- `Save Override Success` / 已下发并执行 RELOAD CONFIG
- `Save Override Warning Reload Failed` / 配置已保存但 RELOAD 失败，请手动重启节点
- `Override Clear Confirm` / 确认清空该节点的个性化配置？

### 7. 错误处理 & 用户反馈

| 场景 | 后端 | 前端表现 |
|---|---|---|
| XML 语法非法 | 422 | 编辑器下方红字 + 保存按钮置灰 |
| scp 失败 | 500，不入库 | toast "下发失败：xxx" |
| reload 失败 | 200 + warning | toast warning "已保存，但 RELOAD CONFIG 失败，请重启节点" |
| 节点 SSH 不通 | 500 | toast "SSH 连接失败" |
| 节点不在 conf.Hosts | 404 | toast "节点不存在" |

### 8. 测试

#### 8.1 单元测试

- `common.PrettyXML`：合法 XML、空 XML、仅空白、非法 XML、带注释、带 CDATA、带属性、嵌套深度。
- `applyNodeOverride`：scp 失败回滚、reload 失败保留入库 + warning。

#### 8.2 集成 / 手测

- 给节点 A 加 `merge_tree.parts_to_throw_insert = 300`，登录该节点查 `SHOW MERGE TREE SETTINGS WHERE name='parts_to_throw_insert'`，验证生效。
- 集群级 `Expert` 也设了同名 key，验证 override 覆盖（看 `<yandex>` / `<clickhouse>` 合并后值）。
- 集群级 redeploy，验证 `node_override.xml` 未被删。
- DELETE 后查 `ls config.d/`，验证文件已物理删除。
- 表格模式编辑 → 切到 XML 模式 → 检查格式化是否漂亮、是否包含表格里的修改。
- XML 模式写一个带属性的节点（`<disk name="x">`）→ 切到表格模式，验证该项标灰只读。

## 实施步骤（按提交粒度）

1. **后端：PrettyXML 工具 + 单测**（独立 commit，便于复用）。
2. **后端：CKManClickHouseConfig 加 NodeOverrides 字段 + repository 序列化兼容**（确认各 backend：local/sqlite/mysql/postgres/dm8 都用 JSON marshal 即可自动适配，不需 schema migration）。
3. **后端：修改 deploy/ck.go 的 rm 命令排除 node_override.xml**（含 NeedSudo 两种路径）。
4. **后端：API（GET / PUT / DELETE）+ applyNodeOverride 内部函数**。
5. **后端：swag init 更新 docs**。
6. **前端：API 客户端 + nodeOverride.vue 弹窗组件 + manage.vue 入口 + i18n**。
7. **前端：表格 ↔ XML 双形态切换、不可扁平化检测、编辑器选型落地**。
8. **make build / make test** 端到端验证。

## 风险

- **PrettyXML 不保留注释 / 处理指令**：`encoding/xml` 的 Token API 会保留 Comment / ProcInst，但格式细节（空白）会重写。可接受。
- **XML 注入**：用户提交的 XML 直接写到节点 config.d/。ClickHouse 自身对 config.d 文件解析失败时会拒绝启动 → 出错时通过 RELOAD CONFIG 失败被捕获。但若用户写了能正常解析的"恶意"配置（如指向不存在的存储路径），可能影响节点。这与已有 Expert 字段的风险等价，已通过用户认证收敛。
- **CodeMirror / Monaco 包体积**：若引入新依赖，前端构建产物变大。先选 CodeMirror 6（按需 import lang-xml，gzip 后约 30KB）。
- **节点级 SSH 操作放大**：每次保存触发一次 SSH。高频保存可能压力大，但属于运维低频操作，可接受。
