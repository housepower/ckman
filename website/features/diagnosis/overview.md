# AI 场景诊断

CKMAN 把诊断入口放在异常发生的工作现场：先用 ClickHouse 系统表完成确定性检查，再由可选 LLM 把结构化证据解释成一轮中文结论。它不是聊天机器人，也不会根据模型猜测直接执行修复动作。

## 支持的场景

| 入口 | 诊断范围 | 重点检查 |
| --- | --- | --- |
| 管理集群 → AI 诊断资源异常 | 当前集群 | CPU、内存、磁盘压力与磁盘空间 |
| 复制表状态 → AI 诊断 | 指定数据库和表 | 副本状态、复制队列积压与失败 |
| 表指标 → AI 诊断 | 指定数据库和表 | detached parts、too many parts、卡住的 mutation |
| 慢查询 → AI 诊断 | 指定 `query_id` | 查询耗时、资源消耗与执行证据 |
| 分布式 DDL → AI 诊断 | 指定 DDL entry | 未完成节点、异常与等待状态 |

表级诊断只采集当前表的数据，不会为了诊断一张表扫描全库，既降低 ClickHouse 查询开销，也避免把无关内容送入模型上下文。

![在表指标页面对指定表执行健康诊断，依次展示确定性检查与 AI 一次性解释](/img/features/diagnosis/table-health.png)

慢查询诊断直接从慢查询列表中的目标行发起，并使用该行的 `query_id` 采集对应节点证据：

![在慢查询列表中诊断指定 query_id，右侧抽屉流式展示结论](/img/features/diagnosis/slow-query.png)

## 一次诊断如何工作

1. CKMAN 根据当前页面生成带范围的诊断任务。
2. 诊断库对集群各副本执行只读系统表查询，并保留无法连接的节点作为覆盖缺口。
3. 页面先展示执行状态、覆盖率、发现结果和逐项结构化证据。
4. 启用 LLM 后，CKMAN 将裁剪、脱敏后的结构化报告发送给模型，并流式渲染一次性 Markdown 结论。

结构化报告始终是事实源。模型解释与报告冲突时，以报告中的 `Execution`、`Outcome`、`Severity` 和节点证据为准。

## 结果状态

| 字段 | 含义 |
| --- | --- |
| `Execution` | 本次采集是否完整；`partial` 表示仅覆盖部分节点 |
| `Outcome` | 检查是否发现匹配异常；`no_match` 不等于整个集群健康 |
| `Severity` | 基于确定性规则计算的严重程度 |

::: warning 覆盖率优先
节点不可达、权限不足或系统表版本不兼容时，诊断可能是 `partial` 或 `failed`。此时应先处理覆盖缺口，不能把“未获取证据”理解成“没有异常”。
:::

## LLM 是可选增强

未配置 LLM 时，确定性诊断、结构化发现和节点证据仍可正常使用。启用后只增加解释层：

- 一次诊断只生成一轮结论，不提供追问入口。
- 不生成或执行 `ALTER`、`KILL`、重启命令和 shell 命令。
- API key 不写入主配置，通过环境变量或只读 secret file 提供。
- 默认不发送原始异常文本，降低敏感信息进入第三方模型的风险。

配置方法参见 [LLM 与 AI 诊断](/config/llm)。

## API

诊断 API 位于 `/api/v1/diagnostics`：

- `GET /checks`：查看可用检查、场景和 LLM 状态
- `POST /:cluster/runs`：创建诊断任务
- `GET /:cluster/runs/:runId`：读取状态和结构化报告
- `POST /:cluster/runs/:runId/cancel`：取消排队中或运行中的任务
- `POST /:cluster/runs/:runId/explain`：以 SSE 流式返回 AI 解释

完整请求结构可在 [API Playground](/reference/api-playground) 中查看。
