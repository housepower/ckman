# ClickHouse 用户管理:SQL(RBAC) vs 配置文件

本文梳理 ClickHouse 两种用户管理方式的区别、profile/quota 的 SQL 管理能力,以及推荐的落地方案。

---

## 一、两种创建方式的核心区别

ClickHouse 创建用户有两条路径:**SQL(RBAC)** 和 **配置文件(users.xml)**。二者是两套并存的机制。

| 维度 | SQL 创建 (`CREATE USER`) | 配置文件创建 (users.xml) |
|------|------------------------|------------------------|
| 存储位置 | `access_control_path` 目录下的 `.sql` 文件(默认 `/var/lib/clickhouse/access/`) | `users.xml` / `users.d/*.xml` |
| 生效方式 | 立即生效,无需重启 | 需要 reload 配置,部分改动需重启 |
| 是否可被 SQL 修改 | 可以(`ALTER USER` / `DROP USER`) | **不可以**,SQL 无法修改/删除 xml 里定义的用户 |
| 集群同步 | 可配合 ZooKeeper/Keeper 做 replicated 存储,全集群统一 | 每个节点各自的文件,需要自己分发 |
| 权限模型 | 完整 RBAC(ROLE、ROW POLICY、QUOTA、GRANT 精细授权) | 相对静态,靠 profile + 简单权限 |
| 密码修改 | `ALTER USER ... IDENTIFIED BY` | 改文件 |
| 谁能创建 | 需要有 `ACCESS MANAGEMENT` 权限的用户 | 有服务器文件系统访问权限的运维 |

### 关键理解点

1. **两套系统并存**:ClickHouse 会同时加载 xml 用户和 SQL 用户。xml 定义的用户对 SQL 层是"只读"的——在客户端 `DROP USER` 一个 xml 用户会报错。
2. **xml 用户是"引导/兜底"用户**:通常 `default` 用户就定义在 xml 里。常见做法是先用 xml 建一个有 `access_management=1` 的管理员,再用它通过 SQL 做后续所有用户/权限管理。
3. **动态 vs 静态**:
   - SQL 方式适合**运行时动态管理**、多用户多角色、需要审计和精细授权的生产场景。
   - xml 方式适合**部署时固定配置**、基础设施即代码、不希望被运行时改动的场景。

---

## 二、Profile 与 Quota 的 SQL 管理

profile 和 quota 都是 RBAC 的**一等对象**,有完整的 `CREATE / ALTER / DROP / SHOW` 生命周期,且可绑定到 user 或 role。

### SETTINGS PROFILE 全套 SQL

```sql
-- 创建
CREATE SETTINGS PROFILE profile_a SETTINGS
    max_memory_usage = 10000000000,
    max_threads = 8,
    readonly = 1;

-- 修改
ALTER SETTINGS PROFILE profile_a SETTINGS max_threads = 16;

-- 继承其他 profile
CREATE SETTINGS PROFILE profile_b SETTINGS INHERIT 'profile_a';

-- 约束(限制用户能否自己改这些 setting)
CREATE SETTINGS PROFILE profile_c SETTINGS max_memory_usage = 5000000000 READONLY;

-- 绑定到 user / role
ALTER SETTINGS PROFILE profile_a TO alice, analyst;

-- 删除 / 查看
DROP SETTINGS PROFILE profile_a;
SHOW CREATE SETTINGS PROFILE profile_a;
SELECT * FROM system.settings_profiles;
```

> 注意:xml 里的 `<profiles>` 和 SQL 的 `SETTINGS PROFILE` 是**同一套东西**,底层共享。xml 里定义的 profile 名字,SQL `CREATE USER ... SETTINGS PROFILE 'xxx'` 也能引用到。

### QUOTA 全套 SQL

```sql
-- 创建(可定义多个时间区间)
CREATE QUOTA quota_a
    FOR INTERVAL 1 hour MAX queries = 1000, errors = 100,
    FOR INTERVAL 1 day  MAX read_rows = 1000000000, execution_time = 3600
    TO alice;

-- 修改
ALTER QUOTA quota_a FOR INTERVAL 1 hour MAX queries = 2000;

-- 按 key 维度隔离(每个用户/IP 各自计数)
CREATE QUOTA quota_b KEYED BY user_name
    FOR INTERVAL 1 minute MAX queries = 100
    TO ALL EXCEPT admin;

-- 重新绑定 / 删除 / 查看
ALTER QUOTA quota_a TO bob, analyst;
DROP QUOTA quota_a;
SHOW CREATE QUOTA quota_a;
SELECT * FROM system.quotas;
SELECT * FROM system.quota_usage;   -- 实时用量
```

### 绑定能力对比

| | SQL RBAC | xml |
|---|---|---|
| profile 绑定 | `SETTINGS PROFILE 'x'`,可绑 user 或 role | `<profile>x</profile>` 只绑 user |
| quota 绑定 | `CREATE/ALTER QUOTA ... TO`,可绑 user/role/ALL EXCEPT | `<quota>x</quota>` 只绑 user |
| 绑到 ROLE | ✅ 支持,继承更灵活 | ❌ 不支持 role |
| 运行时修改 | ✅ 立即生效 | ❌ 需 reload/重启 |

RBAC 最大的价值是**可以把 profile/quota 绑到 ROLE 上**,再 `GRANT role TO user`,管理粒度是"角色"而非"每个用户单独配"。典型分层:

```
SETTINGS PROFILE ─┐
                  ├─> ROLE ─> GRANT ─> USER
QUOTA ────────────┘
```

### 使用前提

1. **有权限**:`GRANT ACCESS MANAGEMENT ON *.* TO admin;`
2. **SQL-driven access control 已开启**:执行 DDL 的用户在配置里要有 `<access_management>1</access_management>`(通常先在 xml 里给引导用户打开)。

### 相关系统表

| 系统表 | 用途 |
|--------|------|
| `system.settings_profiles` | 所有 profile |
| `system.settings_profile_elements` | profile 里每条 setting |
| `system.quotas` | 所有 quota 定义 |
| `system.quota_limits` | quota 各区间限额 |
| `system.quota_usage` | 当前用户实时用量 |
| `system.quotas_usage` | 所有用户用量(需权限) |

---

## 三、配置文件的优势

纯从灵活性看 RBAC 完胜,但 xml 并未被淘汰。它的优势是**"确定性"和"引导性"**,而非灵活性。

1. **引导 / 鸡生蛋问题(最关键)**
   RBAC 需要一个有 `ACCESS MANAGEMENT` 权限的用户来创建,而这第一个管理员只能来自 xml。集群全新启动、access 存储损坏、Keeper 挂掉时,xml 用户是**唯一的兜底登录通道**。xml 是 RBAC 的地基。

2. **基础设施即代码 / 可复现**
   xml 是静态文件,天然适合纳入 Git、Ansible、K8s ConfigMap:配置**版本化、可 diff、可回滚、可审计**;多节点分发一致性有保证。

3. **防篡改 / 强约束**
   xml 定义的用户 SQL 层无法 `DROP`/`ALTER`。想要一个"谁都删不掉、改不动"的管理/审计账号,xml 最稳。

4. **不依赖任何在线组件**
   xml 在进程启动阶段加载,不依赖 Keeper、不依赖 SQL access 存储健康。灾难恢复场景更可靠。

5. **无权限门槛**
   改 xml 只需文件系统 + 重启权限。忘记密码、锁死自己时,是"物理层"救回来的手段。

### 对比总结

| 维度 | xml 配置文件 | SQL RBAC |
|------|-------------|----------|
| 定位 | 静态**地基 / 兜底** | 动态**业务管理** |
| 灵活性 | 低 | 高 |
| 动态生效 | 需 reload/重启 | 即时 |
| role 支持 | ❌ | ✅ |
| 可被运行时篡改 | ❌(优势:防改) | ✅ |
| 依赖在线组件 | 无 | replicated 模式依赖 Keeper |
| IaC / 版本化 | ✅ 天然 | 需额外导出 |
| 引导管理员 | ✅ 唯一来源 | 依赖 xml |

---

## 四、推荐落地方案

**结论:xml 只管"最小、最稳、最不该变的那部分",RBAC 管"其余所有会变的部分"。**

常见诉求是"只在配置文件里配好一个管理员并给 `ACCESS MANAGEMENT`,后续全走 RBAC"——方向正确,但**不要直接用 `default` 当管理员**。

### 为什么不用 default

1. **default 是集群内部通信账号**:分布式查询、副本间交互(`remote()`、Distributed 表、副本同步)默认用 `default` 身份互连,给它挂最高权限安全面太大。
2. **default 常被配成无密码**:拿它做管理员风险高。
3. **职责混乱**:内部通信账号 + 人类管理员混在一起,审计时分不清谁干的。

### 推荐:xml 里单独建一个引导管理员

```xml
<!-- users.xml / users.d/admin.xml -->
<clickhouse>
    <users>
        <!-- default 保持本职:内部通信,权限收窄 -->
        <default>
            <password_sha256_hex>...</password_sha256_hex>
            <access_management>0</access_management>
        </default>

        <!-- 专门的引导管理员 -->
        <ckadmin>
            <password_sha256_hex>...</password_sha256_hex>
            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
            <show_named_collections>1</show_named_collections>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
        </ckadmin>
    </users>
</clickhouse>
```

### 必做前提:开启 replicated access 存储

否则 SQL 建的用户**只存在于执行那台节点**,不会同步到全集群。在 `config.xml` 配:

```xml
<clickhouse>
    <user_directories>
        <users_xml>
            <path>users.xml</path>
        </users_xml>
        <!-- SQL 建的用户存本地 -->
        <local_directory>
            <path>/var/lib/clickhouse/access/</path>
        </local_directory>
        <!-- 关键:通过 Keeper 全集群同步 RBAC 对象 -->
        <replicated>
            <zookeeper_path>/clickhouse/access/</zookeeper_path>
        </replicated>
    </user_directories>
</clickhouse>
```

配了 `<replicated>` 后,在任意节点 `CREATE USER` 都会通过 Keeper 同步到所有节点;**没配它就得逐节点执行 SQL**,失去 RBAC 的意义。

### 完整落地公式

| 步骤 | 放在哪 | 内容 |
|------|--------|------|
| 1. 收窄 default | xml | 内部通信用,`access_management=0` |
| 2. 建引导管理员 | xml | `ckadmin` + `access_management=1` |
| 3. 开集群同步 | config.xml | `user_directories` 加 `<replicated>`(依赖 Keeper) |
| 4. 其余全部 | SQL RBAC | user / role / grant / profile / quota,动态管理 |

最终形态:

```
xml:  只放 default / 引导管理员 / 少量固定系统账号 + 基础 profile
       ↓ (用引导管理员登入)
SQL:  所有业务用户、role、精细 GRANT、动态 profile/quota
```

这样 xml 只承担"地基 + 兜底 + 引导"三件最小的事;日常一切在 RBAC 里动态管理、即时生效、全集群同步;default 回归本职,安全边界清晰。
