# 用户与角色管理 Phase 1 重构设计

- 状态：草案 → 待 review
- 目标版本：next release
- 范围：用户存储、用户管理 API、前端管理页、enforce 改造、`cmd/password` 清理、顺带补 5 处接口策略缺口
- 非范围：接口权限可配置化（Phase 2 已取消，权限规则维持代码内硬编码）、审计日志、多 admin、数据级权限、API v2

## 1. 背景

ckman 当前的用户与角色权限实现存在以下不足：

- 用户凭据以"AES 加密文件名 + bcrypt(md5) 内容"形式散落在 `conf/users/` 与 `conf/password`，没有 HTTP 管理接口
- 用户管理只能通过本地 CLI 工具 `cmd/password` 交互式完成，无法远程运维
- 角色常量与每个角色的接口白名单（`server/enforce/{guest,ordinary}.go`）硬编码在 Go 代码里
- 部分新增路由（partition PUT、backup policy PUT、`/data_manage/disks/*`、`/data_manage/tables/*` 等）从未补入策略白名单，且存在僵尸规则 `/data_manage/backup_history/*`

Phase 1 目标：把"用户存储 + 管理 UI"做到生产级；接口权限规则继续硬编码在代码里（git review + 发布作为安全基线，避免 DB 改写带来的提权风险）。

## 2. 关键决策记录

| # | 决策 | 选择 | 备注 |
|---|---|---|---|
| 1 | 角色模型 | 固定 3 角色 `admin/guest/ordinary` 不可增删 | policy 作为字符串列存于 users 表 |
| 2 | 旧文件迁移 | 不迁移，升级即重置 | 旧 `conf/password`、`conf/users/*` 留磁盘备份不读 |
| 3 | 默认管理员 | 用户名 `ckman`、密码 `Ckman123456!`、唯一不可改 | 启动时若 users 表无该记录则种子写入 |
| 4 | `cmd/password` CLI | 删除整目录 | UI 取代 |
| 5 | unified portal sentinel | 保留 `ordinary` 作保留用户名 | 不入 DB，给第三方 API 调用使用 |
| 6 | 首次登录强制改密 | 不做 | 沿用现有习惯，仅启动 WARN 日志提醒 |
| 7 | 密码哈希 | 登录沿用 `bcrypt(md5(plain))`；用户 CRUD 改密前端送明文 | 登录契约不动；新接口用明文 + 服务端做复杂度校验 |
| 8 | 密码复杂度规则 | 沿用 `common.VerifyPassword` 现有规则 | ≥8 位、3 类字符 |
| 9 | 多后端支持 | sqlite / mysql / postgres / dm8 全部 | 4 份镜像 DDL + DAO |
| 10 | 用户字段 | + `enabled` 启用标志，不加 `display_name` / `last_login_at` / `must_change_password` / `deleted_at` | 软删用 enabled 表达 |
| 11 | 运行时缓存 | 不做应用层缓存，每次鉴权查 DB | 用户数极少，DB 压力可忽略 |
| 12 | 用户列表 UI | 不分页 / 不搜索 / 按 username 升序 | 列：用户名 / 角色 / 启用 / 创建时间 / 操作 |
| 13 | 密码修改权限矩阵 | admin 改自己要旧密码；admin 改他人不要；普通用户改自己要；普通用户改他人 403 | 见 §6.2 路由设计 |
| 14 | 用户名规则 | `^[A-Za-z][A-Za-z0-9_]{2,31}$` | `ckman`、`ordinary` 为保留名 |
| 15 | 删除策略 | 硬删 + 撤销该用户全部 token | `ckman` 不可删 |
| 16 | 审计日志 | 不做 | 留待后续 |
| 17 | 普通用户菜单 | 前端隐藏入口 + 后端 403 | 输入 URL 跳首页 + 后端 enforce 拒 |
| 18 | 顶栏入口位置 | `安装包` 图标右侧；admin 可见 | 用 `fa-users` 图标 |
| 19 | 启用状态控件 | 内联 `<el-switch>` + 二次确认 | 体验更快 |
| 20 | LoginRsp 字段 | 扩展 `policy` 与 `enabled` 两个字段 | 登录后避免多调 `/user/me` |
| 21 | enforce 策略补丁 | Phase 1 同一 PR、独立 commit | 4 加 1 删 |
| 22 | API 版本 | 沿用 `/api/v1`，不开 v2 | YAGNI |
| 23 | Phase 2 | 取消 | 接口权限继续代码硬编码 |

## 3. 数据模型

新增 GORM 表 `ckman_users`（每后端各一份）。逻辑结构：

```
ckman_users
├── id            BIGINT PK              (gorm.Model 自带)
├── created_at    DATETIME               (gorm.Model 自带)
├── updated_at    DATETIME               (gorm.Model 自带)
├── deleted_at    DATETIME nullable      (gorm.Model 自带，本表不使用)
├── username      VARCHAR(32) UNIQUE NOT NULL
├── password_hash VARCHAR(64) NOT NULL   (bcrypt(md5(plain))，60 字节 + 余量)
├── policy        VARCHAR(16) NOT NULL   (admin|guest|ordinary，应用层校验)
└── enabled       BOOLEAN NOT NULL DEFAULT TRUE
```

`gorm.Model` 嵌入会带来未使用的 `deleted_at` 列，是与其他表保持风格一致的小代价，接受。

应用层模型 `model/user.go`：

```go
type CkmanUser struct {
    ID           int64  `json:"-"`
    Username     string `json:"username"`
    PasswordHash string `json:"-"`
    Policy       string `json:"policy"`
    Enabled      bool   `json:"enabled"`
    CreatedAt    int64  `json:"created_at"`
    UpdatedAt    int64  `json:"updated_at"`
}
```

Policy 常量复用 `common/user.go` 现有 `ADMIN/GUEST/ORDINARY`。

## 4. 持久化层

### 4.1 接口

`repository/persistent.go` 追加：

```go
type PersistentUserService interface {
    GetUserByName(username string) (model.CkmanUser, error)
    UserExists(username string) bool
    GetAllUsers() ([]model.CkmanUser, error)
    CreateUser(u model.CkmanUser) error
    UpdateUser(u model.CkmanUser) error
    DeleteUser(username string) error
}

type PersistentMgr interface {
    PersistentBase
    ...                              // 现有接口
    PersistentUserService            // 新增
}
```

`GetUserByName` 在不存在时返回 `repository.ErrRecordNotFound`（与现有 cluster 接口一致）。`CreateUser` 把 UNIQUE 冲突归一化为 `repository.ErrRecordExists`（如果常量未定义则新增）。

### 4.2 四份 GORM 模型

`repository/{sqlite,mysql,postgres,dm8}/model.go` 各加：

```go
type TblUser struct {
    gorm.Model
    Username     string `gorm:"index:idx_user_name,unique; column:username; size:32; not null"`
    PasswordHash string `gorm:"column:password_hash; size:64; not null"`
    Policy       string `gorm:"column:policy; size:16; not null"`
    Enabled      bool   `gorm:"column:enabled; not null; default:true"`
}
func (TblUser) TableName() string { return <BACKEND>_TBL_USER }
```

各后端 `constant.go` 加表名常量 `SQLITE_TBL_USER = "tbl_user"` / `MYSQL_TBL_USER` / `POSTGRES_TBL_USER` / `DM8_TBL_USER`。

各后端 `Init()` 的 `AutoMigrate` 调用追加 `&TblUser{}`。

### 4.3 DAO

每后端实现 6 个方法，严格仿照 `GetClusterbyName/CreateCluster` 等的写法。模型 ↔ Tbl 的转换函数放在各后端 `model.go`。

### 4.4 启动种子

在 `repository/persistent.go` 的 `InitPersistent()` 末尾追加：

```go
if err := seedAdminIfAbsent(); err != nil {
    return err
}
```

```go
func seedAdminIfAbsent() error {
    if Ps.UserExists(common.DefaultAdminName) {
        return nil
    }
    md5pw := fmt.Sprintf("%x", md5.Sum([]byte(common.DefaultAdminPassword)))
    hash, err := common.HashPassword(md5pw)
    if err != nil { return err }
    now := time.Now().Unix()
    err = Ps.CreateUser(model.CkmanUser{
        Username:     common.DefaultAdminName,
        PasswordHash: hash,
        Policy:       common.ADMIN,
        Enabled:      true,
        CreatedAt:    now,
        UpdatedAt:    now,
    })
    if errors.Is(err, repository.ErrRecordExists) {
        return nil           // 多实例并发种子：UNIQUE 冲突视为成功
    }
    if err == nil {
        log.Logger.Warnf("bootstrap: seeded admin user '%s' with default password '%s'.",
            common.DefaultAdminName, common.DefaultAdminPassword)
        log.Logger.Warnf("bootstrap: please log in via Web UI and change the password immediately.")
    }
    return err
}
```

## 5. 鉴权改造

### 5.1 `common/user.go` 清理

**保留**：常量 `ADMIN`、`GUEST`、`ORDINARY`、`DefaultAdminName`、`InternalOrdinaryName`，新增 `DefaultAdminPassword = "Ckman123456!"`。

**删除**：`type UserInfo`、`var UserMap`、`var lock`、`func LoadUsers`、`func GetUserInfo`。

### 5.2 调用点改造

| 文件 | 现状 | 改造后 |
|---|---|---|
| `server/server.go:118` | `common.LoadUsers(...)` | 删除 |
| `controller/user.go:43` | `common.LoadUsers(...)` | 删除 |
| `controller/user.go:49` | `common.GetUserInfo(req.Username)` | `repository.Ps.GetUserByName(req.Username)` |
| `controller/user.go` 新逻辑 | — | 校验 `user.Enabled`；false 返回 `E_LOGIN_DISABLED` |
| `server/enforce/enforce.go:72` | `common.GetUserInfo(username)` | `repository.Ps.GetUserByName(username)` |
| `server/enforce/enforce.go:29` | 字段 `orinary` | 修正为 `ordinary`（typo） |

### 5.3 `enforce.Enforce` 新实现

```go
func Enforce(username, url, method string) bool {
    if username == common.DefaultAdminName {        // admin 不查 DB
        return true
    }
    var policies []Policy
    if username == common.InternalOrdinaryName {    // portal sentinel
        policies = e.ordinary
    } else {
        user, err := repository.Ps.GetUserByName(username)
        if err != nil || !user.Enabled {
            return false
        }
        switch user.Policy {
        case common.GUEST:    policies = e.guest
        case common.ORDINARY: policies = e.ordinary
        case common.ADMIN:    return true            // 防御性
        default:              return false
        }
    }
    for _, p := range policies {
        if e.Match(p.URL, url) && p.Method == method {
            return true
        }
    }
    return false
}
```

### 5.4 Token 撤销

| 事件 | Token 是否失效 |
|---|---|
| Logout | 是（现有 `TokenCache.Delete`） |
| 过期 | 是（TTL） |
| 删除用户 | 是（删除接口内部撤销）|
| 禁用用户 | 否，但下一次 enforce 查 DB 时被拒，等同失效 |
| 改密码（自己 / 他人） | 否（与主流 SaaS 行为一致） |
| 改角色 | 否，但下一次 enforce 拿到新策略生效 |

按 username 撤销 token 的实现：遍历 `TokenCache.Items()`，对每个 key（token 字符串）解析 JWT claims，比对 `claims.Name == username` 则 `TokenCache.Delete`。删除用户是低频人工操作，性能损耗可忽略。

## 6. HTTP API

### 6.1 路由分组

沿用 `/api/v1`。新建 `controller.UsernamePath = "username"` 常量于 `controller/controller.go`。

### 6.2 端点

| # | Method + Path | 鉴权 | 用途 |
|---|---|---|---|
| 1 | `GET /api/v1/user/me` | 任意已登录用户 | 查自己（username / policy / enabled） |
| 2 | `PUT /api/v1/user/password` | 任意已登录用户 | 改自己密码（带旧密码） |
| 3 | `GET /api/v1/users` | admin only | 列表 |
| 4 | `POST /api/v1/users` | admin only | 创建用户 |
| 5 | `PUT /api/v1/users/:username` | admin only | 改 policy / enabled |
| 6 | `DELETE /api/v1/users/:username` | admin only | 硬删 + 撤 token |
| 7 | `PUT /api/v1/users/:username/password` | admin only | 重置他人密码（无旧密码） |

`/user/...`（单数）= 自助；`/users/...`（复数）= admin 管理。语义自然，enforce 白名单只需放 `/user/me`、`/user/password` 两条到 `guest.go` 即可（`ordinary` 通过 `append(GuestPolicies()...)` 继承）。

### 6.3 请求 / 响应 DTO

```go
type UserListItem struct {
    Username  string `json:"username"`
    Policy    string `json:"policy"`
    Enabled   bool   `json:"enabled"`
    CreatedAt int64  `json:"created_at"`
    UpdatedAt int64  `json:"updated_at"`
}

type CreateUserReq struct {
    Username string `json:"username"`
    Password string `json:"password"`   // 明文，HTTPS 传输
    Policy   string `json:"policy"`     // guest|ordinary
    Enabled  bool   `json:"enabled"`
}

type UpdateUserReq struct {
    Policy  string `json:"policy"`      // 空字符串=不改
    Enabled *bool  `json:"enabled"`     // 指针区分未传与 false
}

type ChangeMyPasswordReq struct {
    OldPassword string `json:"old_password"`   // 明文
    NewPassword string `json:"new_password"`   // 明文
}

type ResetPasswordReq struct {
    NewPassword string `json:"new_password"`   // 明文
}

// 扩展现有 LoginRsp
type LoginRsp struct {
    Username string `json:"username"`
    Token    string `json:"token"`
    Policy   string `json:"policy"`    // 新增
    Enabled  bool   `json:"enabled"`   // 新增
}
```

### 6.4 服务端校验

1. 用户名 `^[A-Za-z][A-Za-z0-9_]{2,31}$`；保留名 `ckman`、`ordinary` 禁止创建
2. 密码：`common.VerifyPassword`（≥8 位、3 类字符）—— 改密接口拿到明文，可直接调
3. policy：创建/编辑限定为 `guest|ordinary`；`ckman` 的 policy 不可改
4. 删除 / 编辑 `ckman`：返回 `E_FORBIDDEN_TARGET`
5. `enabled` 在登录时校验，false → `E_LOGIN_DISABLED`

### 6.5 错误码（`model/code.go` 新增）

```go
E_LOGIN_DISABLED        = "5032"
E_USER_ALREADY_EXISTS   = "5033"
E_INVALID_USERNAME      = "5034"
E_INVALID_POLICY        = "5035"
E_FORBIDDEN_TARGET      = "5036"
E_OLD_PASSWORD_MISMATCH = "5037"
E_USER_NOT_FOUND        = "5038"
```

### 6.6 enforce 白名单新增

`server/enforce/guest.go` 追加：

```go
{"/user/me", GET},
{"/user/password", PUT},
```

`/users` 路径**不进任何白名单**，仅 admin 可调。

## 7. enforce 策略补丁（独立 commit）

修补缺漏与僵尸规则。按你"集群级运维仅 admin / 表级运维 ordinary / 游客只读"的原则：

**`server/enforce/guest.go`**
- 删除 `{"/data_manage/backup_history/*", GET}` —— 路由不存在的僵尸规则
- 新增 `{"/data_manage/disks/*", GET}` —— `/data_manage/disks/:cluster` 只读盘信息
- 新增 `{"/data_manage/tables/*", GET}` —— `/data_manage/tables/:cluster/:db/summary` 只读统计

**`server/enforce/ordinary.go`**
- 新增 `{"/ck/partition/*", PUT}` —— `OperatePartition`，表级 partition 操作
- 新增 `{"/data_manage/backup/*", PUT}` —— 备份策略修改

提交信息建议：

```
fix(enforce): patch missing policies and drop dead backup_history rule

- guest: add /data_manage/disks/* GET, /data_manage/tables/* GET
- guest: remove dead /data_manage/backup_history/* GET
- ordinary: add /ck/partition/* PUT, /data_manage/backup/* PUT
```

## 8. 前端

工作目录：`/data/root/go/src/github.com/housepower/ckman-fe`（参考 CLAUDE.md 的 submodule 流程；不在 `ckman/frontend` 内修改）。

### 8.1 顶栏 / 下拉

`src/views/layout/layout.vue`：

- `fa-briefcase` 安装包入口右侧新增 `fa-users` "用户管理"链接（`router-link to="/users"`），用 `v-if="isAdmin"` 隔离
- 用户下拉（dropdown）新增 "修改密码" 选项，所有已登录用户可见
- mounted 时调 `/user/me` 刷新 `this.$root.userInfo`，写 localStorage，决定 `isAdmin`
- 若 `/user/me` 401 或 `enabled=false` → 跳 `/login`

### 8.2 路由与守卫

`src/services/router.ts`：

```ts
{
  path: 'users',
  name: 'Users',
  component: () => import('@/views/users/users.vue'),
  meta: { requiresAdmin: true },
}
```

全局 `beforeEach` 守卫：`meta.requiresAdmin && localStorage user.policy !== 'admin'` → `next('/home')`。后端 403 是安全边界。

### 8.3 新增文件

```
src/apis/user.ts                              # 7 个端点
src/views/users/users.vue                     # 列表页
src/views/users/modal/createUser.vue          # 新增
src/views/users/modal/editUser.vue            # 改 policy / enabled
src/views/users/modal/resetPassword.vue       # admin 重置他人
src/views/users/modal/changePassword.vue      # 用户改自己
```

### 8.4 列表页

`<el-table>`（用 `element-ui-eoi`，与项目其它表格风格一致）：

| 列 | 备注 |
|---|---|
| 用户名 | 文本 |
| 角色 | 文本，i18n 映射 |
| 启用 | `<el-switch>` 内联切换，点击弹 confirm，确认后调 `PUT /users/:username` |
| 创建时间 | 格式化时间 |
| 操作 | `编辑` `改密` `删除` 三个按钮；`ckman` 行禁用 `编辑` 和 `删除`（tooltip 提示"内置管理员，禁止修改"），仅允许 `改密` |

顶部右侧 `[+ 新增用户]` 按钮。

### 8.5 弹窗

**新增用户**：用户名、角色（`guest`/`ordinary`）、启用、密码、确认密码；前端预校验密码复杂度，提交明文。

**编辑用户**：角色、启用。不含密码。

**重置他人密码**：新密码 + 确认密码。

**改自己密码**：旧密码 + 新密码 + 确认密码。

### 8.6 i18n

`src/assets/locales/{en,zh}.json` 加翻译键：

```
common.users
user.User Management
user.Add User
user.Edit User
user.Reset Password
user.Change Password
user.Username
user.Role
user.Enabled
user.Created At
user.Old Password
user.New Password
user.Confirm Password
user.Policy.admin
user.Policy.guest
user.Policy.ordinary
errors.5032 ~ 5038
```

## 9. 部署与清理

### 9.1 升级感知

- 升级到本版本后旧 `conf/password` 与 `conf/users/*` 仍在磁盘上但不被读取
- 首次启动若 `ckman_users` 表空 → 种子 `ckman / Ckman123456!`
- 启动日志打印 WARN（见 §4.4）

### 9.2 删除项

```
cmd/password/                # 整目录删除
```

主入口若有 `password.PasswordHandle` 调用需同步删除。

### 9.3 配置

`conf/ckman.hjson` 不新增配置项，默认密码硬编码。

### 9.4 Swagger

新增 7 个端点在 controller 上加 `@Summary` `@Tags user` 等注解，`make build` 时 `swag init` 自动重生 `docs/docs.go`。

## 10. 安全考量

- **密码哈希**：登录沿用 `bcrypt(md5(plain))`，依赖 HTTPS 部署 + bcrypt 抗离线破解；用户 CRUD 改密走明文 → `common.VerifyPassword` → `bcrypt(md5(plain))` 写库（保持 hash 格式一致）
- **JWT 绑定 client IP**：现状不变
- **TokenCache 是 per-instance in-memory**：多实例下不共享（现状），本次不改
- **CSRF**：JWT 在 `token` header 内，无 cookie，天然抗 CSRF
- **SQL 注入**：DAO 走 GORM 参数化查询；用户名有正则限制
- **默认密码 `Ckman123456!` 公开**：依赖 WARN 日志提醒与运维责任；未来可改为"启动时生成随机串"

## 11. 已知限制

1. **游客可在 SQL 执行页 `DROP DATABASE`**：网关不解析 SQL；根治需要 ClickHouse 层 ACL，超出 Phase 1 范围
2. **`enabled=false` 不踢已签发 token**：下一次接口调用被 enforce 拒，等同延迟失效
3. **多实例 TokenCache 不共享**：实例 A logout 后实例 B 短时仍认；ckman 历史行为
4. **默认密码 `Ckman123456!` 写死**：见 §10
5. **`ordinary` 是保留用户名**：给 unified portal sentinel 使用，admin 不能创建同名真实用户
6. **历史 `conf/password`、`conf/users/*` 文件**：升级后仍在磁盘但不读取；可手动删除

## 12. 验证清单

实施完毕后必须走完：

- [ ] `make test` 通过
- [ ] `make lint` 通过
- [ ] `make build` 成功
- [ ] sqlite 后端首次启动种子 `ckman`（单测 + 手测）
- [ ] mysql / postgres / dm8 后端 AutoMigrate 不报错（各起一次启动）
- [ ] 用默认密码登录 ckman 成功
- [ ] 改 ckman 密码 → 登出 → 新密码登入
- [ ] 新建 ordinary 用户 → 登录 → 顶栏无"用户管理"
- [ ] 同上 → 手输 `/users` URL → 跳回 `/home`
- [ ] 同上 → 直接 curl `GET /api/v1/users` → 403
- [ ] 删除用户 → 该用户 token 全部失效
- [ ] 禁用用户 → 旧 token 下一次调用 403
- [ ] admin 重置他人密码不需要旧密码
- [ ] ordinary 改自己密码必须输入旧密码
- [ ] 补丁后 ordinary 可调 `PUT /ck/partition/:cluster`
- [ ] 补丁后 guest 可调 `GET /data_manage/disks/:cluster`
- [ ] portal token（`userToken` header）仍走 ordinary 策略

## 13. 实施顺序（粗）

详细分解由 writing-plans 输出。粗粒度顺序：

1. enforce 策略补丁（独立 commit）
2. `model.CkmanUser` + `PersistentUserService` 接口
3. 4 后端 GORM 模型与 DAO
4. 启动种子 + 删除 `common.LoadUsers` 与 `UserMap`
5. 登录改造（含 enabled 校验、LoginRsp 扩展、`E_LOGIN_DISABLED`）
6. enforce 接 DB（含 `orinary` typo 修正）
7. 用户 CRUD controller 与路由
8. 删除 `cmd/password/` 整目录
9. 前端 `apis/user.ts` 与 `views/users/*`
10. layout 顶栏入口 + dropdown 改密 + 路由守卫
11. i18n 翻译 + Swagger 重生成
12. §12 验证清单逐项走通
