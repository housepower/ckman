# User & Role Management Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate ckman user storage from filesystem (`conf/password`, `conf/users/*`) into the persistence layer (sqlite / mysql / postgres / dm8), expose user CRUD via HTTP + Web UI in `ckman-fe`, delete the `cmd/password` CLI, and patch 5 missing/dead `enforce` policy entries along the way.

**Architecture:** A single `ckman_users` table (4 backend mirrors) holds the user records. `policy` is a fixed enum string (`admin|guest|ordinary`) — role-to-API mapping stays hardcoded in `server/enforce/*.go`. Default admin `ckman / Ckman123456!` is seeded on first start. Login still uses md5+bcrypt; user CRUD endpoints take plaintext over HTTPS. `/user/...` self-service is open to any logged-in user; `/users/...` admin paths are admin-only via the existing `enforce` middleware.

**Tech Stack:** Go 1.24 (Gin, GORM, bcrypt, JWT), Vue.js + element-ui-eoi, gorm AutoMigrate per backend.

**Spec:** `docs/superpowers/specs/2026-05-16-user-role-mgmt-phase1-design.md`

**Working directories:**
- Backend: `/data/root/go/src/github.com/housepower/ckman` (this repo, branch `main`)
- Frontend: `/data/root/go/src/github.com/housepower/ckman-fe` (sibling working copy, branch `main`). **Never edit `ckman/frontend` submodule directly.**

---

## File Map

### Backend — modify

- `model/code.go` — append 7 error codes (5033–5039)
- `model/msg.go` — append Chinese / English messages for those codes
- `model/user.go` — extend `LoginRsp`, add `CkmanUser`, add request DTOs
- `repository/persistent.go` — add `PersistentUserService`, embed in `PersistentMgr`, add `seedAdminIfAbsent` in `InitPersistent`
- `repository/sqlite/constant.go` — add `SQLITE_TBL_USER`
- `repository/sqlite/model.go` — add `TblUser` + converters
- `repository/sqlite/sqlite.go` — add 6 DAO methods, append `&TblUser{}` to `AutoMigrate`
- `repository/sqlite/sqlite_test.go` — add `TblUser` DAO tests
- `repository/mysql/constant.go`, `model.go`, `mysql.go` — same pattern as sqlite
- `repository/postgres/constant.go`, `model.go`, `postgres.go` — same pattern
- `repository/dm8/constant.go`, `model.go`, `dm8.go` — same pattern
- `common/user.go` — strip filesystem code, add `DefaultAdminPassword` const
- `controller/controller.go` — add `UsernamePath` constant
- `controller/user.go` — overhaul Login + add 6 new handlers + revoke-tokens helper
- `server/server.go` — delete `common.LoadUsers` call
- `server/enforce/enforce.go` — replace filesystem lookup with `repository.Ps.GetUserByName`, fix `orinary` typo
- `server/enforce/guest.go` — patch policy entries (delete 1, add 4)
- `server/enforce/ordinary.go` — patch policy entries (add 2)
- `router/v1.go` — register 7 new routes with swag annotations
- `cmd/ckmanctl/ckmanctl.go` — remove `password` kingpin command + dispatch case + import

### Backend — delete

- `cmd/password/password.go`
- `cmd/password/` directory

### Frontend — create (in `../ckman-fe`)

- `src/apis/user.ts`
- `src/views/users/users.vue`
- `src/views/users/modal/createUser.vue`
- `src/views/users/modal/editUser.vue`
- `src/views/users/modal/resetPassword.vue`
- `src/views/users/modal/changePassword.vue`

### Frontend — modify (in `../ckman-fe`)

- `src/apis/index.ts` — export user api
- `src/views/login/login.vue` — store `policy`, `enabled` in localStorage from login response
- `src/views/layout/layout.vue` — fetch `/user/me`, conditionally render Users link, add Change Password dropdown item
- `src/services/router.ts` — add `/users` route + `beforeEach` admin guard
- `src/services/i18n.ts` — add new translation keys

---

## Pre-Flight

- [ ] Confirm `git status` is clean except the known `frontend` submodule pointer drift
- [ ] In `../ckman-fe`: `git status && git branch -vv` shows clean tree on `main`
- [ ] `make build` currently passes on a fresh checkout

---

## Task 1: Patch `enforce` Policies (Independent Commit)

**Files:**
- Modify: `server/enforce/guest.go`
- Modify: `server/enforce/ordinary.go`

This task is independent — it stands alone and can ship even if everything else slips. It produces a single commit.

- [ ] **Step 1: Read current guest.go**

Read `server/enforce/guest.go` fully to confirm baseline.

- [ ] **Step 2: Edit `guest.go` — delete dead rule and add 2 read-only entries**

In `server/enforce/guest.go`, locate the `{"/data_manage/backup_history/*", GET},` line and delete it. Add the following two lines, grouped near the existing `/data_manage/backup/*` entry:

```go
{"/data_manage/disks/*", GET},
{"/data_manage/tables/*", GET},
```

- [ ] **Step 3: Edit `ordinary.go` — add 2 table-level write entries**

In `server/enforce/ordinary.go`, inside `OrdinaryPolicies()` before the closing `}` of the `OrdinaryPolicies` slice literal (i.e. before `return append(...)`), add:

```go
{"/ck/partition/*", PUT},
{"/data_manage/backup/*", PUT},
```

- [ ] **Step 4: Build to confirm no syntax errors**

```bash
go build ./...
```

Expected: exit 0, no output.

- [ ] **Step 5: Run existing tests for the enforce package (if any)**

```bash
go test ./server/enforce/... -v
```

Expected: pass (or "no test files" — both are fine).

- [ ] **Step 6: Commit**

```bash
git add server/enforce/guest.go server/enforce/ordinary.go
git commit -m "fix(enforce): patch missing policies and drop dead backup_history rule

- guest: add /data_manage/disks/* GET, /data_manage/tables/* GET
- guest: remove dead /data_manage/backup_history/* GET (no matching route)
- ordinary: add /ck/partition/* PUT, /data_manage/backup/* PUT"
```

---

## Task 2: Add Error Codes & Messages

**Files:**
- Modify: `model/code.go`
- Modify: `model/msg.go`

Note: `E_PASSWORD_VERIFY_FAIL = "5032"` already exists; new codes start at **5033**.

- [ ] **Step 1: Append codes in `model/code.go`**

In `model/code.go`, after the line `E_PASSWORD_VERIFY_FAIL   = "5032"`, add:

```go
	E_LOGIN_DISABLED         = "5033"
	E_USER_ALREADY_EXISTS    = "5034"
	E_INVALID_USERNAME       = "5035"
	E_INVALID_POLICY         = "5036"
	E_FORBIDDEN_TARGET       = "5037"
	E_OLD_PASSWORD_MISMATCH  = "5038"
	E_USER_NOT_FOUND         = "5039"
```

- [ ] **Step 2: Append messages in `model/msg.go`**

In `model/msg.go`, inside the `Messages` map, after the `E_PASSWORD_VERIFY_FAIL` line, add:

```go
	E_LOGIN_DISABLED:        {"E_LOGIN_DISABLED", "账号已禁用"},
	E_USER_ALREADY_EXISTS:   {"E_USER_ALREADY_EXISTS", "用户名已存在"},
	E_INVALID_USERNAME:      {"E_INVALID_USERNAME", "用户名不合法"},
	E_INVALID_POLICY:        {"E_INVALID_POLICY", "角色不合法"},
	E_FORBIDDEN_TARGET:      {"E_FORBIDDEN_TARGET", "保留用户不允许此操作"},
	E_OLD_PASSWORD_MISMATCH: {"E_OLD_PASSWORD_MISMATCH", "旧密码不正确"},
	E_USER_NOT_FOUND:        {"E_USER_NOT_FOUND", "用户不存在"},
```

- [ ] **Step 3: Build**

```bash
go build ./...
```

Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add model/code.go model/msg.go
git commit -m "feat(model): add user-management error codes 5033-5039"
```

---

## Task 3: Add `CkmanUser` Model + DTOs + Extend `LoginRsp`

**Files:**
- Modify: `model/user.go`

- [ ] **Step 1: Replace `model/user.go` with the extended set**

Read current file (12 lines), then write:

```go
package model

type LoginReq struct {
	Username string `json:"username" example:"ckman"`
	Password string `json:"password" example:"63cb91a2ceb9d4f7c8b1ba5e50046f52"`
}

type LoginRsp struct {
	Username string `json:"username" example:"ckman"`
	Token    string `json:"token" example:"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"`
	Policy   string `json:"policy" example:"admin"`
	Enabled  bool   `json:"enabled" example:"true"`
}

// CkmanUser is the persisted user record. PasswordHash is never serialized to JSON.
type CkmanUser struct {
	ID           int64  `json:"-"`
	Username     string `json:"username"`
	PasswordHash string `json:"-"`
	Policy       string `json:"policy"`
	Enabled      bool   `json:"enabled"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type UserListItem struct {
	Username  string `json:"username"`
	Policy    string `json:"policy"`
	Enabled   bool   `json:"enabled"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type CreateUserReq struct {
	Username string `json:"username" example:"alice"`
	Password string `json:"password" example:"PlainPass1!"`
	Policy   string `json:"policy" example:"ordinary"`
	Enabled  bool   `json:"enabled" example:"true"`
}

type UpdateUserReq struct {
	Policy  string `json:"policy" example:"guest"`
	Enabled *bool  `json:"enabled"`
}

type ChangeMyPasswordReq struct {
	OldPassword string `json:"old_password"`
	NewPassword string `json:"new_password"`
}

type ResetPasswordReq struct {
	NewPassword string `json:"new_password"`
}
```

- [ ] **Step 2: Build**

```bash
go build ./...
```

Expected: exit 0.

- [ ] **Step 3: Commit**

```bash
git add model/user.go
git commit -m "feat(model): add CkmanUser, user CRUD DTOs, extend LoginRsp"
```

---

## Task 4: Repository Layer — Interface + Constants + ErrRecordExists Reuse

**Files:**
- Modify: `repository/persistent.go`
- Modify: `repository/sqlite/constant.go`
- Modify: `repository/mysql/constant.go`
- Modify: `repository/postgres/constant.go`
- Modify: `repository/dm8/constant.go`

- [ ] **Step 1: Add the new service interface in `repository/persistent.go`**

After `PersistentBackupRunService` block (before `type PersistentMgr interface`), add:

```go
type PersistentUserService interface {
	GetUserByName(username string) (model.CkmanUser, error)
	UserExists(username string) bool
	GetAllUsers() ([]model.CkmanUser, error)
	CreateUser(u model.CkmanUser) error
	UpdateUser(u model.CkmanUser) error
	DeleteUser(username string) error
}
```

- [ ] **Step 2: Embed in `PersistentMgr`**

In the `PersistentMgr` interface declaration, append after `PersistentBackupRunService` (mind the trailing inline comments):

```go
	PersistentUserService             // 新增 — Phase 1 用户管理
```

- [ ] **Step 3: Add table-name constants per backend**

In each of the four constant files, append a new constant inside the existing `const (...)` block that lists `*_TBL_*` names:

```go
// sqlite/constant.go — append after SQLITE_TBL_BACKUP_RUN
SQLITE_TBL_USER         = "tbl_user"
```

```go
// mysql/constant.go — append in the same block
MYSQL_TBL_USER         = "tbl_user"
```

```go
// postgres/constant.go — append in the same block
POSTGRES_TBL_USER      = "tbl_user"
```

```go
// dm8/constant.go — append in the same block
DM8_TBL_USER           = "tbl_user"
```

If a backend's constant file does not yet declare a block of table names (some only declare the policy name), inspect the file with Read first and add a new `const (...)` block that mirrors the sqlite pattern.

- [ ] **Step 4: Build**

```bash
go build ./...
```

Expected: build will fail because none of the 4 backends implements the new interface yet. **This is intentional** — record the failing output (the four missing methods × four backends = 24 compile errors) to confirm the interface contract is wired up. Do not commit yet.

- [ ] **Step 5: Verify the build failure mentions all four backends**

```bash
go build ./... 2>&1 | grep -E "(sqlite|mysql|postgres|dm8).*does not implement"
```

Expected: at least 4 lines, one per backend.

Continue to Task 5 to satisfy the contract before committing.

---

## Task 5: SQLite Backend — Model + DAO + Tests

**Files:**
- Modify: `repository/sqlite/model.go`
- Modify: `repository/sqlite/sqlite.go`
- Modify: `repository/sqlite/sqlite_test.go`

- [ ] **Step 1: Add `TblUser` and converters in `sqlite/model.go`**

Append to `repository/sqlite/model.go` (after `TblMeta`):

```go
type TblUser struct {
	gorm.Model
	Username     string `gorm:"index:idx_user_name,unique; column:username; size:32; not null"`
	PasswordHash string `gorm:"column:password_hash; size:64; not null"`
	Policy       string `gorm:"column:policy; size:16; not null"`
	Enabled      bool   `gorm:"column:enabled; not null; default:true"`
}

func (TblUser) TableName() string { return SQLITE_TBL_USER }
```

Add to the same file (still in `package sqlite`) helper converters — placed at the bottom:

```go
func tblUserToModel(t TblUser) model.CkmanUser {
	return model.CkmanUser{
		ID:           int64(t.ID),
		Username:     t.Username,
		PasswordHash: t.PasswordHash,
		Policy:       t.Policy,
		Enabled:      t.Enabled,
		CreatedAt:    t.CreatedAt.Unix(),
		UpdatedAt:    t.UpdatedAt.Unix(),
	}
}
```

Add the import for `"github.com/housepower/ckman/model"` to `sqlite/model.go` if it is not already present.

- [ ] **Step 2: Register `&TblUser{}` in `AutoMigrate`**

In `repository/sqlite/sqlite.go` `Init()` function, locate the `db.AutoMigrate(...)` call and append `&TblUser{}` to the argument list (place it after `&TblMeta{}`):

```go
if err := db.AutoMigrate(
    &TblCluster{},
    &TblLogic{},
    &TblQueryHistory{},
    &TblTask{},
    &TblBackup{},
    &TblBackupPolicy{},
    &TblBackupRun{},
    &TblMeta{},
    &TblUser{},
); err != nil {
    return errors.Wrap(err, "")
}
```

- [ ] **Step 3: Append 6 DAO methods to `sqlite/sqlite.go`**

At the end of the file, add a new section:

```go
// ─── User ─────────────────────────────────────────────────────────────────────

func (sp *SQLitePersistent) UserExists(username string) bool {
	_, err := sp.GetUserByName(username)
	return err == nil
}

func (sp *SQLitePersistent) GetUserByName(username string) (model.CkmanUser, error) {
	var tbl TblUser
	if err := sp.Client.Where("username = ?", username).First(&tbl).Error; err != nil {
		return model.CkmanUser{}, wrapError(err)
	}
	return tblUserToModel(tbl), nil
}

func (sp *SQLitePersistent) GetAllUsers() ([]model.CkmanUser, error) {
	var tbls []TblUser
	if err := sp.Client.Order("username ASC").Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make([]model.CkmanUser, 0, len(tbls))
	for _, t := range tbls {
		out = append(out, tblUserToModel(t))
	}
	return out, nil
}

func (sp *SQLitePersistent) CreateUser(u model.CkmanUser) error {
	tbl := TblUser{
		Username:     u.Username,
		PasswordHash: u.PasswordHash,
		Policy:       u.Policy,
		Enabled:      u.Enabled,
	}
	if err := sp.Client.Create(&tbl).Error; err != nil {
		// sqlite UNIQUE violation surfaces as a plain error string; treat as exists
		if isUniqueViolation(err) {
			return repository.ErrRecordExists
		}
		return wrapError(err)
	}
	return nil
}

func (sp *SQLitePersistent) UpdateUser(u model.CkmanUser) error {
	updates := map[string]interface{}{
		"password_hash": u.PasswordHash,
		"policy":        u.Policy,
		"enabled":       u.Enabled,
	}
	tx := sp.Client.Model(&TblUser{}).Where("username = ?", u.Username).Updates(updates)
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (sp *SQLitePersistent) DeleteUser(username string) error {
	tx := sp.Client.Where("username = ?", username).Delete(&TblUser{})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
```

Add `"strings"` to the imports at the top of `sqlite/sqlite.go` if absent.

The other 3 backends will re-implement this helper with their own dialect check (DRY violation acknowledged — each backend file is self-contained per project convention).

- [ ] **Step 4: Add DAO tests in `sqlite/sqlite_test.go`**

Read existing test file to confirm helper patterns (`newTempSqlite()` or similar). Append a new test function — adapt the helper name to whatever the file already exposes:

```go
func TestUserCRUD(t *testing.T) {
	sp := newTestSQLite(t)

	user := model.CkmanUser{
		Username:     "alice",
		PasswordHash: "hashvalue",
		Policy:       "ordinary",
		Enabled:      true,
	}
	require.NoError(t, sp.CreateUser(user))
	require.True(t, sp.UserExists("alice"))

	got, err := sp.GetUserByName("alice")
	require.NoError(t, err)
	require.Equal(t, "alice", got.Username)
	require.Equal(t, "ordinary", got.Policy)
	require.True(t, got.Enabled)

	// Duplicate insert → ErrRecordExists
	err = sp.CreateUser(user)
	require.ErrorIs(t, err, repository.ErrRecordExists)

	// Update
	user.Policy = "guest"
	user.Enabled = false
	user.PasswordHash = "newhash"
	require.NoError(t, sp.UpdateUser(user))
	got, _ = sp.GetUserByName("alice")
	require.Equal(t, "guest", got.Policy)
	require.False(t, got.Enabled)
	require.Equal(t, "newhash", got.PasswordHash)

	// GetAll
	all, err := sp.GetAllUsers()
	require.NoError(t, err)
	require.Len(t, all, 1)

	// Delete
	require.NoError(t, sp.DeleteUser("alice"))
	require.False(t, sp.UserExists("alice"))
	_, err = sp.GetUserByName("alice")
	require.ErrorIs(t, err, repository.ErrRecordNotFound)

	// Delete missing → ErrRecordNotFound
	err = sp.DeleteUser("ghost")
	require.ErrorIs(t, err, repository.ErrRecordNotFound)
}
```

If the existing test file uses a helper with a different name (e.g. `setupSQLite(t)`), use that. Match the existing test style exactly.

- [ ] **Step 5: Run sqlite tests**

```bash
go test ./repository/sqlite/... -run TestUserCRUD -v
```

Expected: PASS.

- [ ] **Step 6: Build the whole project**

```bash
go build ./...
```

Expected: 3 backends (mysql, postgres, dm8) still fail to compile (missing interface methods). That is OK — they are next.

- [ ] **Step 7: Stage but do not commit yet — defer to Task 8 (combined backend commit)**

Move on to Task 6.

---

## Task 6: MySQL Backend — Model + DAO

**Files:**
- Modify: `repository/mysql/model.go`
- Modify: `repository/mysql/mysql.go`

- [ ] **Step 1: Mirror `TblUser` in `mysql/model.go`**

```go
type TblUser struct {
	gorm.Model
	Username     string `gorm:"index:idx_user_name,unique; column:username; size:32; not null"`
	PasswordHash string `gorm:"column:password_hash; size:64; not null"`
	Policy       string `gorm:"column:policy; size:16; not null"`
	Enabled      bool   `gorm:"column:enabled; not null; default:true"`
}

func (TblUser) TableName() string { return MYSQL_TBL_USER }

func tblUserToModel(t TblUser) model.CkmanUser {
	return model.CkmanUser{
		ID:           int64(t.ID),
		Username:     t.Username,
		PasswordHash: t.PasswordHash,
		Policy:       t.Policy,
		Enabled:      t.Enabled,
		CreatedAt:    t.CreatedAt.Unix(),
		UpdatedAt:    t.UpdatedAt.Unix(),
	}
}
```

Add `"github.com/housepower/ckman/model"` to imports if absent.

- [ ] **Step 2: Register in AutoMigrate**

In `mysql/mysql.go` `Init()`, append `&TblUser{}` to the existing `db.AutoMigrate(...)` call (after the last entry).

- [ ] **Step 3: Add 6 DAO methods + `isUniqueViolation`**

Append at end of `mysql/mysql.go`:

```go
// ─── User ─────────────────────────────────────────────────────────────────────

func (mp *MySQLPersistent) UserExists(username string) bool {
	_, err := mp.GetUserByName(username)
	return err == nil
}

func (mp *MySQLPersistent) GetUserByName(username string) (model.CkmanUser, error) {
	var tbl TblUser
	if err := mp.Client.Where("username = ?", username).First(&tbl).Error; err != nil {
		return model.CkmanUser{}, wrapError(err)
	}
	return tblUserToModel(tbl), nil
}

func (mp *MySQLPersistent) GetAllUsers() ([]model.CkmanUser, error) {
	var tbls []TblUser
	if err := mp.Client.Order("username ASC").Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make([]model.CkmanUser, 0, len(tbls))
	for _, t := range tbls {
		out = append(out, tblUserToModel(t))
	}
	return out, nil
}

func (mp *MySQLPersistent) CreateUser(u model.CkmanUser) error {
	tbl := TblUser{
		Username:     u.Username,
		PasswordHash: u.PasswordHash,
		Policy:       u.Policy,
		Enabled:      u.Enabled,
	}
	if err := mp.Client.Create(&tbl).Error; err != nil {
		if isUniqueViolationMySQL(err) {
			return repository.ErrRecordExists
		}
		return wrapError(err)
	}
	return nil
}

func (mp *MySQLPersistent) UpdateUser(u model.CkmanUser) error {
	updates := map[string]interface{}{
		"password_hash": u.PasswordHash,
		"policy":        u.Policy,
		"enabled":       u.Enabled,
	}
	tx := mp.Client.Model(&TblUser{}).Where("username = ?", u.Username).Updates(updates)
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (mp *MySQLPersistent) DeleteUser(username string) error {
	tx := mp.Client.Where("username = ?", username).Delete(&TblUser{})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func isUniqueViolationMySQL(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Duplicate entry")
}
```

Confirm `MySQLPersistent` is the correct struct name (read the file's existing `func (mp *MySQLPersistent) ...` to verify the receiver name). If the receiver is different (e.g. `mysqlPersistent`), adjust accordingly.

Add `"strings"` to imports if absent.

- [ ] **Step 4: Build**

```bash
go build ./repository/mysql/...
```

Expected: exit 0.

- [ ] **Step 5: Defer commit to Task 8.**

---

## Task 7: PostgreSQL Backend — Model + DAO

**Files:**
- Modify: `repository/postgres/model.go`
- Modify: `repository/postgres/postgres.go`

- [ ] **Step 1: Mirror `TblUser` in `postgres/model.go`**

```go
type TblUser struct {
	gorm.Model
	Username     string `gorm:"index:idx_user_name,unique; column:username; size:32; not null"`
	PasswordHash string `gorm:"column:password_hash; size:64; not null"`
	Policy       string `gorm:"column:policy; size:16; not null"`
	Enabled      bool   `gorm:"column:enabled; not null; default:true"`
}

func (TblUser) TableName() string { return POSTGRES_TBL_USER }

func tblUserToModel(t TblUser) model.CkmanUser {
	return model.CkmanUser{
		ID:           int64(t.ID),
		Username:     t.Username,
		PasswordHash: t.PasswordHash,
		Policy:       t.Policy,
		Enabled:      t.Enabled,
		CreatedAt:    t.CreatedAt.Unix(),
		UpdatedAt:    t.UpdatedAt.Unix(),
	}
}
```

Add `"github.com/housepower/ckman/model"` to imports if absent.

- [ ] **Step 2: Register `&TblUser{}` in `AutoMigrate`** in `postgres/postgres.go` `Init()`.

- [ ] **Step 3: Add 6 DAO methods**

Receiver name: verify via `grep "^func (.* )" repository/postgres/postgres.go | head -2`. Below assumes `pp *PGPersistent` — adjust if the actual receiver is different.

Append to `postgres/postgres.go`:

```go
// ─── User ─────────────────────────────────────────────────────────────────────

func (pp *PGPersistent) UserExists(username string) bool {
	_, err := pp.GetUserByName(username)
	return err == nil
}

func (pp *PGPersistent) GetUserByName(username string) (model.CkmanUser, error) {
	var tbl TblUser
	if err := pp.Client.Where("username = ?", username).First(&tbl).Error; err != nil {
		return model.CkmanUser{}, wrapError(err)
	}
	return tblUserToModel(tbl), nil
}

func (pp *PGPersistent) GetAllUsers() ([]model.CkmanUser, error) {
	var tbls []TblUser
	if err := pp.Client.Order("username ASC").Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make([]model.CkmanUser, 0, len(tbls))
	for _, t := range tbls {
		out = append(out, tblUserToModel(t))
	}
	return out, nil
}

func (pp *PGPersistent) CreateUser(u model.CkmanUser) error {
	tbl := TblUser{
		Username:     u.Username,
		PasswordHash: u.PasswordHash,
		Policy:       u.Policy,
		Enabled:      u.Enabled,
	}
	if err := pp.Client.Create(&tbl).Error; err != nil {
		if isUniqueViolationPostgres(err) {
			return repository.ErrRecordExists
		}
		return wrapError(err)
	}
	return nil
}

func (pp *PGPersistent) UpdateUser(u model.CkmanUser) error {
	updates := map[string]interface{}{
		"password_hash": u.PasswordHash,
		"policy":        u.Policy,
		"enabled":       u.Enabled,
	}
	tx := pp.Client.Model(&TblUser{}).Where("username = ?", u.Username).Updates(updates)
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (pp *PGPersistent) DeleteUser(username string) error {
	tx := pp.Client.Where("username = ?", username).Delete(&TblUser{})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func isUniqueViolationPostgres(err error) bool {
	return err != nil && strings.Contains(err.Error(), "duplicate key value")
}
```

Add `"strings"` to imports if absent.

- [ ] **Step 4: Build**

```bash
go build ./repository/postgres/...
```

Expected: exit 0.

- [ ] **Step 5: Defer commit to Task 8.**

---

## Task 8: DM8 Backend — Model + DAO + Combined Commit

**Files:**
- Modify: `repository/dm8/model.go`
- Modify: `repository/dm8/dm8.go`

- [ ] **Step 1: Mirror `TblUser` in `dm8/model.go`**

```go
type TblUser struct {
	gorm.Model
	Username     string `gorm:"index:idx_user_name,unique; column:username; size:32; not null"`
	PasswordHash string `gorm:"column:password_hash; size:64; not null"`
	Policy       string `gorm:"column:policy; size:16; not null"`
	Enabled      bool   `gorm:"column:enabled; not null; default:true"`
}

func (TblUser) TableName() string { return DM8_TBL_USER }

func tblUserToModel(t TblUser) model.CkmanUser {
	return model.CkmanUser{
		ID:           int64(t.ID),
		Username:     t.Username,
		PasswordHash: t.PasswordHash,
		Policy:       t.Policy,
		Enabled:      t.Enabled,
		CreatedAt:    t.CreatedAt.Unix(),
		UpdatedAt:    t.UpdatedAt.Unix(),
	}
}
```

Add `"github.com/housepower/ckman/model"` to imports if absent.

- [ ] **Step 2: Register `&TblUser{}` in `AutoMigrate`** in `dm8/dm8.go` `Init()`.

- [ ] **Step 3: Add 6 DAO methods**

Receiver name: verify via `grep "^func (.* )" repository/dm8/dm8.go | head -2`. Below assumes `dp *DM8Persistent` — adjust if needed.

```go
// ─── User ─────────────────────────────────────────────────────────────────────

func (dp *DM8Persistent) UserExists(username string) bool {
	_, err := dp.GetUserByName(username)
	return err == nil
}

func (dp *DM8Persistent) GetUserByName(username string) (model.CkmanUser, error) {
	var tbl TblUser
	if err := dp.Client.Where("username = ?", username).First(&tbl).Error; err != nil {
		return model.CkmanUser{}, wrapError(err)
	}
	return tblUserToModel(tbl), nil
}

func (dp *DM8Persistent) GetAllUsers() ([]model.CkmanUser, error) {
	var tbls []TblUser
	if err := dp.Client.Order("username ASC").Find(&tbls).Error; err != nil {
		return nil, wrapError(err)
	}
	out := make([]model.CkmanUser, 0, len(tbls))
	for _, t := range tbls {
		out = append(out, tblUserToModel(t))
	}
	return out, nil
}

func (dp *DM8Persistent) CreateUser(u model.CkmanUser) error {
	tbl := TblUser{
		Username:     u.Username,
		PasswordHash: u.PasswordHash,
		Policy:       u.Policy,
		Enabled:      u.Enabled,
	}
	if err := dp.Client.Create(&tbl).Error; err != nil {
		if isUniqueViolationDM8(err) {
			return repository.ErrRecordExists
		}
		return wrapError(err)
	}
	return nil
}

func (dp *DM8Persistent) UpdateUser(u model.CkmanUser) error {
	updates := map[string]interface{}{
		"password_hash": u.PasswordHash,
		"policy":        u.Policy,
		"enabled":       u.Enabled,
	}
	tx := dp.Client.Model(&TblUser{}).Where("username = ?", u.Username).Updates(updates)
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func (dp *DM8Persistent) DeleteUser(username string) error {
	tx := dp.Client.Where("username = ?", username).Delete(&TblUser{})
	if tx.Error != nil {
		return wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return repository.ErrRecordNotFound
	}
	return nil
}

func isUniqueViolationDM8(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "ORA-00001") ||
		strings.Contains(msg, "唯一性约束") ||
		strings.Contains(msg, "unique constraint")
}
```

Add `"strings"` to imports if absent.

- [ ] **Step 4: Build the whole project**

```bash
go build ./...
```

Expected: exit 0 — all 4 backends now satisfy `PersistentMgr`.

- [ ] **Step 5: Run sqlite test once more to ensure no regression**

```bash
go test ./repository/sqlite/... -v
```

Expected: pass.

- [ ] **Step 6: Commit all backend work as one unit**

```bash
git add repository/persistent.go \
        repository/sqlite/ \
        repository/mysql/ \
        repository/postgres/ \
        repository/dm8/
git commit -m "feat(repository): add PersistentUserService + TblUser across 4 backends

- new interface PersistentUserService embedded in PersistentMgr
- TblUser GORM model + AutoMigrate + 6 DAO methods per backend
- unique-violation normalized to repository.ErrRecordExists per dialect
- sqlite unit tests cover full CRUD + duplicate + not-found paths"
```

---

## Task 9: Seed Default Admin On First Start

**Files:**
- Modify: `common/user.go` — add `DefaultAdminPassword` const (do NOT yet remove `LoadUsers` — that happens in Task 11)
- Modify: `repository/persistent.go`

- [ ] **Step 1: Add `DefaultAdminPassword` constant**

In `common/user.go`, inside the existing `const ( ... )` block (lines 11-18), add at the end:

```go
DefaultAdminPassword = "Ckman123456!"
```

- [ ] **Step 2: Add `seedAdminIfAbsent` to `repository/persistent.go`**

At the bottom of `repository/persistent.go`, add:

```go
func seedAdminIfAbsent() error {
	if Ps.UserExists(common.DefaultAdminName) {
		return nil
	}
	md5pw := common.Md5CheckSum(common.DefaultAdminPassword)
	hash, err := common.HashPassword(md5pw)
	if err != nil {
		return errors.Wrap(err, "hash default admin password")
	}
	now := time.Now().Unix()
	err = Ps.CreateUser(model.CkmanUser{
		Username:     common.DefaultAdminName,
		PasswordHash: hash,
		Policy:       common.ADMIN,
		Enabled:      true,
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err != nil {
		if errors.Is(err, ErrRecordExists) {
			return nil // multi-instance race: another instance won, treat as success
		}
		return err
	}
	log.Logger.Warnf("bootstrap: seeded admin user %q with default password %q.",
		common.DefaultAdminName, common.DefaultAdminPassword)
	log.Logger.Warnf("bootstrap: please log in via Web UI and change the password immediately.")
	return nil
}
```

Add to the imports at the top of `repository/persistent.go`:

```go
"time"
"github.com/housepower/ckman/common"
"github.com/housepower/ckman/log"
```

- [ ] **Step 3: Call `seedAdminIfAbsent` from `InitPersistent`**

In `repository/persistent.go`, at the end of `InitPersistent()`, replace:

```go
if err := Ps.Init(pcfg); err != nil {
    return err
}
return nil
```

with:

```go
if err := Ps.Init(pcfg); err != nil {
    return err
}
return seedAdminIfAbsent()
```

- [ ] **Step 4: Build**

```bash
go build ./...
```

Expected: exit 0.

- [ ] **Step 5: Write a quick sanity test for the seed in `repository/sqlite/sqlite_test.go`**

Append:

```go
func TestSeedAdminFromInitPersistent(t *testing.T) {
	// Stage a temp config so InitPersistent picks SQLite.
	dir := t.TempDir()
	config.GlobalConfig.Server.PersistentPolicy = sqlite.SQLitePersistentName
	config.GlobalConfig.PersistentConfig = map[string]map[string]interface{}{
		sqlite.SQLitePersistentName: {"config_dir": dir, "config_file": "test.db"},
	}
	repository.Ps = nil
	require.NoError(t, repository.InitPersistent())

	got, err := repository.Ps.GetUserByName(common.DefaultAdminName)
	require.NoError(t, err)
	require.Equal(t, common.ADMIN, got.Policy)
	require.True(t, got.Enabled)

	// Idempotent: re-initializing must not duplicate or fail.
	repository.Ps = nil
	require.NoError(t, repository.InitPersistent())
	all, err := repository.Ps.GetAllUsers()
	require.NoError(t, err)
	require.Len(t, all, 1)
}
```

If the test file already establishes its own `config.GlobalConfig` setup helper, use that helper instead. The imports likely needed: `config`, `repository`, `repository/sqlite`, `common`.

- [ ] **Step 6: Run the test**

```bash
go test ./repository/sqlite/... -run TestSeedAdminFromInitPersistent -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add common/user.go repository/persistent.go repository/sqlite/sqlite_test.go
git commit -m "feat(repository): seed default admin on first start

- DefaultAdminPassword constant added to common
- seedAdminIfAbsent invoked at end of InitPersistent
- multi-instance race handled by treating UNIQUE violation as success
- WARN logs on actual seed event"
```

---

## Task 10: Rework Login to Use DB + Enabled Check + Extended LoginRsp

**Files:**
- Modify: `controller/user.go`

- [ ] **Step 1: Replace `Login` handler body**

In `controller/user.go`, replace the entire `Login` function (current lines ~40–81) with:

```go
func (controller *UserController) Login(c *gin.Context) {
	var req model.LoginReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	user, err := repository.Ps.GetUserByName(req.Username)
	if err != nil {
		controller.wrapfunc(c, model.E_USER_VERIFY_FAIL, err)
		return
	}
	if !user.Enabled {
		controller.wrapfunc(c, model.E_LOGIN_DISABLED, nil)
		return
	}
	if pass := common.ComparePassword(user.PasswordHash, req.Password); !pass {
		controller.wrapfunc(c, model.E_PASSWORD_VERIFY_FAIL, nil)
		return
	}

	j := common.NewJWT()
	claims := common.CustomClaims{
		StandardClaims: jwt.StandardClaims{
			IssuedAt: time.Now().Unix(),
		},
		Name:     req.Username,
		ClientIP: c.ClientIP(),
	}
	token, err := j.CreateToken(claims)
	if err != nil {
		controller.wrapfunc(c, model.E_CREAT_TOKEN_FAIL, err)
		return
	}

	rsp := model.LoginRsp{
		Username: user.Username,
		Token:    token,
		Policy:   user.Policy,
		Enabled:  user.Enabled,
	}
	TokenCache.SetDefault(token,
		time.Now().Add(time.Second*time.Duration(controller.config.Server.SessionTimeout)).Unix())

	controller.wrapfunc(c, model.E_SUCCESS, rsp)
}
```

- [ ] **Step 2: Remove the `path/filepath` import** if it is now unused

Check the file's import block; if `path/filepath` is no longer referenced anywhere in `controller/user.go` after removing the `common.LoadUsers(filepath.Dir(...))` line in the previous block, delete it.

Add `"github.com/housepower/ckman/repository"` to the imports.

- [ ] **Step 3: Build**

```bash
go build ./controller/...
```

Expected: exit 0.

- [ ] **Step 4: Commit**

```bash
git add controller/user.go
git commit -m "feat(controller): login reads user from DB, returns policy/enabled, blocks disabled accounts"
```

---

## Task 11: Strip Filesystem User Code + Wire enforce to DB

**Files:**
- Modify: `common/user.go`
- Modify: `server/server.go`
- Modify: `server/enforce/enforce.go`

This task removes the filesystem-based user loading entirely. After it, `make build` must still pass and login must still work because Tasks 9 + 10 have seeded the DB and redirected login.

> **DEPENDENCY:** `cmd/password/password.go` still imports `common.UserMap`, `common.UserInfo`, `common.LoadUsers`, `common.GetUserInfo`. You **MUST** complete Task 14 (delete `cmd/password` directory + clean up `ckmanctl`) **before** running `go build` in Step 4 below — otherwise the build will fail with "undefined: common.LoadUsers" et al. If you are executing tasks in sequence, jump to Task 14 first, then return here.

- [ ] **Step 1: Rewrite `common/user.go` to constants only**

Replace the entire file content with:

```go
package common

const (
	ADMIN    string = "admin"
	GUEST    string = "guest"
	ORDINARY string = "ordinary"

	DefaultAdminName     = "ckman"
	InternalOrdinaryName = "ordinary"
	DefaultAdminPassword = "Ckman123456!"
)
```

All of `UserInfo`, `UserMap`, `lock`, `LoadUsers`, `GetUserInfo` are deleted.

- [ ] **Step 2: Delete `LoadUsers` call in `server/server.go`**

In `server/server.go`, line ~118, delete the line:

```go
common.LoadUsers(filepath.Dir(server.config.ConfigFile))
```

If `path/filepath` is no longer used anywhere else in the file, remove its import too.

- [ ] **Step 3: Rewrite `enforce.Enforce` to query DB, fix typo**

In `server/enforce/enforce.go`:

a. Rename `orinary` → `ordinary` everywhere in the file (struct field and assignments):

```go
type Enforcer struct {
    model    Model
    guest    []Policy
    ordinary []Policy
}
```

```go
e = &Enforcer{
    model:    DefaultModel,
    guest:    GuestPolicies(),
    ordinary: OrdinaryPolicies(),
}
```

b. Replace `Enforce` body:

```go
func Enforce(username, url, method string) bool {
	if username == common.DefaultAdminName {
		return true
	}

	var policies []Policy
	if username == common.InternalOrdinaryName {
		policies = e.ordinary
	} else {
		user, err := repository.Ps.GetUserByName(username)
		if err != nil || !user.Enabled {
			return false
		}
		switch user.Policy {
		case common.GUEST:
			policies = e.guest
		case common.ORDINARY:
			policies = e.ordinary
		case common.ADMIN:
			return true
		default:
			return false
		}
	}
	for _, policy := range policies {
		if e.Match(policy.URL, url) && policy.Method == method {
			return true
		}
	}
	return false
}
```

Add import `"github.com/housepower/ckman/repository"`.

- [ ] **Step 4: Build**

```bash
go build ./...
```

Expected: exit 0. If a stale reference to `common.LoadUsers` or `common.GetUserInfo` is reported, locate and remove it (also check `cmd/ckmanctl` — Task 14 covers `cmd/password` deletion, but if any other consumer surfaces here, fix it).

- [ ] **Step 5: Run all tests so nothing regressed**

```bash
go test ./... 2>&1 | tail -30
```

Expected: all pass. (Some packages without tests print "no test files" — fine.)

- [ ] **Step 6: Commit**

```bash
git add common/user.go server/server.go server/enforce/enforce.go
git commit -m "refactor(auth): retire filesystem user store, route auth through DB

- common/user.go reduced to role + sentinel constants
- removed UserInfo, UserMap, lock, LoadUsers, GetUserInfo
- enforce.Enforce now queries repository.Ps and honours enabled=false
- fixed long-standing typo: orinary -> ordinary"
```

---

## Task 12: User CRUD Controller Handlers

**Files:**
- Modify: `controller/controller.go`
- Modify: `controller/user.go`

- [ ] **Step 1: Add `UsernamePath` constant in `controller/user.go`**

`ClickHouseClusterPath` lives in `controller/clickhouse.go` and `TaskIdPath` in `controller/task.go`. Follow that convention: declare `UsernamePath` near the top of `controller/user.go` (after the imports, before `var TokenCache`):

```go
const UsernamePath = "username"
```

- [ ] **Step 2: Add username regex helper + reserved-name guard in `controller/user.go`**

Add near the top of `controller/user.go`:

```go
var (
	usernameRegex = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{2,31}$`)
	reservedNames = map[string]struct{}{
		common.DefaultAdminName:     {},
		common.InternalOrdinaryName: {},
	}
)

func validUsername(name string) bool {
	return usernameRegex.MatchString(name)
}

func isReservedName(name string) bool {
	_, ok := reservedNames[name]
	return ok
}

func validPolicyForCreate(p string) bool {
	return p == common.GUEST || p == common.ORDINARY
}
```

Add imports: `"regexp"`.

- [ ] **Step 3: Add `Me` handler**

```go
// @Summary 获取当前登录用户信息
// @Description Get the current logged-in user
// @Tags user
// @Success 200 {object} model.UserListItem
// @Router /api/v1/user/me [get]
func (controller *UserController) Me(c *gin.Context) {
	username := c.GetString("username")
	user, err := repository.Ps.GetUserByName(username)
	if err != nil {
		if username == common.InternalOrdinaryName {
			// portal sentinel — synthesise response (not in DB)
			controller.wrapfunc(c, model.E_SUCCESS, model.UserListItem{
				Username: username,
				Policy:   common.ORDINARY,
				Enabled:  true,
			})
			return
		}
		controller.wrapfunc(c, model.E_USER_NOT_FOUND, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, model.UserListItem{
		Username:  user.Username,
		Policy:    user.Policy,
		Enabled:   user.Enabled,
		CreatedAt: user.CreatedAt,
		UpdatedAt: user.UpdatedAt,
	})
}
```

- [ ] **Step 4: Add `List` handler**

```go
// @Summary 获取所有用户
// @Description List all users (admin only)
// @Tags user
// @Success 200 {array} model.UserListItem
// @Router /api/v1/users [get]
func (controller *UserController) List(c *gin.Context) {
	users, err := repository.Ps.GetAllUsers()
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	out := make([]model.UserListItem, 0, len(users))
	for _, u := range users {
		out = append(out, model.UserListItem{
			Username:  u.Username,
			Policy:    u.Policy,
			Enabled:   u.Enabled,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		})
	}
	controller.wrapfunc(c, model.E_SUCCESS, out)
}
```

- [ ] **Step 5: Add `Create` handler**

```go
// @Summary 创建用户
// @Description Create a non-admin user (admin only)
// @Tags user
// @Param req body model.CreateUserReq true "request body"
// @Success 200
// @Router /api/v1/users [post]
func (controller *UserController) Create(c *gin.Context) {
	var req model.CreateUserReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	if !validUsername(req.Username) {
		controller.wrapfunc(c, model.E_INVALID_USERNAME, nil)
		return
	}
	if isReservedName(req.Username) {
		controller.wrapfunc(c, model.E_FORBIDDEN_TARGET, nil)
		return
	}
	if !validPolicyForCreate(req.Policy) {
		controller.wrapfunc(c, model.E_INVALID_POLICY, nil)
		return
	}
	if err := common.VerifyPassword(req.Password); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	md5pw := common.Md5CheckSum(req.Password)
	hash, err := common.HashPassword(md5pw)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	now := time.Now().Unix()
	user := model.CkmanUser{
		Username:     req.Username,
		PasswordHash: hash,
		Policy:       req.Policy,
		Enabled:      req.Enabled,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := repository.Ps.CreateUser(user); err != nil {
		if errors.Is(err, repository.ErrRecordExists) {
			controller.wrapfunc(c, model.E_USER_ALREADY_EXISTS, nil)
			return
		}
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}
```

Add imports: `"github.com/pkg/errors"`, `"github.com/housepower/ckman/repository"`.

- [ ] **Step 6: Add `Update` handler**

```go
// @Summary 更新用户角色或启用状态
// @Description Update policy and/or enabled flag (admin only)
// @Tags user
// @Param username path string true "username"
// @Param req body model.UpdateUserReq true "request body"
// @Success 200
// @Router /api/v1/users/{username} [put]
func (controller *UserController) Update(c *gin.Context) {
	username := c.Param(UsernamePath)
	if isReservedName(username) {
		controller.wrapfunc(c, model.E_FORBIDDEN_TARGET, nil)
		return
	}
	var req model.UpdateUserReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	user, err := repository.Ps.GetUserByName(username)
	if err != nil {
		controller.wrapfunc(c, model.E_USER_NOT_FOUND, err)
		return
	}
	if req.Policy != "" {
		if !validPolicyForCreate(req.Policy) {
			controller.wrapfunc(c, model.E_INVALID_POLICY, nil)
			return
		}
		user.Policy = req.Policy
	}
	if req.Enabled != nil {
		user.Enabled = *req.Enabled
	}
	user.UpdatedAt = time.Now().Unix()
	if err := repository.Ps.UpdateUser(user); err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}
```

- [ ] **Step 7: Add `Delete` + token revoke helper**

```go
// @Summary 删除用户
// @Description Hard-delete a user and revoke their tokens (admin only)
// @Tags user
// @Param username path string true "username"
// @Success 200
// @Router /api/v1/users/{username} [delete]
func (controller *UserController) Delete(c *gin.Context) {
	username := c.Param(UsernamePath)
	if isReservedName(username) {
		controller.wrapfunc(c, model.E_FORBIDDEN_TARGET, nil)
		return
	}
	if err := repository.Ps.DeleteUser(username); err != nil {
		if errors.Is(err, repository.ErrRecordNotFound) {
			controller.wrapfunc(c, model.E_USER_NOT_FOUND, err)
			return
		}
		controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, err)
		return
	}
	revokeTokensFor(username)
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// revokeTokensFor walks the in-memory TokenCache and removes entries whose JWT
// claims.Name equals username. Used after deleting a user account.
func revokeTokensFor(username string) {
	for token := range TokenCache.Items() {
		j := common.NewJWT()
		claims, code := j.ParserToken(token)
		if code != model.E_SUCCESS {
			continue
		}
		if claims.Name == username {
			TokenCache.Delete(token)
		}
	}
}
```

- [ ] **Step 8: Add `ChangeMyPassword` handler**

```go
// @Summary 修改自己的密码
// @Description Change current user's own password (requires old password)
// @Tags user
// @Param req body model.ChangeMyPasswordReq true "request body"
// @Success 200
// @Router /api/v1/user/password [put]
func (controller *UserController) ChangeMyPassword(c *gin.Context) {
	username := c.GetString("username")
	if username == common.InternalOrdinaryName {
		// portal sentinel has no DB record; password change is meaningless
		controller.wrapfunc(c, model.E_FORBIDDEN_TARGET, nil)
		return
	}
	var req model.ChangeMyPasswordReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	user, err := repository.Ps.GetUserByName(username)
	if err != nil {
		controller.wrapfunc(c, model.E_USER_NOT_FOUND, err)
		return
	}
	if !common.ComparePassword(user.PasswordHash, common.Md5CheckSum(req.OldPassword)) {
		controller.wrapfunc(c, model.E_OLD_PASSWORD_MISMATCH, nil)
		return
	}
	if err := common.VerifyPassword(req.NewPassword); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}
	hash, err := common.HashPassword(common.Md5CheckSum(req.NewPassword))
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	user.PasswordHash = hash
	user.UpdatedAt = time.Now().Unix()
	if err := repository.Ps.UpdateUser(user); err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}
```

- [ ] **Step 9: Add `ResetPassword` handler (admin resets another user)**

```go
// @Summary 重置他人密码
// @Description Admin resets another user's password without needing the old password
// @Tags user
// @Param username path string true "username"
// @Param req body model.ResetPasswordReq true "request body"
// @Success 200
// @Router /api/v1/users/{username}/password [put]
func (controller *UserController) ResetPassword(c *gin.Context) {
	username := c.Param(UsernamePath)
	var req model.ResetPasswordReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	user, err := repository.Ps.GetUserByName(username)
	if err != nil {
		controller.wrapfunc(c, model.E_USER_NOT_FOUND, err)
		return
	}
	if err := common.VerifyPassword(req.NewPassword); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}
	hash, err := common.HashPassword(common.Md5CheckSum(req.NewPassword))
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	user.PasswordHash = hash
	user.UpdatedAt = time.Now().Unix()
	if err := repository.Ps.UpdateUser(user); err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}
```

- [ ] **Step 10: Build**

```bash
go build ./controller/...
```

Expected: exit 0.

- [ ] **Step 11: Commit**

```bash
git add controller/user.go controller/controller.go
git commit -m "feat(controller): add user CRUD handlers (list, create, update, delete, password)

- 5 admin-only endpoints under /users
- 2 self-service endpoints under /user (me, change own password)
- input validation: username regex, reserved-name guard, policy whitelist
- password strength enforced via common.VerifyPassword on plaintext input
- delete revokes all in-flight tokens for the removed user"
```

---

## Task 13: Register Routes + Update enforce Whitelist

**Files:**
- Modify: `router/v1.go`
- Modify: `server/enforce/guest.go`

- [ ] **Step 1: Register new routes in `router/v1.go`**

In `InitRouterV1`, near the existing block where other controllers are constructed (`taskController`, etc.), add:

```go
userController := controller.NewUserController(config, WrapMsg)

groupV1.GET("/user/me", userController.Me)
groupV1.PUT("/user/password", userController.ChangeMyPassword)
groupV1.GET("/users", userController.List)
groupV1.POST("/users", userController.Create)
groupV1.PUT(fmt.Sprintf("/users/:%s", controller.UsernamePath), userController.Update)
groupV1.DELETE(fmt.Sprintf("/users/:%s", controller.UsernamePath), userController.Delete)
groupV1.PUT(fmt.Sprintf("/users/:%s/password", controller.UsernamePath), userController.ResetPassword)
```

(If `controller.UsernamePath` ended up in `controller/user.go` instead of `controller/controller.go`, the import still works because both files are in the same package.)

`config` should already be available as `InitRouterV1` parameter — verify with `grep "func InitRouterV1" router/v1.go`.

- [ ] **Step 2: Add self-service paths to `server/enforce/guest.go`**

In `server/enforce/guest.go`, add two lines:

```go
{"/user/me", GET},
{"/user/password", PUT},
```

(Anywhere in the slice — group near the top with `/version` and `/instances` for readability.)

`ordinary.go` inherits these via `append(GuestPolicies()...)` — no change there.

- [ ] **Step 3: Build**

```bash
go build ./...
```

Expected: exit 0.

- [ ] **Step 4: Run swag (manual local check, optional)**

```bash
swag init -q || true
```

Expected: regenerated `docs/docs.go` reflects the new endpoints. (CI/Makefile does this automatically; manual run just verifies annotations parse.)

- [ ] **Step 5: Commit**

```bash
git add router/v1.go server/enforce/guest.go docs/docs.go docs/swagger.json docs/swagger.yaml
git commit -m "feat(router): register user management endpoints and self-service paths"
```

Only include `docs/*` files in the commit if they actually changed (i.e. if `swag init` was run locally). Otherwise omit them — `make build` regenerates them.

---

## Task 14: Delete `cmd/password` + Clean up `cmd/ckmanctl`

> **ORDERING:** Although numbered 14, this task is a prerequisite for Task 11's `go build` step. Execute it **before** Task 11 if you are doing tasks in number order — otherwise both tasks can be done at this point (Task 11 + Task 14 share the goal of retiring filesystem-based user code).

**Files:**
- Delete: `cmd/password/password.go`
- Delete: `cmd/password/` directory
- Modify: `cmd/ckmanctl/ckmanctl.go`

- [ ] **Step 1: Remove `password` import + kingpin command + dispatch case in `ckmanctl.go`**

Open `cmd/ckmanctl/ckmanctl.go`:

a. **Top of file (line ~5)** — remove the `ckmanctl password` line from the usage doc comment.

b. **Imports (line ~21)** — remove the line `"github.com/housepower/ckman/cmd/password"`.

c. **Kingpin declarations (lines ~45–46)** — remove:

```go
passCmd = kingpin.Command("password", "encrypt password")
p_cwd   = passCmd.Flag("cwd", "current working directory").Short('p').Default("/etc/ckman").String()
```

d. **Dispatch switch (lines ~94–95)** — remove the `case "password":` arm.

- [ ] **Step 2: Delete the password command directory**

```bash
rm -rf cmd/password
```

- [ ] **Step 3: Build**

```bash
go build ./...
```

Expected: exit 0. If any other consumer references `cmd/password`, the build will tell — fix and re-run.

- [ ] **Step 4: Run all tests**

```bash
go test ./... 2>&1 | tail -10
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add cmd/ckmanctl/ckmanctl.go
git rm -r cmd/password
git commit -m "chore(cmd): remove cmd/password CLI tool

User management is now done through the web UI; the CLI tool is superseded.
Both the password subcommand and its directory are deleted, plus the matching
kingpin declarations and dispatch case in ckmanctl."
```

---

## Task 15: Backend Verification

**Files:** none (verification only)

- [ ] **Step 1: `make build` from a clean state**

```bash
make build 2>&1 | tail -30
```

Expected: success, generated binary in `bin/` or similar (verify against `Makefile` output).

- [ ] **Step 2: `make test` runs the full suite**

```bash
make test 2>&1 | tail -30
```

Expected: pass.

- [ ] **Step 3: `make lint`**

```bash
make lint 2>&1 | tail -30
```

Expected: clean.

- [ ] **Step 4: Fresh-start sqlite smoke**

Run the freshly built binary against a temp config that selects sqlite, no pre-existing DB:

```bash
TMP=$(mktemp -d)
cat > "$TMP/ckman.hjson" <<EOF
{
  server: { port: 18808, persistent_policy: "local" }
  persistent_config: { local: { config_dir: "$TMP", config_file: "test.db" } }
  log: { level: "info" }
}
EOF
./bin/ckman -c "$TMP/ckman.hjson" &
SVR=$!
sleep 2
curl -s -X POST http://localhost:18808/api/login \
  -H "Content-Type: application/json" \
  -d '{"username":"ckman","password":"'"$(printf '%s' 'Ckman123456!' | md5sum | awk '{print $1}')"'"}' | jq .
kill $SVR
```

Expected: response code `0000` with a token and `"policy":"admin"`, `"enabled":true`. Log file should contain the seed WARN line.

- [ ] **Step 5: Verify enforce denies a non-admin to `/users`**

Continuing the fresh DB, create a `guest` user via curl using the admin token, then attempt `GET /api/v1/users` with the guest token:

```bash
ADMIN_TOKEN=...   # from step 4
GUEST_PASS='Guest123!'
GUEST_PASS_MD5=$(printf '%s' "$GUEST_PASS" | md5sum | awk '{print $1}')

curl -s -X POST http://localhost:18808/api/v1/users \
  -H "token: $ADMIN_TOKEN" -H "Content-Type: application/json" \
  -d "{\"username\":\"bob\",\"password\":\"$GUEST_PASS\",\"policy\":\"guest\",\"enabled\":true}" | jq .

GUEST_TOKEN=$(curl -s -X POST http://localhost:18808/api/login \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"bob\",\"password\":\"$GUEST_PASS_MD5\"}" | jq -r .data.token)

curl -s -o /dev/null -w "%{http_code}\n" \
  http://localhost:18808/api/v1/users -H "token: $GUEST_TOKEN"
# enforce response is wrapped — actual semantic is "5021/5022" or "5023" or "5xxx forbidden" in body
curl -s http://localhost:18808/api/v1/users -H "token: $GUEST_TOKEN" | jq .
```

Expected: the body for `GET /users` with guest token reports a non-success retCode (enforce-derived). The exact code depends on existing middleware response; check `server/server.go ginEnforce` for the precise code.

- [ ] **Step 6: Verify portal sentinel still works**

Skip if no `userToken`/RSA keys are configured; otherwise call any guest endpoint with a `userToken` header and confirm 0000.

- [ ] **Step 7: No commit — verification only.**

---

## Task 16: Frontend — APIs Module

**Working directory:** `../ckman-fe` (the sibling working copy, **not** `frontend/`).

**Files:**
- Create: `src/apis/user.ts`
- Modify: `src/apis/index.ts`

- [ ] **Step 1: Confirm clean state**

```bash
cd ../ckman-fe
git status
git branch -vv
```

Expected: clean tree on `main`.

- [ ] **Step 2: Create `src/apis/user.ts`**

```ts
import axios from 'axios';

const url = '/api/v1';

export const UserApi = {
  me() {
    return axios.get(`${url}/user/me`);
  },
  changeMyPassword(params: { old_password: string; new_password: string }) {
    return axios.put(`${url}/user/password`, params);
  },
  list() {
    return axios.get(`${url}/users`);
  },
  create(params: { username: string; password: string; policy: string; enabled: boolean }) {
    return axios.post(`${url}/users`, params);
  },
  update(username: string, params: { policy?: string; enabled?: boolean }) {
    return axios.put(`${url}/users/${encodeURIComponent(username)}`, params);
  },
  delete(username: string) {
    return axios.delete(`${url}/users/${encodeURIComponent(username)}`);
  },
  resetPassword(username: string, params: { new_password: string }) {
    return axios.put(`${url}/users/${encodeURIComponent(username)}/password`, params);
  },
};
```

- [ ] **Step 3: Export from `src/apis/index.ts`**

Append:

```ts
export * from './user';
```

- [ ] **Step 4: Lint check**

```bash
cd ../ckman-fe
make lint 2>&1 | tail -20
```

Expected: no new errors related to the new file.

- [ ] **Step 5: Commit**

```bash
cd ../ckman-fe
git add src/apis/user.ts src/apis/index.ts
git commit -m "feat(api): add user management API client"
```

---

## Task 17: Frontend — Update login.vue to Store policy/enabled

**Working directory:** `../ckman-fe`

**Files:**
- Modify: `src/views/login/login.vue`

- [ ] **Step 1: Update the `login` method**

In `src/views/login/login.vue`, replace the `login` method body's `localStorage.setItem` call so the entire entity (with new `policy` and `enabled`) is stored. Existing code already does `localStorage.setItem("user", JSON.stringify(entity))` — the backend response now includes `policy` and `enabled`, so the existing call captures them automatically. **No change needed** unless the file destructures only some fields.

Re-read `src/views/login/login.vue` lines 75–90 to verify. If `entity` is opaque (saved whole) → no edit. If `entity` is destructured → ensure `policy` and `enabled` are included in what is persisted.

- [ ] **Step 2: Confirm no commit needed if no functional change.**

If a change was needed, commit:

```bash
git add src/views/login/login.vue
git commit -m "feat(login): persist policy and enabled from extended LoginRsp"
```

Otherwise skip.

---

## Task 18: Frontend — Update layout.vue with Users Link, Dropdown Change-Password, /user/me Refresh

**Working directory:** `../ckman-fe`

**Files:**
- Modify: `src/views/layout/layout.vue`

- [ ] **Step 1: Add data fields + computed**

In the `data()` block, add `isAdmin: false`. In `computed`, leave existing `title`. The script-side imports should include `UserApi`:

```js
import { PackageApi, ClusterApi, UserApi } from "@/apis";
```

- [ ] **Step 2: Replace `mounted()` to fetch `/user/me`**

```js
async mounted() {
  await this.refreshMe();
  this.fetchVersion();
},
methods: {
  async refreshMe() {
    try {
      const { data: { entity } } = await UserApi.me();
      if (!entity || !entity.enabled) {
        this.$router.push('/login');
        return;
      }
      this.user = entity.username;
      this.isAdmin = entity.policy === 'admin';
      this.$root.userInfo = entity;
      localStorage.setItem('user', JSON.stringify(entity));
    } catch (e) {
      this.$router.push('/login');
    }
  },
  // ... existing methods ...
  openChangePassword() {
    this.$modal.open({
      component: () => import('@/views/users/modal/changePassword.vue'),
      props: {},
    });
  },
}
```

(If the project uses a different modal launcher than `$modal.open` — inspect existing modal launches in `src/views/manage/manage.vue` or similar — use whatever pattern is already in use.)

- [ ] **Step 3: Add Users link + dropdown item to the template**

After the existing `<router-link to="/setting"` block, insert:

```html
<router-link
  v-if="isAdmin"
  to="/users"
  class="fa fa-users fs-20 pointer mr-15">
  <span class="fs-16 ml-5">{{ $t('common.users') }}</span>
</router-link>
```

Inside the existing `<el-dropdown-menu>`, add before the Logout item:

```html
<el-dropdown-item @click.native="openChangePassword">{{ $t('user.Change Password') }}</el-dropdown-item>
```

- [ ] **Step 4: Lint**

```bash
make lint 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add src/views/layout/layout.vue
git commit -m "feat(layout): refresh user info on mount, conditionally show Users link + Change Password"
```

---

## Task 19: Frontend — Router + Admin Guard

**Working directory:** `../ckman-fe`

**Files:**
- Modify: `src/services/router.ts`

- [ ] **Step 1: Add `/users` route**

Inside the `children` of the `Layout` route (around line 50–60 of `router.ts`), add:

```ts
{
  path: 'users',
  name: 'Users',
  component: () => import('@/views/users/users.vue'),
  meta: { requiresAdmin: true },
},
```

- [ ] **Step 2: Add global `beforeEach` guard**

Below the `$router = new Router(...)` declaration:

```ts
$router.beforeEach((to, _from, next) => {
  if (to.matched.some(r => r.meta && r.meta.requiresAdmin)) {
    let policy = '';
    try {
      policy = JSON.parse(localStorage.getItem('user') || '{}').policy || '';
    } catch (_e) {
      policy = '';
    }
    if (policy !== 'admin') {
      return next({ path: '/home' });
    }
  }
  next();
});
```

- [ ] **Step 3: Lint**

```bash
make lint 2>&1 | tail -10
```

- [ ] **Step 4: Commit**

```bash
git add src/services/router.ts
git commit -m "feat(router): add /users route with admin-only guard"
```

---

## Task 20: Frontend — Users List Page

**Working directory:** `../ckman-fe`

**Files:**
- Create: `src/views/users/users.vue`

- [ ] **Step 1: Create the file**

```vue
<template>
  <main class="users-page">
    <header class="users-page__bar">
      <h2 class="users-page__title">{{ $t('user.User Management') }}</h2>
      <el-button type="primary" size="small" @click="openCreate">
        <i class="fa fa-plus" /> {{ $t('user.Add User') }}
      </el-button>
    </header>
    <el-table :data="rows" stripe v-loading="loading">
      <el-table-column prop="username" :label="$t('user.Username')" />
      <el-table-column :label="$t('user.Role')">
        <template slot-scope="{ row }">
          {{ $t('user.Policy.' + row.policy) }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('user.Enabled')" width="120">
        <template slot-scope="{ row }">
          <el-switch
            :value="row.enabled"
            :disabled="isBuiltin(row.username)"
            @change="onToggleEnabled(row, $event)"
          />
        </template>
      </el-table-column>
      <el-table-column :label="$t('user.Created At')" width="180">
        <template slot-scope="{ row }">
          {{ formatTs(row.created_at) }}
        </template>
      </el-table-column>
      <el-table-column :label="$t('common.Action')" width="280">
        <template slot-scope="{ row }">
          <el-button
            size="mini"
            :disabled="isBuiltin(row.username)"
            @click="openEdit(row)"
          >{{ $t('user.Edit User') }}</el-button>
          <el-button size="mini" @click="openResetPwd(row)">
            {{ $t('user.Reset Password') }}
          </el-button>
          <el-button
            size="mini"
            type="danger"
            :disabled="isBuiltin(row.username)"
            @click="onDelete(row)"
          >{{ $t('common.Delete') }}</el-button>
        </template>
      </el-table-column>
    </el-table>
  </main>
</template>

<script>
import { UserApi } from '@/apis';

const BUILTIN = new Set(['ckman', 'ordinary']);

export default {
  name: 'Users',
  data() {
    return { rows: [], loading: false };
  },
  async created() {
    await this.refresh();
  },
  methods: {
    async refresh() {
      this.loading = true;
      try {
        const { data: { entity } } = await UserApi.list();
        this.rows = entity || [];
      } finally {
        this.loading = false;
      }
    },
    isBuiltin(name) {
      return BUILTIN.has(name);
    },
    formatTs(sec) {
      if (!sec) return '';
      return new Date(sec * 1000).toLocaleString();
    },
    async onToggleEnabled(row, next) {
      const action = next ? this.$t('common.Enable') : this.$t('common.Disable');
      try {
        await this.$confirm(`${action} ${row.username}?`, this.$t('common.tips'), {
          confirmButtonText: this.$t('common.Confirm'),
          cancelButtonText: this.$t('common.Cancel'),
        });
      } catch (_) {
        return;
      }
      await UserApi.update(row.username, { enabled: next });
      this.$message.success(this.$t('common.Success'));
      await this.refresh();
    },
    async onDelete(row) {
      try {
        await this.$confirm(this.$t('common.Confirm Delete'), this.$t('common.tips'), {
          confirmButtonText: this.$t('common.Confirm'),
          cancelButtonText: this.$t('common.Cancel'),
          type: 'warning',
        });
      } catch (_) {
        return;
      }
      await UserApi.delete(row.username);
      this.$message.success(this.$t('common.Success'));
      await this.refresh();
    },
    openCreate() {
      this.$modal.open({
        component: () => import('@/views/users/modal/createUser.vue'),
        props: {},
        events: { submitted: () => this.refresh() },
      });
    },
    openEdit(row) {
      this.$modal.open({
        component: () => import('@/views/users/modal/editUser.vue'),
        props: { user: row },
        events: { submitted: () => this.refresh() },
      });
    },
    openResetPwd(row) {
      this.$modal.open({
        component: () => import('@/views/users/modal/resetPassword.vue'),
        props: { username: row.username },
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.users-page {
  padding: var(--s-4) var(--s-5);

  &__bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: var(--s-3);
  }

  &__title {
    margin: 0;
    font-size: var(--fs-lg);
    font-weight: var(--fw-semibold);
  }
}
</style>
```

If `this.$modal.open(...)` is not the project pattern, look at an existing list page like `src/views/task/list.vue` for how modals are launched and adapt accordingly.

- [ ] **Step 2: Build the frontend**

```bash
cd ../ckman-fe
make build 2>&1 | tail -30
```

Expected: success. If `make build` fails because the modal files don't exist yet, set them up as stubs first (Tasks 21–24) and circle back.

If `make build` is too heavy at this point, run `make lint` instead:

```bash
make lint 2>&1 | tail -10
```

- [ ] **Step 3: Commit**

```bash
git add src/views/users/users.vue
git commit -m "feat(users): add users list page with inline switch, edit/delete/reset actions"
```

---

## Task 21: Frontend — Create User Modal

**Working directory:** `../ckman-fe`

**Files:**
- Create: `src/views/users/modal/createUser.vue`

- [ ] **Step 1: Create the modal**

```vue
<template>
  <el-dialog :visible.sync="visible" :title="$t('user.Add User')" width="480px" :before-close="cancel">
    <el-form ref="form" :model="form" :rules="rules" label-width="120px" @submit.native.prevent>
      <el-form-item :label="$t('user.Username')" prop="username">
        <el-input v-model="form.username" autocomplete="off" />
      </el-form-item>
      <el-form-item :label="$t('user.Role')" prop="policy">
        <el-select v-model="form.policy" style="width: 100%">
          <el-option value="ordinary" :label="$t('user.Policy.ordinary')" />
          <el-option value="guest" :label="$t('user.Policy.guest')" />
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('user.Enabled')">
        <el-switch v-model="form.enabled" />
      </el-form-item>
      <el-form-item :label="$t('user.New Password')" prop="password">
        <el-input v-model="form.password" type="password" autocomplete="new-password" show-password />
      </el-form-item>
      <el-form-item :label="$t('user.Confirm Password')" prop="confirm">
        <el-input v-model="form.confirm" type="password" autocomplete="new-password" show-password />
      </el-form-item>
    </el-form>
    <span slot="footer">
      <el-button @click="cancel">{{ $t('common.Cancel') }}</el-button>
      <el-button type="primary" :loading="loading" @click="submit">{{ $t('common.Confirm') }}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import { UserApi } from '@/apis';

const USERNAME_RE = /^[A-Za-z][A-Za-z0-9_]{2,31}$/;
const RESERVED = new Set(['ckman', 'ordinary']);

function passwordPolicy(pwd) {
  if (typeof pwd !== 'string' || pwd.length < 8) return false;
  let cats = 0;
  if (/[a-z]/.test(pwd)) cats++;
  if (/[A-Z]/.test(pwd)) cats++;
  if (/[0-9]/.test(pwd)) cats++;
  if (/[^A-Za-z0-9]/.test(pwd)) cats++;
  return cats >= 3;
}

export default {
  data() {
    return {
      visible: true,
      loading: false,
      form: { username: '', policy: 'ordinary', enabled: true, password: '', confirm: '' },
      rules: {
        username: [
          { required: true, message: this.$t('common.Required'), trigger: 'blur' },
          {
            validator: (_r, v, cb) => {
              if (!USERNAME_RE.test(v)) return cb(new Error(this.$t('user.Username Rule')));
              if (RESERVED.has(v)) return cb(new Error(this.$t('user.Reserved Username')));
              cb();
            },
            trigger: 'blur',
          },
        ],
        policy: [{ required: true, message: this.$t('common.Required'), trigger: 'change' }],
        password: [
          { required: true, message: this.$t('common.Required'), trigger: 'blur' },
          {
            validator: (_r, v, cb) => passwordPolicy(v) ? cb() : cb(new Error(this.$t('user.Password Rule'))),
            trigger: 'blur',
          },
        ],
        confirm: [
          {
            validator: (_r, v, cb) => v === this.form.password ? cb() : cb(new Error(this.$t('user.Password Mismatch'))),
            trigger: 'blur',
          },
        ],
      },
    };
  },
  methods: {
    cancel() {
      this.visible = false;
      this.$emit('close');
    },
    async submit() {
      await this.$refs.form.validate();
      this.loading = true;
      try {
        await UserApi.create({
          username: this.form.username,
          password: this.form.password,
          policy: this.form.policy,
          enabled: this.form.enabled,
        });
        this.$message.success(this.$t('common.Success'));
        this.$emit('submitted');
        this.cancel();
      } finally {
        this.loading = false;
      }
    },
  },
};
</script>
```

- [ ] **Step 2: Lint**

```bash
make lint 2>&1 | tail -10
```

- [ ] **Step 3: Commit**

```bash
git add src/views/users/modal/createUser.vue
git commit -m "feat(users): add create-user modal with policy + enabled + password strength validation"
```

---

## Task 22: Frontend — Edit User Modal

**Working directory:** `../ckman-fe`

**Files:**
- Create: `src/views/users/modal/editUser.vue`

- [ ] **Step 1: Create the modal**

```vue
<template>
  <el-dialog :visible.sync="visible" :title="$t('user.Edit User')" width="480px" :before-close="cancel">
    <el-form ref="form" :model="form" label-width="120px" @submit.native.prevent>
      <el-form-item :label="$t('user.Username')">
        <el-input :value="user.username" disabled />
      </el-form-item>
      <el-form-item :label="$t('user.Role')">
        <el-select v-model="form.policy" style="width: 100%">
          <el-option value="ordinary" :label="$t('user.Policy.ordinary')" />
          <el-option value="guest" :label="$t('user.Policy.guest')" />
        </el-select>
      </el-form-item>
      <el-form-item :label="$t('user.Enabled')">
        <el-switch v-model="form.enabled" />
      </el-form-item>
    </el-form>
    <span slot="footer">
      <el-button @click="cancel">{{ $t('common.Cancel') }}</el-button>
      <el-button type="primary" :loading="loading" @click="submit">{{ $t('common.Confirm') }}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import { UserApi } from '@/apis';

export default {
  props: {
    user: { type: Object, required: true },
  },
  data() {
    return {
      visible: true,
      loading: false,
      form: { policy: this.user.policy, enabled: this.user.enabled },
    };
  },
  methods: {
    cancel() {
      this.visible = false;
      this.$emit('close');
    },
    async submit() {
      this.loading = true;
      try {
        await UserApi.update(this.user.username, {
          policy: this.form.policy,
          enabled: this.form.enabled,
        });
        this.$message.success(this.$t('common.Success'));
        this.$emit('submitted');
        this.cancel();
      } finally {
        this.loading = false;
      }
    },
  },
};
</script>
```

- [ ] **Step 2: Commit**

```bash
git add src/views/users/modal/editUser.vue
git commit -m "feat(users): add edit-user modal (policy + enabled)"
```

---

## Task 23: Frontend — Reset Password Modal (Admin) + Change My Password Modal (Self)

**Working directory:** `../ckman-fe`

**Files:**
- Create: `src/views/users/modal/resetPassword.vue`
- Create: `src/views/users/modal/changePassword.vue`

- [ ] **Step 1: Create `resetPassword.vue`**

```vue
<template>
  <el-dialog :visible.sync="visible" :title="$t('user.Reset Password')" width="440px" :before-close="cancel">
    <p class="reset-target">{{ $t('user.Username') }}: <strong>{{ username }}</strong></p>
    <el-form ref="form" :model="form" :rules="rules" label-width="120px" @submit.native.prevent>
      <el-form-item :label="$t('user.New Password')" prop="password">
        <el-input v-model="form.password" type="password" show-password autocomplete="new-password" />
      </el-form-item>
      <el-form-item :label="$t('user.Confirm Password')" prop="confirm">
        <el-input v-model="form.confirm" type="password" show-password autocomplete="new-password" />
      </el-form-item>
    </el-form>
    <span slot="footer">
      <el-button @click="cancel">{{ $t('common.Cancel') }}</el-button>
      <el-button type="primary" :loading="loading" @click="submit">{{ $t('common.Confirm') }}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import { UserApi } from '@/apis';

function passwordPolicy(pwd) {
  if (typeof pwd !== 'string' || pwd.length < 8) return false;
  let cats = 0;
  if (/[a-z]/.test(pwd)) cats++;
  if (/[A-Z]/.test(pwd)) cats++;
  if (/[0-9]/.test(pwd)) cats++;
  if (/[^A-Za-z0-9]/.test(pwd)) cats++;
  return cats >= 3;
}

export default {
  props: {
    username: { type: String, required: true },
  },
  data() {
    return {
      visible: true,
      loading: false,
      form: { password: '', confirm: '' },
      rules: {
        password: [
          { required: true, message: this.$t('common.Required'), trigger: 'blur' },
          {
            validator: (_r, v, cb) => passwordPolicy(v) ? cb() : cb(new Error(this.$t('user.Password Rule'))),
            trigger: 'blur',
          },
        ],
        confirm: [
          {
            validator: (_r, v, cb) => v === this.form.password ? cb() : cb(new Error(this.$t('user.Password Mismatch'))),
            trigger: 'blur',
          },
        ],
      },
    };
  },
  methods: {
    cancel() {
      this.visible = false;
      this.$emit('close');
    },
    async submit() {
      await this.$refs.form.validate();
      this.loading = true;
      try {
        await UserApi.resetPassword(this.username, { new_password: this.form.password });
        this.$message.success(this.$t('common.Success'));
        this.cancel();
      } finally {
        this.loading = false;
      }
    },
  },
};
</script>

<style scoped>
.reset-target { margin: 0 0 16px; }
</style>
```

- [ ] **Step 2: Create `changePassword.vue`**

```vue
<template>
  <el-dialog :visible.sync="visible" :title="$t('user.Change Password')" width="440px" :before-close="cancel">
    <el-form ref="form" :model="form" :rules="rules" label-width="120px" @submit.native.prevent>
      <el-form-item :label="$t('user.Old Password')" prop="oldp">
        <el-input v-model="form.oldp" type="password" show-password autocomplete="current-password" />
      </el-form-item>
      <el-form-item :label="$t('user.New Password')" prop="newp">
        <el-input v-model="form.newp" type="password" show-password autocomplete="new-password" />
      </el-form-item>
      <el-form-item :label="$t('user.Confirm Password')" prop="confirm">
        <el-input v-model="form.confirm" type="password" show-password autocomplete="new-password" />
      </el-form-item>
    </el-form>
    <span slot="footer">
      <el-button @click="cancel">{{ $t('common.Cancel') }}</el-button>
      <el-button type="primary" :loading="loading" @click="submit">{{ $t('common.Confirm') }}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import { UserApi } from '@/apis';

function passwordPolicy(pwd) {
  if (typeof pwd !== 'string' || pwd.length < 8) return false;
  let cats = 0;
  if (/[a-z]/.test(pwd)) cats++;
  if (/[A-Z]/.test(pwd)) cats++;
  if (/[0-9]/.test(pwd)) cats++;
  if (/[^A-Za-z0-9]/.test(pwd)) cats++;
  return cats >= 3;
}

export default {
  data() {
    return {
      visible: true,
      loading: false,
      form: { oldp: '', newp: '', confirm: '' },
      rules: {
        oldp: [{ required: true, message: this.$t('common.Required'), trigger: 'blur' }],
        newp: [
          { required: true, message: this.$t('common.Required'), trigger: 'blur' },
          {
            validator: (_r, v, cb) => passwordPolicy(v) ? cb() : cb(new Error(this.$t('user.Password Rule'))),
            trigger: 'blur',
          },
        ],
        confirm: [
          {
            validator: (_r, v, cb) => v === this.form.newp ? cb() : cb(new Error(this.$t('user.Password Mismatch'))),
            trigger: 'blur',
          },
        ],
      },
    };
  },
  methods: {
    cancel() {
      this.visible = false;
      this.$emit('close');
    },
    async submit() {
      await this.$refs.form.validate();
      this.loading = true;
      try {
        await UserApi.changeMyPassword({
          old_password: this.form.oldp,
          new_password: this.form.newp,
        });
        this.$message.success(this.$t('common.Success'));
        this.cancel();
      } finally {
        this.loading = false;
      }
    },
  },
};
</script>
```

- [ ] **Step 3: Lint**

```bash
make lint 2>&1 | tail -10
```

- [ ] **Step 4: Commit**

```bash
git add src/views/users/modal/resetPassword.vue src/views/users/modal/changePassword.vue
git commit -m "feat(users): add reset-password (admin) and change-password (self) modals"
```

---

## Task 24: Frontend — i18n Keys + Frontend Build

**Working directory:** `../ckman-fe`

**Files:**
- Modify: `src/services/i18n.ts`

- [ ] **Step 1: Add keys to both language maps**

Read `src/services/i18n.ts` to find the `messages.en.common`, `messages.en.user` (may not exist), and corresponding `zh` blocks. Add or extend:

`en.common`:
```
'users': 'Users',
'Enable': 'Enable',
'Disable': 'Disable',
'Action': 'Action',     // if not already present
```

`en.user` (create the block if not present):
```
'User Management': 'User Management',
'Add User': 'Add User',
'Edit User': 'Edit User',
'Reset Password': 'Reset Password',
'Change Password': 'Change Password',
'Username': 'Username',
'Role': 'Role',
'Enabled': 'Enabled',
'Created At': 'Created At',
'Old Password': 'Old Password',
'New Password': 'New Password',
'Confirm Password': 'Confirm Password',
'Username Rule': 'Username must start with a letter and be 3-32 chars (letters, digits, underscore)',
'Reserved Username': 'Reserved username, please choose another',
'Password Rule': 'Password ≥ 8 chars, ≥ 3 of: lowercase, uppercase, digit, special',
'Password Mismatch': 'Passwords do not match',
'Policy.admin': 'Admin',
'Policy.guest': 'Guest',
'Policy.ordinary': 'Ordinary',
```

`zh.common`:
```
'users': '用户',
'Enable': '启用',
'Disable': '禁用',
```

`zh.user`:
```
'User Management': '用户管理',
'Add User': '新增用户',
'Edit User': '编辑用户',
'Reset Password': '重置密码',
'Change Password': '修改密码',
'Username': '用户名',
'Role': '角色',
'Enabled': '启用',
'Created At': '创建时间',
'Old Password': '旧密码',
'New Password': '新密码',
'Confirm Password': '确认密码',
'Username Rule': '用户名以字母开头，3-32 位（字母、数字、下划线）',
'Reserved Username': '保留用户名，请选择其他名称',
'Password Rule': '密码至少 8 位，包含大写、小写、数字、特殊字符中的至少 3 类',
'Password Mismatch': '两次输入的密码不一致',
'Policy.admin': '管理员',
'Policy.guest': '游客',
'Policy.ordinary': '普通用户',
```

- [ ] **Step 2: Build the frontend**

```bash
cd ../ckman-fe
make build 2>&1 | tail -40
```

Expected: success. Output goes to `static/dist/` (or wherever the project's vue.config.js points).

- [ ] **Step 3: Commit**

```bash
git add src/services/i18n.ts
git commit -m "feat(i18n): add translation keys for user management"
```

- [ ] **Step 4: Push the frontend changes**

```bash
# Push to whichever remote the parent ckman repo's `frontend` submodule is configured to fetch from.
# Check the submodule's configured URL with: cd ../ckman && git config -f .gitmodules submodule.frontend.url
# Then push from ../ckman-fe to that remote/branch.
git push <remote> main
```

Capture the latest commit SHA: `git rev-parse HEAD` — you will need it in Task 25.

---

## Task 25: Update Frontend Submodule Pointer in ckman Repo

**Working directory:** `/data/root/go/src/github.com/housepower/ckman`

- [ ] **Step 1: Pull and check out the new frontend SHA in the submodule**

```bash
cd /data/root/go/src/github.com/housepower/ckman/frontend
git fetch
git checkout <SHA from Task 24>   # the latest commit on main in ckman-fe
cd ..
```

- [ ] **Step 2: Stage and commit the submodule bump**

```bash
git add frontend
git commit -m "chore(frontend): bump submodule to include user management UI"
```

- [ ] **Step 3: Rebuild ckman to embed the new dist**

```bash
make build 2>&1 | tail -20
```

Expected: success.

---

## Task 26: End-to-End Verification

**Files:** none (manual / scripted verification).

Walk the full §12 list from the spec. Capture results in a scratchpad.

- [ ] **Step 1: Build + test + lint**

```bash
make test
make lint
make build
```

All three pass.

- [ ] **Step 2: Each backend AutoMigrates cleanly**

Start ckman against each of `local` (sqlite), `mysql`, `postgres`, `dm8` configs in sequence (or whichever the host machine has). Verify `ckman_users` table is created on each, and the seed admin appears.

If any backend isn't locally available, document that this step was deferred and which backend(s) remain unverified.

- [ ] **Step 3: Run the full UI scenario in a browser**

   a. Open the UI, login as `ckman / Ckman123456!`
   b. Top bar shows the "Users" link (admin)
   c. Open the Users list — only `ckman` is listed
   d. Add user `alice / Ordi123!` with role `ordinary`, enabled
   e. Add user `bob / Gues123!` with role `guest`, enabled
   f. Inline-toggle `bob`'s switch to disabled, confirm
   g. Logout, login as `bob` → no Users link, navigating to `/users` redirects to `/home`
   h. Logout, login as `bob` again (should fail with "账号已禁用")
   i. Login as admin, re-enable `bob`, login succeeds again
   j. As `alice`, open "Change Password" from the user dropdown, change to a new password, login again succeeds
   k. As admin, reset `alice`'s password (no old password required); `alice` can log in with new password
   l. As admin, delete `alice`; her active token (if still cached) now yields a 401/403 on next call
   m. Try to edit or delete `ckman` from the list — buttons are disabled with tooltip

- [ ] **Step 4: Sanity-check the patched policies**

   - As `bob` (guest, re-enabled): `GET /api/v1/data_manage/disks/<cluster>` returns 200 (formerly was admin-only)
   - As `alice` (ordinary): `PUT /api/v1/ck/partition/<cluster>` no longer denied (the call may fail at the business layer if data is missing, but it should pass enforce)

- [ ] **Step 5: Verify `cmd/password` is gone**

```bash
ls cmd/password 2>&1
# Expected: ls: cannot access 'cmd/password': No such file or directory
./bin/ckmanctl --help 2>&1 | grep -i password
# Expected: no match
```

- [ ] **Step 6: Verify portal token path still works**

If a third-party caller is configured, hit any guest-allowed endpoint with the `userToken` RSA header. Expected: 0000.

If no portal caller is configured locally, document this step as untested.

- [ ] **Step 7: No commit — verification only.**

---

## Done Criteria

When every box above is checked:

1. `ckman_users` table exists in all 4 backends with the seed admin record
2. `cmd/password` directory deleted, `ckmanctl password` subcommand removed
3. 7 new HTTP endpoints live under `/api/v1/{user,users}`
4. 5 enforce policy patches in place (4 add + 1 delete)
5. Frontend Users page accessible only to admins; password change available to all logged-in users
6. `make build` produces a single binary serving the new UI from `static/dist/`
7. All listed manual verification steps pass

---

## Notes For The Implementer

- **Always run sqlite tests first** when a refactor touches the repository layer — they catch wiring bugs cheapest.
- **GORM AutoMigrate is additive-only** for column adds; if you ever need to change a column type or rename a column, write an explicit migration in `tbl_meta` style.
- **Do not introduce schema_version bumps** for this work — it's purely an additive table addition; AutoMigrate handles it transparently.
- **Frontend submodule discipline** — every edit goes in `../ckman-fe`. The `frontend/` submodule in the parent repo is only ever bumped to a SHA that is already on the `ckman-fe:main` remote tip.
- **No new configuration knobs** — `Ckman123456!` is hardcoded by spec decision. If a future security review demands per-deployment overrides, that's a follow-up change.
- **Token revoke is best-effort** — `revokeTokensFor` iterates the in-memory `TokenCache`, so multi-instance deployments will not have cross-instance revocation. This matches existing ckman behaviour and is called out as a known limitation in the spec.
