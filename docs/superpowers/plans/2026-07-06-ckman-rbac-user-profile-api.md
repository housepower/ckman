# ckman RBAC 用户/Profile 接口 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 ckman 增加一组后端 API,通过 SQL RBAC 在 CH 集群上动态管理用户(只读/读写)和 settings profile。

**Architecture:** CH 系统表为唯一真相源,零持久层改动。所有变更类 SQL 走 `ON CLUSTER` 由 CH 分布式 DDL 队列下发。分三层:`model/rbac.go`(结构体)、`service/clickhouse/rbac.go`(纯 SQL 拼接函数 + 执行/查询封装)、`controller/rbac.go`(HTTP handler + 校验),路由注册进 `router/v1.go`。

**Tech Stack:** Go 1.24, Gin, ClickHouse(`common.Conn`),现有 `clickhouse.GetCkService` 连接池。

## Global Constraints

- 全部变更类 SQL 必须带 `ON CLUSTER \`<cluster>\``,cluster 名取自 `ck.Config.Cluster`。
- 标识符(用户名/profile 名/setting key/name 筛选关键字)只允许 `^[a-zA-Z0-9_]+$`,不合法直接拒绝(防注入)。
- 字符串字面量(密码、setting 字符串值、network)入 SQL 前必须转义:`\` → `\\`,`'` → `\'`。
- readwrite = `GRANT ALL` 后 `REVOKE ACCESS MANAGEMENT`;readonly = `GRANT SELECT`。
- 保留用户 `default` 禁止 Alter/Drop。
- 删除类 SQL 用 `IF EXISTS` 保证幂等。
- 不改动 `repository/` 任何后端,不触发 dbtest 集成脚手架。
- 路由前缀 `/api/v1/ck/rbac/...`,注册进 `router/v1.go` 的 `InitRouterV1`。

参考签名(现成,直接用):
- `clickhouse.GetCkService(clusterName string) (*clickhouse.CkService, error)`
- `CkService{ Config *model.CKManClickHouseConfig; Conn *common.Conn }`,`ck.Config.Cluster string`
- `ck.Conn.Exec(query string, args ...any) error`
- `ck.QueryInfo(query string) ([][]interface{}, error)` — 返回值 `data[0]` 是列名行,`data[1:]` 是数据行
- 错误码:`model.E_INVALID_PARAMS`、`model.E_CH_CONNECT_FAILED`、`model.E_DATA_SELECT_FAILED`、`model.E_DATA_INSERT_FAILED`、`model.E_DATA_DELETE_FAILED`、`model.E_SUCCESS`
- Handler 惯例:`c.Param(controller.ClickHouseClusterPath)`、`c.Query("name")`、`c.ShouldBindJSON(&req)`、`controller.wrapfunc(c, code, data)`

## File Structure

- Create `model/rbac.go` — 请求/响应结构体 + access 常量。
- Create `service/clickhouse/rbac.go` — 纯 SQL 拼接函数、校验/转义 helper、执行与查询封装。
- Create `service/clickhouse/rbac_test.go` — 纯函数单元测试(主要测试面,不连库)。
- Create `controller/rbac.go` — `RBACController` + 10 个 handler。
- Modify `router/v1.go` — 构造 controller + 注册 10 条路由。

---

### Task 1: 数据模型 + SQL 拼接纯函数(TDD 核心)

**Files:**
- Create: `model/rbac.go`
- Create: `service/clickhouse/rbac.go`(本任务只放纯函数与 helper)
- Test: `service/clickhouse/rbac_test.go`

**Interfaces:**
- Produces(供 Task 2/3 使用):
  - 结构体 `model.RbacUserCreateReq`、`model.RbacUserAlterReq`、`model.RbacUserInfo`、`model.RbacProfileCreateReq`、`model.RbacProfileConstraint`、`model.RbacProfileInfo`
  - 常量 `model.RbacAccessReadOnly = "readonly"`、`model.RbacAccessReadWrite = "readwrite"`
  - 纯函数(均属 package `clickhouse`):
    - `BuildCreateUserSQL(cluster string, req model.RbacUserCreateReq) ([]string, error)`
    - `BuildAlterUserSQL(cluster, name string, req model.RbacUserAlterReq) ([]string, error)`
    - `BuildDropUserSQL(cluster, name string) (string, error)`
    - `BuildListUsersSQL(nameFilter string) (string, error)`
    - `BuildCreateProfileSQL(cluster string, req model.RbacProfileCreateReq) (string, error)`
    - `BuildAlterProfileSQL(cluster string, req model.RbacProfileCreateReq) (string, error)`
    - `BuildDropProfileSQL(cluster, name string) (string, error)`
    - `BuildListProfilesSQL(nameFilter string) (string, error)`
    - `DeriveAccess(grants []string) string`
    - `IsReservedRbacUser(name string) bool`

- [ ] **Step 1: 写 model/rbac.go**

```go
package model

const (
	RbacAccessReadOnly  = "readonly"
	RbacAccessReadWrite = "readwrite"
)

// RbacUserCreateReq 创建用户请求
type RbacUserCreateReq struct {
	Name     string   `json:"name" example:"alice"`
	Password string   `json:"password" example:"123456"`
	Access   string   `json:"access" example:"readonly"` // readonly | readwrite
	Profile  string   `json:"profile" example:"readonly_p"`
	Networks []string `json:"networks"`
}

// RbacUserAlterReq 修改用户请求,字段均可选,仅修改传入项
type RbacUserAlterReq struct {
	Password string `json:"password"`
	Access   string `json:"access"`
	Profile  string `json:"profile"`
}

// RbacUserInfo 用户信息(List/Get 返回)
type RbacUserInfo struct {
	Name     string   `json:"name"`
	Access   string   `json:"access"`
	Profile  string   `json:"profile"`
	Networks []string `json:"networks"`
}

// RbacProfileConstraint profile 约束
type RbacProfileConstraint struct {
	Readonly bool        `json:"readonly"`
	Min      interface{} `json:"min"`
	Max      interface{} `json:"max"`
}

// RbacProfileCreateReq 创建/修改 profile 请求
type RbacProfileCreateReq struct {
	Name        string                           `json:"name" example:"readonly_p"`
	Settings    map[string]interface{}           `json:"settings"`
	Constraints map[string]RbacProfileConstraint `json:"constraints"`
}

// RbacProfileInfo profile 信息(List/Get 返回)
type RbacProfileInfo struct {
	Name     string                 `json:"name"`
	Settings map[string]interface{} `json:"settings"`
}
```

- [ ] **Step 2: 写失败测试 service/clickhouse/rbac_test.go**

```go
package clickhouse

import (
	"testing"

	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
)

func TestBuildCreateUserSQL_ReadOnly(t *testing.T) {
	req := model.RbacUserCreateReq{
		Name: "alice", Password: "p@ss", Access: model.RbacAccessReadOnly, Profile: "ro",
	}
	stmts, err := BuildCreateUserSQL("test", req)
	assert.NoError(t, err)
	assert.Equal(t, []string{
		"CREATE USER `alice` ON CLUSTER `test` IDENTIFIED BY 'p@ss' HOST IP '::/0' SETTINGS PROFILE `ro`",
		"GRANT ON CLUSTER `test` SELECT ON *.* TO `alice`",
	}, stmts)
}

func TestBuildCreateUserSQL_ReadWrite(t *testing.T) {
	req := model.RbacUserCreateReq{Name: "bob", Password: "x", Access: model.RbacAccessReadWrite}
	stmts, err := BuildCreateUserSQL("test", req)
	assert.NoError(t, err)
	assert.Equal(t, []string{
		"CREATE USER `bob` ON CLUSTER `test` IDENTIFIED BY 'x' HOST IP '::/0'",
		"GRANT ON CLUSTER `test` ALL ON *.* TO `bob`",
		"REVOKE ON CLUSTER `test` ACCESS MANAGEMENT ON *.* FROM `bob`",
	}, stmts)
}

func TestBuildCreateUserSQL_InvalidName(t *testing.T) {
	_, err := BuildCreateUserSQL("test", model.RbacUserCreateReq{Name: "a; DROP", Password: "x", Access: "readonly"})
	assert.Error(t, err)
}

func TestBuildCreateUserSQL_EmptyPassword(t *testing.T) {
	_, err := BuildCreateUserSQL("test", model.RbacUserCreateReq{Name: "a", Access: "readonly"})
	assert.Error(t, err)
}

func TestBuildCreateUserSQL_EscapesPassword(t *testing.T) {
	stmts, err := BuildCreateUserSQL("test", model.RbacUserCreateReq{Name: "a", Password: "a'b", Access: "readonly"})
	assert.NoError(t, err)
	assert.Contains(t, stmts[0], `IDENTIFIED BY 'a\'b'`)
}

func TestBuildAlterUserSQL(t *testing.T) {
	stmts, err := BuildAlterUserSQL("test", "alice", model.RbacUserAlterReq{Password: "np", Profile: "p2", Access: "readwrite"})
	assert.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER USER `alice` ON CLUSTER `test` IDENTIFIED BY 'np'",
		"ALTER USER `alice` ON CLUSTER `test` SETTINGS PROFILE `p2`",
		"REVOKE ON CLUSTER `test` ALL ON *.* FROM `alice`",
		"GRANT ON CLUSTER `test` ALL ON *.* TO `alice`",
		"REVOKE ON CLUSTER `test` ACCESS MANAGEMENT ON *.* FROM `alice`",
	}, stmts)
}

func TestBuildDropUserSQL(t *testing.T) {
	sql, err := BuildDropUserSQL("test", "alice")
	assert.NoError(t, err)
	assert.Equal(t, "DROP USER IF EXISTS `alice` ON CLUSTER `test`", sql)
}

func TestBuildListUsersSQL(t *testing.T) {
	sql, err := BuildListUsersSQL("")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT name FROM system.users ORDER BY name", sql)

	sql, err = BuildListUsersSQL("ali")
	assert.NoError(t, err)
	assert.Equal(t, "SELECT name FROM system.users WHERE name LIKE '%ali%' ORDER BY name", sql)

	_, err = BuildListUsersSQL("a;b")
	assert.Error(t, err)
}

func TestBuildCreateProfileSQL(t *testing.T) {
	req := model.RbacProfileCreateReq{
		Name:     "ro",
		Settings: map[string]interface{}{"max_threads": float64(8), "readonly": float64(1)},
	}
	sql, err := BuildCreateProfileSQL("test", req)
	assert.NoError(t, err)
	// keys 排序保证确定性:max_threads 在 readonly 前
	assert.Equal(t, "CREATE SETTINGS PROFILE `ro` ON CLUSTER `test` SETTINGS max_threads = 8, readonly = 1", sql)
}

func TestBuildCreateProfileSQL_InvalidSettingKey(t *testing.T) {
	req := model.RbacProfileCreateReq{Name: "ro", Settings: map[string]interface{}{"bad key": float64(1)}}
	_, err := BuildCreateProfileSQL("test", req)
	assert.Error(t, err)
}

func TestBuildDropProfileSQL(t *testing.T) {
	sql, err := BuildDropProfileSQL("test", "ro")
	assert.NoError(t, err)
	assert.Equal(t, "DROP SETTINGS PROFILE IF EXISTS `ro` ON CLUSTER `test`", sql)
}

func TestDeriveAccess(t *testing.T) {
	assert.Equal(t, model.RbacAccessReadOnly, DeriveAccess([]string{"SELECT"}))
	assert.Equal(t, model.RbacAccessReadWrite, DeriveAccess([]string{"SELECT", "INSERT"}))
	assert.Equal(t, model.RbacAccessReadWrite, DeriveAccess([]string{"ALTER DELETE"}))
}

func TestIsReservedRbacUser(t *testing.T) {
	assert.True(t, IsReservedRbacUser("default"))
	assert.True(t, IsReservedRbacUser("DEFAULT"))
	assert.False(t, IsReservedRbacUser("alice"))
}
```

- [ ] **Step 3: 运行测试确认失败**

Run: `go test ./service/clickhouse/ -run TestBuild -v`
Expected: 编译失败(`undefined: BuildCreateUserSQL` 等)

- [ ] **Step 4: 写 service/clickhouse/rbac.go 的纯函数部分**

```go
package clickhouse

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/housepower/ckman/model"
)

var rbacIdentRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func validRbacIdent(s string) bool { return rbacIdentRe.MatchString(s) }

// escapeSQLString 转义 CH 字符串字面量
func escapeSQLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}

var rbacReservedUsers = map[string]bool{"default": true}

func IsReservedRbacUser(name string) bool {
	return rbacReservedUsers[strings.ToLower(name)]
}

// rbacWritePrivileges 出现任一即判定为 readwrite
var rbacWritePrivileges = []string{"INSERT", "ALTER", "CREATE", "DROP", "TRUNCATE", "OPTIMIZE", "DELETE", "UPDATE"}

// DeriveAccess 依据 grants 权限列表反推 readonly/readwrite
func DeriveAccess(grants []string) string {
	for _, g := range grants {
		gu := strings.ToUpper(g)
		for _, w := range rbacWritePrivileges {
			if strings.Contains(gu, w) {
				return model.RbacAccessReadWrite
			}
		}
	}
	return model.RbacAccessReadOnly
}

func buildGrantSQL(cluster, name, access string) []string {
	switch access {
	case model.RbacAccessReadOnly:
		return []string{fmt.Sprintf("GRANT ON CLUSTER `%s` SELECT ON *.* TO `%s`", cluster, name)}
	case model.RbacAccessReadWrite:
		return []string{
			fmt.Sprintf("GRANT ON CLUSTER `%s` ALL ON *.* TO `%s`", cluster, name),
			fmt.Sprintf("REVOKE ON CLUSTER `%s` ACCESS MANAGEMENT ON *.* FROM `%s`", cluster, name),
		}
	}
	return nil
}

func BuildCreateUserSQL(cluster string, req model.RbacUserCreateReq) ([]string, error) {
	if !validRbacIdent(req.Name) {
		return nil, fmt.Errorf("invalid user name: %s", req.Name)
	}
	if req.Password == "" {
		return nil, fmt.Errorf("password is required")
	}
	if req.Access != model.RbacAccessReadOnly && req.Access != model.RbacAccessReadWrite {
		return nil, fmt.Errorf("invalid access: %s", req.Access)
	}
	nets := req.Networks
	if len(nets) == 0 {
		nets = []string{"::/0"}
	}
	hostParts := make([]string, 0, len(nets))
	for _, n := range nets {
		hostParts = append(hostParts, fmt.Sprintf("IP '%s'", escapeSQLString(n)))
	}
	createSQL := fmt.Sprintf("CREATE USER `%s` ON CLUSTER `%s` IDENTIFIED BY '%s' HOST %s",
		req.Name, cluster, escapeSQLString(req.Password), strings.Join(hostParts, ", "))
	if req.Profile != "" {
		if !validRbacIdent(req.Profile) {
			return nil, fmt.Errorf("invalid profile name: %s", req.Profile)
		}
		createSQL += fmt.Sprintf(" SETTINGS PROFILE `%s`", req.Profile)
	}
	stmts := []string{createSQL}
	stmts = append(stmts, buildGrantSQL(cluster, req.Name, req.Access)...)
	return stmts, nil
}

func BuildAlterUserSQL(cluster, name string, req model.RbacUserAlterReq) ([]string, error) {
	if !validRbacIdent(name) {
		return nil, fmt.Errorf("invalid user name: %s", name)
	}
	var stmts []string
	if req.Password != "" {
		stmts = append(stmts, fmt.Sprintf("ALTER USER `%s` ON CLUSTER `%s` IDENTIFIED BY '%s'",
			name, cluster, escapeSQLString(req.Password)))
	}
	if req.Profile != "" {
		if !validRbacIdent(req.Profile) {
			return nil, fmt.Errorf("invalid profile name: %s", req.Profile)
		}
		stmts = append(stmts, fmt.Sprintf("ALTER USER `%s` ON CLUSTER `%s` SETTINGS PROFILE `%s`",
			name, cluster, req.Profile))
	}
	if req.Access != "" {
		if req.Access != model.RbacAccessReadOnly && req.Access != model.RbacAccessReadWrite {
			return nil, fmt.Errorf("invalid access: %s", req.Access)
		}
		stmts = append(stmts, fmt.Sprintf("REVOKE ON CLUSTER `%s` ALL ON *.* FROM `%s`", cluster, name))
		stmts = append(stmts, buildGrantSQL(cluster, name, req.Access)...)
	}
	if len(stmts) == 0 {
		return nil, fmt.Errorf("nothing to alter")
	}
	return stmts, nil
}

func BuildDropUserSQL(cluster, name string) (string, error) {
	if !validRbacIdent(name) {
		return "", fmt.Errorf("invalid user name: %s", name)
	}
	return fmt.Sprintf("DROP USER IF EXISTS `%s` ON CLUSTER `%s`", name, cluster), nil
}

func BuildListUsersSQL(nameFilter string) (string, error) {
	sql := "SELECT name FROM system.users"
	if nameFilter != "" {
		if !validRbacIdent(nameFilter) {
			return "", fmt.Errorf("invalid name filter: %s", nameFilter)
		}
		sql += fmt.Sprintf(" WHERE name LIKE '%%%s%%'", nameFilter)
	}
	sql += " ORDER BY name"
	return sql, nil
}

func formatSettingValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", escapeSQLString(val))
	case bool:
		if val {
			return "1"
		}
		return "0"
	case float64:
		if val == float64(int64(val)) {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func buildProfileSettingsClause(settings map[string]interface{}) (string, error) {
	if len(settings) == 0 {
		return "", nil
	}
	keys := make([]string, 0, len(settings))
	for k := range settings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		if !validRbacIdent(k) {
			return "", fmt.Errorf("invalid setting key: %s", k)
		}
		parts = append(parts, fmt.Sprintf("%s = %s", k, formatSettingValue(settings[k])))
	}
	return " SETTINGS " + strings.Join(parts, ", "), nil
}

func BuildCreateProfileSQL(cluster string, req model.RbacProfileCreateReq) (string, error) {
	if !validRbacIdent(req.Name) {
		return "", fmt.Errorf("invalid profile name: %s", req.Name)
	}
	clause, err := buildProfileSettingsClause(req.Settings)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("CREATE SETTINGS PROFILE `%s` ON CLUSTER `%s`%s", req.Name, cluster, clause), nil
}

func BuildAlterProfileSQL(cluster string, req model.RbacProfileCreateReq) (string, error) {
	if !validRbacIdent(req.Name) {
		return "", fmt.Errorf("invalid profile name: %s", req.Name)
	}
	clause, err := buildProfileSettingsClause(req.Settings)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ALTER SETTINGS PROFILE `%s` ON CLUSTER `%s`%s", req.Name, cluster, clause), nil
}

func BuildDropProfileSQL(cluster, name string) (string, error) {
	if !validRbacIdent(name) {
		return "", fmt.Errorf("invalid profile name: %s", name)
	}
	return fmt.Sprintf("DROP SETTINGS PROFILE IF EXISTS `%s` ON CLUSTER `%s`", name, cluster), nil
}

func BuildListProfilesSQL(nameFilter string) (string, error) {
	sql := "SELECT name FROM system.settings_profiles"
	if nameFilter != "" {
		if !validRbacIdent(nameFilter) {
			return "", fmt.Errorf("invalid name filter: %s", nameFilter)
		}
		sql += fmt.Sprintf(" WHERE name LIKE '%%%s%%'", nameFilter)
	}
	sql += " ORDER BY name"
	return sql, nil
}
```

- [ ] **Step 5: 运行测试确认通过**

Run: `go test ./service/clickhouse/ -run 'TestBuild|TestDerive|TestIsReserved' -v`
Expected: 全部 PASS

- [ ] **Step 6: 提交**

```bash
git add model/rbac.go service/clickhouse/rbac.go service/clickhouse/rbac_test.go
git commit -m "feat(rbac): CH 用户/profile SQL 拼接纯函数与模型"
```

---

### Task 2: Service 执行/查询层

**Files:**
- Modify: `service/clickhouse/rbac.go`(追加执行/查询封装函数)
- Test: `service/clickhouse/rbac_test.go`(追加 `parseNameColumn` 单测)

**Interfaces:**
- Consumes:Task 1 的所有 `Build*SQL`、`DeriveAccess`、`IsReservedRbacUser`;`GetCkService`;`ck.Conn.Exec`;`ck.QueryInfo`。
- Produces(供 Task 3 使用):
  - `RbacCreateUser(clusterName string, req model.RbacUserCreateReq) error`
  - `RbacAlterUser(clusterName, name string, req model.RbacUserAlterReq) error`
  - `RbacDropUser(clusterName, name string) error`
  - `RbacListUsers(clusterName, nameFilter string) ([]model.RbacUserInfo, error)`
  - `RbacGetUser(clusterName, name string) (model.RbacUserInfo, error)`
  - `RbacCreateProfile(clusterName string, req model.RbacProfileCreateReq) error`
  - `RbacAlterProfile(clusterName string, req model.RbacProfileCreateReq) error`
  - `RbacDropProfile(clusterName, name string) error`
  - `RbacListProfiles(clusterName, nameFilter string) ([]model.RbacProfileInfo, error)`
  - `RbacGetProfile(clusterName, name string) (model.RbacProfileInfo, error)`

- [ ] **Step 1: 写 parseNameColumn 失败测试(追加到 rbac_test.go)**

```go
func TestParseNameColumn(t *testing.T) {
	// QueryInfo 返回:data[0] 是列名行,data[1:] 是数据
	data := [][]interface{}{
		{"name"},
		{"alice"},
		{"bob"},
	}
	assert.Equal(t, []string{"alice", "bob"}, parseNameColumn(data))
	assert.Empty(t, parseNameColumn([][]interface{}{{"name"}}))
	assert.Empty(t, parseNameColumn(nil))
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./service/clickhouse/ -run TestParseNameColumn -v`
Expected: FAIL(`undefined: parseNameColumn`)

- [ ] **Step 3: 追加执行/查询封装到 service/clickhouse/rbac.go**

```go
// parseNameColumn 从 QueryInfo 结果(data[0] 为列名行)提取第 0 列的字符串值
func parseNameColumn(data [][]interface{}) []string {
	if len(data) <= 1 {
		return nil
	}
	out := make([]string, 0, len(data)-1)
	for _, row := range data[1:] {
		if len(row) > 0 {
			out = append(out, fmt.Sprintf("%v", row[0]))
		}
	}
	return out
}

func rbacExec(clusterName string, stmts []string) error {
	ck, err := GetCkService(clusterName)
	if err != nil {
		return err
	}
	for _, s := range stmts {
		if err := ck.Conn.Exec(s); err != nil {
			return err
		}
	}
	return nil
}

func RbacCreateUser(clusterName string, req model.RbacUserCreateReq) error {
	ck, err := GetCkService(clusterName)
	if err != nil {
		return err
	}
	stmts, err := BuildCreateUserSQL(ck.Config.Cluster, req)
	if err != nil {
		return err
	}
	for _, s := range stmts {
		if err := ck.Conn.Exec(s); err != nil {
			return err
		}
	}
	return nil
}

func RbacAlterUser(clusterName, name string, req model.RbacUserAlterReq) error {
	if IsReservedRbacUser(name) {
		return fmt.Errorf("user %s is reserved and cannot be altered", name)
	}
	ck, err := GetCkService(clusterName)
	if err != nil {
		return err
	}
	stmts, err := BuildAlterUserSQL(ck.Config.Cluster, name, req)
	if err != nil {
		return err
	}
	for _, s := range stmts {
		if err := ck.Conn.Exec(s); err != nil {
			return err
		}
	}
	return nil
}

func RbacDropUser(clusterName, name string) error {
	if IsReservedRbacUser(name) {
		return fmt.Errorf("user %s is reserved and cannot be dropped", name)
	}
	ck, err := GetCkService(clusterName)
	if err != nil {
		return err
	}
	sql, err := BuildDropUserSQL(ck.Config.Cluster, name)
	if err != nil {
		return err
	}
	return ck.Conn.Exec(sql)
}

// userAccessAndProfile 查单个用户的 access 与绑定 profile
func userAccessAndProfile(ck *CkService, name string) (string, string) {
	access := model.RbacAccessReadOnly
	grantsSQL := fmt.Sprintf("SELECT access_type FROM system.grants WHERE user_name = '%s'", escapeSQLString(name))
	if data, err := ck.QueryInfo(grantsSQL); err == nil {
		access = DeriveAccess(parseNameColumn(data))
	}
	profile := ""
	profSQL := fmt.Sprintf("SELECT arrayStringConcat(settings_profiles, ',') FROM system.users WHERE name = '%s'", escapeSQLString(name))
	if data, err := ck.QueryInfo(profSQL); err == nil {
		if names := parseNameColumn(data); len(names) > 0 {
			profile = names[0]
		}
	}
	return access, profile
}

func RbacListUsers(clusterName, nameFilter string) ([]model.RbacUserInfo, error) {
	ck, err := GetCkService(clusterName)
	if err != nil {
		return nil, err
	}
	sql, err := BuildListUsersSQL(nameFilter)
	if err != nil {
		return nil, err
	}
	data, err := ck.QueryInfo(sql)
	if err != nil {
		return nil, err
	}
	names := parseNameColumn(data)
	out := make([]model.RbacUserInfo, 0, len(names))
	for _, n := range names {
		access, profile := userAccessAndProfile(ck, n)
		out = append(out, model.RbacUserInfo{Name: n, Access: access, Profile: profile})
	}
	return out, nil
}

func RbacGetUser(clusterName, name string) (model.RbacUserInfo, error) {
	ck, err := GetCkService(clusterName)
	if err != nil {
		return model.RbacUserInfo{}, err
	}
	access, profile := userAccessAndProfile(ck, name)
	return model.RbacUserInfo{Name: name, Access: access, Profile: profile}, nil
}

func RbacCreateProfile(clusterName string, req model.RbacProfileCreateReq) error {
	ck, err := GetCkService(clusterName)
	if err != nil {
		return err
	}
	sql, err := BuildCreateProfileSQL(ck.Config.Cluster, req)
	if err != nil {
		return err
	}
	return ck.Conn.Exec(sql)
}

func RbacAlterProfile(clusterName string, req model.RbacProfileCreateReq) error {
	ck, err := GetCkService(clusterName)
	if err != nil {
		return err
	}
	sql, err := BuildAlterProfileSQL(ck.Config.Cluster, req)
	if err != nil {
		return err
	}
	return ck.Conn.Exec(sql)
}

func RbacDropProfile(clusterName, name string) error {
	ck, err := GetCkService(clusterName)
	if err != nil {
		return err
	}
	sql, err := BuildDropProfileSQL(ck.Config.Cluster, name)
	if err != nil {
		return err
	}
	return ck.Conn.Exec(sql)
}

func RbacListProfiles(clusterName, nameFilter string) ([]model.RbacProfileInfo, error) {
	ck, err := GetCkService(clusterName)
	if err != nil {
		return nil, err
	}
	sql, err := BuildListProfilesSQL(nameFilter)
	if err != nil {
		return nil, err
	}
	data, err := ck.QueryInfo(sql)
	if err != nil {
		return nil, err
	}
	names := parseNameColumn(data)
	out := make([]model.RbacProfileInfo, 0, len(names))
	for _, n := range names {
		out = append(out, model.RbacProfileInfo{Name: n, Settings: profileSettings(ck, n)})
	}
	return out, nil
}

func RbacGetProfile(clusterName, name string) (model.RbacProfileInfo, error) {
	ck, err := GetCkService(clusterName)
	if err != nil {
		return model.RbacProfileInfo{}, err
	}
	return model.RbacProfileInfo{Name: name, Settings: profileSettings(ck, name)}, nil
}

// profileSettings 查 profile 的 setting 键值
func profileSettings(ck *CkService, name string) map[string]interface{} {
	out := map[string]interface{}{}
	sql := fmt.Sprintf("SELECT setting_name, value FROM system.settings_profile_elements WHERE profile_name = '%s' AND setting_name != ''", escapeSQLString(name))
	data, err := ck.QueryInfo(sql)
	if err != nil || len(data) <= 1 {
		return out
	}
	for _, row := range data[1:] {
		if len(row) >= 2 {
			out[fmt.Sprintf("%v", row[0])] = fmt.Sprintf("%v", row[1])
		}
	}
	return out
}
```

- [ ] **Step 4: 运行测试确认通过 + 编译**

Run: `go test ./service/clickhouse/ -run 'TestBuild|TestDerive|TestIsReserved|TestParseNameColumn' -v && go build ./service/...`
Expected: 测试 PASS,编译无错

- [ ] **Step 5: 提交**

```bash
git add service/clickhouse/rbac.go service/clickhouse/rbac_test.go
git commit -m "feat(rbac): service 执行与系统表查询封装"
```

---

### Task 3: Controller handler

**Files:**
- Create: `controller/rbac.go`

**Interfaces:**
- Consumes:Task 2 的所有 `Rbac*` service 函数;`clickhouse` 包;`model` 错误码;`controller.ClickHouseClusterPath`;`Wrapfunc`。
- Produces(供 Task 4 注册):`RBACController` 及方法 `CreateUser/ListUsers/GetUser/AlterUser/DropUser/CreateProfile/ListProfiles/GetProfile/AlterProfile/DropProfile`,构造器 `NewRBACController(wrapfunc Wrapfunc) *RBACController`。

- [ ] **Step 1: 写 controller/rbac.go**

```go
package controller

import (
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
)

type RBACController struct {
	wrapfunc Wrapfunc
}

func NewRBACController(wrapfunc Wrapfunc) *RBACController {
	return &RBACController{wrapfunc: wrapfunc}
}

// CreateUser godoc
// @Summary 创建CH用户
// @Tags rbac
// @Accept json
// @Param clusterName path string true "cluster name"
// @Param req body model.RbacUserCreateReq true "user"
// @Router /api/v1/ck/rbac/user/{clusterName} [post]
func (controller *RBACController) CreateUser(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	var req model.RbacUserCreateReq
	if err := c.ShouldBindJSON(&req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	if err := clickhouse.RbacCreateUser(clusterName, req); err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// ListUsers godoc
// @Summary 列出CH用户
// @Tags rbac
// @Param clusterName path string true "cluster name"
// @Param name query string false "name filter"
// @Router /api/v1/ck/rbac/user/{clusterName} [get]
func (controller *RBACController) ListUsers(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	users, err := clickhouse.RbacListUsers(clusterName, c.Query("name"))
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, users)
}

// GetUser godoc
// @Summary 查询单个CH用户
// @Tags rbac
// @Router /api/v1/ck/rbac/user/{clusterName}/{name} [get]
func (controller *RBACController) GetUser(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	user, err := clickhouse.RbacGetUser(clusterName, c.Param("name"))
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, user)
}

// AlterUser godoc
// @Summary 修改CH用户
// @Tags rbac
// @Router /api/v1/ck/rbac/user/{clusterName}/{name} [put]
func (controller *RBACController) AlterUser(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	var req model.RbacUserAlterReq
	if err := c.ShouldBindJSON(&req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	if err := clickhouse.RbacAlterUser(clusterName, c.Param("name"), req); err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// DropUser godoc
// @Summary 删除CH用户
// @Tags rbac
// @Router /api/v1/ck/rbac/user/{clusterName}/{name} [delete]
func (controller *RBACController) DropUser(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	if err := clickhouse.RbacDropUser(clusterName, c.Param("name")); err != nil {
		controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// CreateProfile godoc
// @Summary 创建settings profile
// @Tags rbac
// @Router /api/v1/ck/rbac/profile/{clusterName} [post]
func (controller *RBACController) CreateProfile(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	var req model.RbacProfileCreateReq
	if err := c.ShouldBindJSON(&req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	if err := clickhouse.RbacCreateProfile(clusterName, req); err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// ListProfiles godoc
// @Summary 列出settings profile
// @Tags rbac
// @Param name query string false "name filter"
// @Router /api/v1/ck/rbac/profile/{clusterName} [get]
func (controller *RBACController) ListProfiles(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	profiles, err := clickhouse.RbacListProfiles(clusterName, c.Query("name"))
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, profiles)
}

// GetProfile godoc
// @Summary 查询单个profile
// @Tags rbac
// @Router /api/v1/ck/rbac/profile/{clusterName}/{name} [get]
func (controller *RBACController) GetProfile(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	profile, err := clickhouse.RbacGetProfile(clusterName, c.Param("name"))
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, profile)
}

// AlterProfile godoc
// @Summary 修改profile
// @Tags rbac
// @Router /api/v1/ck/rbac/profile/{clusterName}/{name} [put]
func (controller *RBACController) AlterProfile(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	var req model.RbacProfileCreateReq
	if err := c.ShouldBindJSON(&req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	req.Name = c.Param("name")
	if err := clickhouse.RbacAlterProfile(clusterName, req); err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// DropProfile godoc
// @Summary 删除profile
// @Tags rbac
// @Router /api/v1/ck/rbac/profile/{clusterName}/{name} [delete]
func (controller *RBACController) DropProfile(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	if err := clickhouse.RbacDropProfile(clusterName, c.Param("name")); err != nil {
		controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}
```

- [ ] **Step 2: 编译确认**

Run: `go build ./controller/...`
Expected: 无错误

- [ ] **Step 3: 提交**

```bash
git add controller/rbac.go
git commit -m "feat(rbac): controller handler 层"
```

---

### Task 4: 路由注册 + swagger + 真库验证

**Files:**
- Modify: `router/v1.go`(在 `InitRouterV1` 内构造 controller 并注册 10 条路由)

**Interfaces:**
- Consumes:Task 3 的 `NewRBACController`、`RBACController` 方法;现有 `controller.ClickHouseClusterPath`、`WrapMsg`。

- [ ] **Step 1: 在 router/v1.go 注册路由**

在 `InitRouterV1` 中,`userController := controller.NewUserController(...)` 附近加入构造,并在路由注册区(其他 `groupV1.POST(...)` 附近)加入以下 10 行:

```go
	rbacController := controller.NewRBACController(WrapMsg)

	groupV1.POST(fmt.Sprintf("/ck/rbac/user/:%s", controller.ClickHouseClusterPath), rbacController.CreateUser)
	groupV1.GET(fmt.Sprintf("/ck/rbac/user/:%s", controller.ClickHouseClusterPath), rbacController.ListUsers)
	groupV1.GET(fmt.Sprintf("/ck/rbac/user/:%s/:name", controller.ClickHouseClusterPath), rbacController.GetUser)
	groupV1.PUT(fmt.Sprintf("/ck/rbac/user/:%s/:name", controller.ClickHouseClusterPath), rbacController.AlterUser)
	groupV1.DELETE(fmt.Sprintf("/ck/rbac/user/:%s/:name", controller.ClickHouseClusterPath), rbacController.DropUser)
	groupV1.POST(fmt.Sprintf("/ck/rbac/profile/:%s", controller.ClickHouseClusterPath), rbacController.CreateProfile)
	groupV1.GET(fmt.Sprintf("/ck/rbac/profile/:%s", controller.ClickHouseClusterPath), rbacController.ListProfiles)
	groupV1.GET(fmt.Sprintf("/ck/rbac/profile/:%s/:name", controller.ClickHouseClusterPath), rbacController.GetProfile)
	groupV1.PUT(fmt.Sprintf("/ck/rbac/profile/:%s/:name", controller.ClickHouseClusterPath), rbacController.AlterProfile)
	groupV1.DELETE(fmt.Sprintf("/ck/rbac/profile/:%s/:name", controller.ClickHouseClusterPath), rbacController.DropProfile)
```

> 注意:路由参数名 `:name` 与 `ClickHouseClusterPath` 不能冲突。确认 `ClickHouseClusterPath` 的值(通常为 `clusterName`),`:name` 独立无冲突。

- [ ] **Step 2: 整体编译**

Run: `go build ./...`
Expected: 无错误

- [ ] **Step 3: 全量单测**

Run: `go test ./service/clickhouse/ ./controller/... -run Rbac -v; go test ./service/clickhouse/ -run 'TestBuild|TestDerive|TestParse|TestIsReserved' -v`
Expected: PASS

- [ ] **Step 4: 重新生成 swagger**

Run: `swag init`
Expected: 生成成功,无报错

- [ ] **Step 5: 提交**

```bash
git add router/v1.go docs/
git commit -m "feat(rbac): 注册路由并更新 swagger"
```

- [ ] **Step 6: 真实集群手动验证(需要一个可连的 CH 集群)**

启动 ckman,取一个已管理的集群名 `<cluster>` 与 token,逐条验证:

```bash
# 建只读用户
curl -s -X POST "http://127.0.0.1:8808/api/v1/ck/rbac/user/<cluster>" \
  -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
  -d '{"name":"ro_alice","password":"123456","access":"readonly"}'

# 建 profile
curl -s -X POST "http://127.0.0.1:8808/api/v1/ck/rbac/profile/<cluster>" \
  -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
  -d '{"name":"p_small","settings":{"max_threads":4,"max_memory_usage":2000000000}}'

# 建读写用户并绑 profile
curl -s -X POST "http://127.0.0.1:8808/api/v1/ck/rbac/user/<cluster>" \
  -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
  -d '{"name":"rw_bob","password":"123456","access":"readwrite","profile":"p_small"}'

# 列用户(按名称筛选)
curl -s "http://127.0.0.1:8808/api/v1/ck/rbac/user/<cluster>?name=bob" -H "Authorization: Bearer <token>"

# 列 profile
curl -s "http://127.0.0.1:8808/api/v1/ck/rbac/profile/<cluster>" -H "Authorization: Bearer <token>"

# 删用户
curl -s -X DELETE "http://127.0.0.1:8808/api/v1/ck/rbac/user/<cluster>/ro_alice" -H "Authorization: Bearer <token>"
```

到 CH 各节点执行 `SHOW USERS`、`SHOW CREATE USER rw_bob`、`SHOW GRANTS FOR rw_bob`,确认:
- `rw_bob` 有 `GRANT ... ON *.*` 但**不含** `ACCESS MANAGEMENT`;
- profile 已绑定;
- 多节点均可见(验证 `ON CLUSTER` 生效);
- 尝试 `DELETE .../user/<cluster>/default` 应被拒绝(保留名保护)。

Expected: 全部符合预期。

## Self-Review

- **Spec coverage**:创建 readonly/readwrite 用户(Task 1/2/3)、创建 profile(Task 1/2/3)、绑定 profile(create 时 `profile` 字段 + alter user 的 profile)、user/profile 各自 CRUD(Task 2/3)、List 名称筛选(Task 1 `Build*ListSQL` + Task 3 `c.Query("name")`)、ON CLUSTER(所有 Build 函数)、保留名保护(Task 2 `IsReservedRbacUser`)、防注入(Task 1 `validRbacIdent`/`escapeSQLString`)、零持久层改动(无 repository 触碰)。均有对应任务。
- **Placeholder scan**:无 TBD/TODO;所有代码步骤含完整代码。
- **Type consistency**:`BuildCreateUserSQL` 等函数签名在 Task 1 定义、Task 2 消费一致;`RbacUserInfo`/`RbacProfileInfo` 字段在 model 定义、service 返回、controller 透传一致;`parseNameColumn` 在 Task 2 定义并被复用。
