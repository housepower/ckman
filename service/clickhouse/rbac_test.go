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
		"CREATE USER OR REPLACE `alice` ON CLUSTER `test` IDENTIFIED BY 'p@ss' HOST IP '::/0' SETTINGS PROFILE `ro`",
		"GRANT ON CLUSTER `test` SELECT ON *.* TO `alice`",
	}, stmts.Cluster)
	assert.Empty(t, stmts.PerNode, "readonly 不用 CURRENT GRANTS, 无需逐节点执行")
}

func TestBuildCreateUserSQL_ReadWrite(t *testing.T) {
	req := model.RbacUserCreateReq{Name: "bob", Password: "x", Access: model.RbacAccessReadWrite}
	stmts, err := BuildCreateUserSQL("test", req)
	assert.NoError(t, err)
	assert.Equal(t, []string{
		"CREATE USER OR REPLACE `bob` ON CLUSTER `test` IDENTIFIED BY 'x' HOST IP '::/0'",
	}, stmts.Cluster)
	// CURRENT GRANTS 不能与 ON CLUSTER 共存(CH 报 "GRANT CURRENT GRANTS can't be
	// executed on cluster"), 故不带 ON CLUSTER、由调用方逐节点执行。
	// 用 CURRENT GRANTS 而非 ALL: 授出的是引导账户自己持有的权限,
	// 不会因 default 缺 NAMED_COLLECTION_ADMIN 之类而整条失败。
	assert.Equal(t, []string{
		"GRANT CURRENT GRANTS ON *.* TO `bob`",
		"REVOKE ACCESS MANAGEMENT ON *.* FROM `bob`",
	}, stmts.PerNode)
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
	assert.Contains(t, stmts.Cluster[0], `IDENTIFIED BY 'a\'b'`)
}

func TestBuildAlterUserSQL(t *testing.T) {
	stmts, err := BuildAlterUserSQL("test", "alice", model.RbacUserAlterReq{Password: "np", Profile: "p2", Access: "readwrite"})
	assert.NoError(t, err)
	assert.Equal(t, []string{
		"ALTER USER `alice` ON CLUSTER `test` IDENTIFIED BY 'np'",
		"ALTER USER `alice` ON CLUSTER `test` SETTINGS PROFILE `p2`",
		"REVOKE ON CLUSTER `test` ALL ON *.* FROM `alice`",
	}, stmts.Cluster)
	// 集群级语句先执行完再执行节点级, 故 REVOKE ALL 一定早于 GRANT
	assert.Equal(t, []string{
		"GRANT CURRENT GRANTS ON *.* TO `alice`",
		"REVOKE ACCESS MANAGEMENT ON *.* FROM `alice`",
	}, stmts.PerNode)
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
	assert.Equal(t, "CREATE SETTINGS PROFILE OR REPLACE `ro` ON CLUSTER `test` SETTINGS max_threads = 8, readonly = 1", sql)
}

func TestBuildCreateProfileSQL_InvalidSettingKey(t *testing.T) {
	req := model.RbacProfileCreateReq{Name: "ro", Settings: map[string]interface{}{"bad key": float64(1)}}
	_, err := BuildCreateProfileSQL("test", req)
	assert.Error(t, err)
}

func TestBuildCreateProfileSQL_UnsupportedValueType(t *testing.T) {
	req := model.RbacProfileCreateReq{Name: "p", Settings: map[string]interface{}{"k": []interface{}{"a", "b"}}}
	_, err := BuildCreateProfileSQL("test", req)
	assert.Error(t, err)
}

func TestBuildAlterProfileSQL_Valid(t *testing.T) {
	req := model.RbacProfileCreateReq{Name: "p", Settings: map[string]interface{}{"max_threads": float64(4)}}
	sql, err := BuildAlterProfileSQL("test", req)
	assert.NoError(t, err)
	assert.Equal(t, "ALTER SETTINGS PROFILE `p` ON CLUSTER `test` SETTINGS max_threads = 4", sql)
}

func TestBuildAlterProfileSQL_EmptySettings(t *testing.T) {
	_, err := BuildAlterProfileSQL("test", model.RbacProfileCreateReq{Name: "p"})
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

func TestDeriveAccess_RealisticInputs(t *testing.T) {
	assert.Equal(t, model.RbacAccessReadWrite, DeriveAccess([]string{"ALL"}))
	assert.Equal(t, model.RbacAccessReadWrite, DeriveAccess([]string{"SELECT", "INSERT", "ALTER", "CREATE", "DROP"}))
	assert.Equal(t, model.RbacAccessReadOnly, DeriveAccess([]string{"SELECT"}))
}

func TestIsReservedRbacUser(t *testing.T) {
	assert.True(t, IsReservedRbacUser("default"))
	assert.True(t, IsReservedRbacUser("DEFAULT"))
	assert.False(t, IsReservedRbacUser("alice"))
}

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

func TestRequireRbacReadWriteVersion(t *testing.T) {
	// readwrite 依赖 GRANT CURRENT GRANTS(23.5+)，低版本要拦住
	assert.Error(t, RequireRbacReadWriteVersion("22.8.1.2", model.RbacAccessReadWrite))
	assert.Error(t, RequireRbacReadWriteVersion("23.4.9", model.RbacAccessReadWrite))
	assert.NoError(t, RequireRbacReadWriteVersion("23.5.1", model.RbacAccessReadWrite))
	assert.NoError(t, RequireRbacReadWriteVersion("24.3.2.1", model.RbacAccessReadWrite))
	// readonly 走 GRANT SELECT，任何版本放行
	assert.NoError(t, RequireRbacReadWriteVersion("20.4.0", model.RbacAccessReadOnly))
	// 空 access(alter 不改权限)放行
	assert.NoError(t, RequireRbacReadWriteVersion("22.8.1", ""))
}
