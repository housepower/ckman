package clickhouse

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

// rbacReadWriteMinVersion readwrite 用户依赖 GRANT ... CURRENT GRANTS 语法,该语法自 ClickHouse 23.5 起支持
const rbacReadWriteMinVersion = "23.5.x"

// RequireRbacReadWriteVersion 校验集群版本是否满足 readwrite 用户所需的 CURRENT GRANTS 语法(>= 23.5)。
// 仅 access=readwrite 时校验;readonly 与 profile 操作在低版本(20.4+)可用,不受此限制。
func RequireRbacReadWriteVersion(version, access string) error {
	if access != model.RbacAccessReadWrite {
		return nil
	}
	if common.CompareClickHouseVersion(version, rbacReadWriteMinVersion) < 0 {
		return fmt.Errorf("access 'readwrite' requires ClickHouse >= 23.5 (uses GRANT CURRENT GRANTS), but cluster version is %s", version)
	}
	return nil
}

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
var rbacWritePrivileges = []string{"ALL", "INSERT", "ALTER", "CREATE", "DROP", "TRUNCATE", "OPTIMIZE", "DELETE", "UPDATE"}

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
			fmt.Sprintf("GRANT ON CLUSTER `%s` CURRENT GRANTS ON *.* TO `%s`", cluster, name),
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
	// OR REPLACE: ON CLUSTER 下若个别节点残留同名用户(local_directory),普通 CREATE 会在该节点
	// 报 ACCESS_ENTITY_ALREADY_EXISTS(493) 导致集群状态不一致;OR REPLACE 强制各节点重建,保证一致。
	createSQL := fmt.Sprintf("CREATE USER OR REPLACE `%s` ON CLUSTER `%s` IDENTIFIED BY '%s' HOST %s",
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

func formatSettingValue(v interface{}) (string, error) {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", escapeSQLString(val)), nil
	case bool:
		if val {
			return "1", nil
		}
		return "0", nil
	case float64:
		if val == float64(int64(val)) {
			return strconv.FormatInt(int64(val), 10), nil
		}
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	default:
		return "", fmt.Errorf("unsupported setting value type %T", v)
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
		valStr, err := formatSettingValue(settings[k])
		if err != nil {
			return "", fmt.Errorf("setting %s: %w", k, err)
		}
		parts = append(parts, fmt.Sprintf("%s = %s", k, valStr))
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
	// OR REPLACE: 同 CreateUser,避免个别节点残留同名 profile 导致 ON CLUSTER 报 493、集群不一致。
	return fmt.Sprintf("CREATE SETTINGS PROFILE OR REPLACE `%s` ON CLUSTER `%s`%s", req.Name, cluster, clause), nil
}

func BuildAlterProfileSQL(cluster string, req model.RbacProfileCreateReq) (string, error) {
	if !validRbacIdent(req.Name) {
		return "", fmt.Errorf("invalid profile name: %s", req.Name)
	}
	if len(req.Settings) == 0 {
		return "", fmt.Errorf("nothing to alter: settings is empty")
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

func RbacCreateUser(clusterName string, req model.RbacUserCreateReq) error {
	ck, err := GetCkService(clusterName)
	if err != nil {
		return err
	}
	if err := RequireRbacReadWriteVersion(ck.Config.Version, req.Access); err != nil {
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
	if err := RequireRbacReadWriteVersion(ck.Config.Version, req.Access); err != nil {
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
	grantsSQL := fmt.Sprintf("SELECT access_type FROM system.grants WHERE user_name = '%s' AND is_partial_revoke = 0", escapeSQLString(name))
	if data, err := ck.QueryInfo(grantsSQL); err == nil {
		access = DeriveAccess(parseNameColumn(data))
	} else {
		log.Logger.Warnf("rbac: query grants for %s failed: %v", name, err)
	}
	profile := ""
	profSQL := fmt.Sprintf("SELECT inherit_profile FROM system.settings_profile_elements WHERE user_name = '%s' AND inherit_profile != ''", escapeSQLString(name))
	if data, err := ck.QueryInfo(profSQL); err == nil {
		if names := parseNameColumn(data); len(names) > 0 {
			profile = names[0]
		}
	} else {
		log.Logger.Warnf("rbac: query profile for %s failed: %v", name, err)
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
	exist, err := rbacUserExists(ck, name)
	if err != nil {
		return model.RbacUserInfo{}, err
	}
	if !exist {
		return model.RbacUserInfo{}, fmt.Errorf("user %s not found", name)
	}
	access, profile := userAccessAndProfile(ck, name)
	return model.RbacUserInfo{Name: name, Access: access, Profile: profile}, nil
}

// rbacUserExists 检查用户是否存在
func rbacUserExists(ck *CkService, name string) (bool, error) {
	data, err := ck.QueryInfo(fmt.Sprintf("SELECT name FROM system.users WHERE name = '%s'", escapeSQLString(name)))
	if err != nil {
		return false, err
	}
	return len(parseNameColumn(data)) > 0, nil
}

// rbacProfileExists 检查 profile 是否存在
func rbacProfileExists(ck *CkService, name string) (bool, error) {
	data, err := ck.QueryInfo(fmt.Sprintf("SELECT name FROM system.settings_profiles WHERE name = '%s'", escapeSQLString(name)))
	if err != nil {
		return false, err
	}
	return len(parseNameColumn(data)) > 0, nil
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
	exist, err := rbacProfileExists(ck, name)
	if err != nil {
		return model.RbacProfileInfo{}, err
	}
	if !exist {
		return model.RbacProfileInfo{}, fmt.Errorf("profile %s not found", name)
	}
	return model.RbacProfileInfo{Name: name, Settings: profileSettings(ck, name)}, nil
}

// profileSettings 查 profile 的 setting 键值
func profileSettings(ck *CkService, name string) map[string]interface{} {
	out := map[string]interface{}{}
	sql := fmt.Sprintf("SELECT setting_name, value FROM system.settings_profile_elements WHERE profile_name = '%s' AND setting_name != ''", escapeSQLString(name))
	data, err := ck.QueryInfo(sql)
	if err != nil {
		log.Logger.Warnf("rbac: query settings for profile %s failed: %v", name, err)
		return out
	}
	if len(data) <= 1 {
		return out
	}
	for _, row := range data[1:] {
		if len(row) >= 2 {
			out[fmt.Sprintf("%v", row[0])] = fmt.Sprintf("%v", row[1])
		}
	}
	return out
}
