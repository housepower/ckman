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
