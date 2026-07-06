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
