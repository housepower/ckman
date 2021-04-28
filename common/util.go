package common

import (
	"database/sql"
	"fmt"
	"github.com/housepower/ckman/log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
)

func GetWorkDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return ""
	}

	return strings.Replace(filepath.Dir(dir), "\\", "/", -1)
}

func VerifyPassword(pwd string) error {
	var hasNumber, hasUpperCase, hasLowercase, hasSpecial bool

	if len(pwd) < 8 {
		return errors.Errorf("password is only %d characters long", len(pwd))
	}

	for _, c := range pwd {
		switch {
		case unicode.IsNumber(c):
			hasNumber = true
		case unicode.IsUpper(c):
			hasUpperCase = true
		case unicode.IsLower(c):
			hasLowercase = true
		case unicode.IsPunct(c) || unicode.IsSymbol(c):
			hasSpecial = true
		}
	}

	typeNum := 0
	if hasNumber {
		typeNum++
	}
	if hasLowercase {
		typeNum++
	}
	if hasUpperCase {
		typeNum++
	}
	if hasSpecial {
		typeNum++
	}

	if typeNum < 3 {
		return errors.Errorf("password don't contain at least three character categories")
	}

	return nil
}

func HashPassword(pwd string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(pwd), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

func ComparePassword(hashedPwd string, plainPwd string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(hashedPwd), []byte(plainPwd))
	if err != nil {
		return false
	}
	return true
}

func EnvStringVar(value *string, key string) {
	realKey := strings.ReplaceAll(strings.ToUpper(key), "-", "_")
	val, found := os.LookupEnv(realKey)
	if found {
		*value = val
	}
}

func EnvIntVar(value *int, key string) {
	realKey := strings.ReplaceAll(strings.ToUpper(key), "-", "_")
	val, found := os.LookupEnv(realKey)
	if found {
		valInt, err := strconv.Atoi(val)
		if err == nil {
			*value = valInt
		}
	}
}

func EnvBoolVar(value *bool, key string) {
	realKey := strings.ReplaceAll(strings.ToUpper(key), "-", "_")
	_, found := os.LookupEnv(realKey)
	if found {
		*value = true
	}
}

func ConnectClickHouse(host string, port int, database string, user string, password string) (*sql.DB, error) {
	var db *sql.DB
	var err error
	dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
		host, port, url.QueryEscape(database), url.QueryEscape(user), url.QueryEscape(password))
	if db, err = sql.Open("clickhouse", dsn); err != nil {
		err = errors.Wrapf(err, "")
		return nil, err
	}
	return db, nil
}

func GetMergeTreeTables(engine string, db *sql.DB) ([]string, map[string][]string, error) {
	var rows *sql.Rows
	var databases []string
	var err error
	dbtables := make(map[string][]string)
	query := fmt.Sprintf("SELECT DISTINCT  database, name FROM system.tables WHERE (match(engine, '%s')) AND (database != 'system') ORDER BY database", engine)
	log.Logger.Debugf("query: %s", query)
	if rows, err = db.Query(query); err != nil {
		err = errors.Wrapf(err, "")
		return nil, nil, err
	}
	defer rows.Close()
	var tables []string
	var predbname string
	for rows.Next() {
		var database, name string
		if err = rows.Scan(&database, &name); err != nil {
			err = errors.Wrapf(err, "")
			return nil, nil, err
		}
		if database != predbname {
			if predbname != "" {
				dbtables[predbname] = tables
				databases = append(databases, predbname)
			}
			tables = []string{}
		}
		tables = append(tables, name)
		predbname = database
	}
	if predbname != "" {
		dbtables[predbname] = tables
		databases = append(databases, predbname)
	}
	return databases, dbtables, nil
}
