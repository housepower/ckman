package common

import (
	"crypto/sha1"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

var ConnectPool sync.Map

type Connection struct {
	dsn string
	db  *sql.DB
}

func ConnectClickHouse(host string, port int, database string, user string, password string) (*sql.DB, error) {
	var db *sql.DB
	var err error

	dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s&read_timeout=300",
		host, port, url.QueryEscape(database), url.QueryEscape(user), url.QueryEscape(password))

	if conn, ok := ConnectPool.Load(host); ok {
		c := conn.(Connection)
		err := c.db.Ping()
		if err == nil {
			if c.dsn == dsn {
				return c.db, nil
			} else {
				//dsn is different, maybe annother user, close connection before and reconnect
				_ = c.db.Close()
			}
		}
	}

	if db, err = sql.Open("clickhouse", dsn); err != nil {
		err = errors.Wrapf(err, "")
		return nil, err
	}

	if err = db.Ping(); err != nil {
		err = errors.Wrapf(err, "")
		return nil, err
	}
	SetConnOptions(db)
	connction := Connection{
		dsn: dsn,
		db:  db,
	}
	ConnectPool.Store(host, connction)
	return db, nil
}

func SetConnOptions(conn *sql.DB) {
	conn.SetMaxOpenConns(2)
	conn.SetMaxIdleConns(0)
	conn.SetConnMaxIdleTime(10 * time.Second)
}

func CloseConns(hosts []string) {
	for _, host := range hosts {
		conn, ok := ConnectPool.LoadAndDelete(host)
		if ok {
			_ = conn.(Connection).db.Close()
		}
	}
}

func GetConnection(host string) *sql.DB {
	if conn, ok := ConnectPool.Load(host); ok {
		db := conn.(Connection).db
		err := db.Ping()
		if err == nil {
			return db
		}
	}
	return nil
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

func GetShardAvaliableHosts(conf *model.CKManClickHouseConfig) ([]string, error) {
	var hosts []string
	var lastErr error

	for _, shard := range conf.Shards {
		for _, replica := range shard.Replicas {
			_, err := ConnectClickHouse(replica.Ip, conf.Port, model.ClickHouseDefaultDB, conf.User, conf.Password)
			if err == nil {
				hosts = append(hosts, replica.Ip)
				break
			} else {
				lastErr = err
			}
		}
	}
	if len(hosts) < len(conf.Shards) {
		log.Logger.Errorf("not all shard avaliable: %v", lastErr)
		return []string{}, nil
	}
	log.Logger.Debugf("hosts: %v", hosts)
	return hosts, nil
}

/*
	v1 == v2 return 0
	v1 > v2 return 1
	v1 < v2 return -1
*/
func CompareClickHouseVersion(v1, v2 string) int {
	s1 := strings.Split(v1, ".")
	s2 := strings.Split(v2, ".")
	for i := 0; i < len(s1); i++ {
		if len(s2) <= i {
			break
		}
		if s1[i] == "x" || s2[i] == "x" {
			continue
		}
		f1, _ := strconv.Atoi(s1[i])
		f2, _ := strconv.Atoi(s2[i])
		if f1 > f2 {
			return 1
		} else if f1 < f2 {
			return -1
		}
	}
	return 0
}

const (
	PLAINTEXT = iota
	SHA256_HEX
	DOUBLE_SHA1_HEX
)

//echo -n "cyc2010" |sha256sum |tr -d "-"
//a40943925ca51a95de7d39bc8c31757207d53b5e7114e695c04db63b6868f3e1
func sha256sum(plaintext string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(plaintext)))
}

//echo -n "cyc2010" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'
//812b8fad11eb25a3cf4cc2c54ae10a4948a0c25b
func hexsha1sum(plaintext string) string {
	sum := fmt.Sprintf("%x", sha1.Sum([]byte(plaintext)))
	xxd, _ := hex.DecodeString(sum)
	return fmt.Sprintf("%x", sha1.Sum(xxd))
}

func CkPassword(passwd string, algorithm int) string {
	var result string
	switch algorithm {
	case SHA256_HEX:
		result = sha256sum(passwd)
	case DOUBLE_SHA1_HEX:
		result = hexsha1sum(passwd)
	default:
		result = passwd
	}
	return result
}

func CkPasswdLabel(algorithm int) string {
	var result string
	switch algorithm {
	case SHA256_HEX:
		result = "password_sha256_hex"
	case DOUBLE_SHA1_HEX:
		result = "password_double_sha1_hex"
	default:
		result = "password"
	}
	return result
}
