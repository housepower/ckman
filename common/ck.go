package common

import (
	"database/sql"
	"fmt"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ConnectPool sync.Map

type Connection struct {
	dsn string
	db  *sql.DB
}

func ConnectClickHouse(host string, port int, database string, user string, password string) (*sql.DB, error) {
	var db *sql.DB
	var err error

	dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
		host, port, url.QueryEscape(database), url.QueryEscape(user), url.QueryEscape(password))
	log.Logger.Debugf("dsn: %s", dsn)

	if conn, ok := ConnectPool.Load(host); ok {
		c := conn.(Connection)
		err := c.db.Ping()
		if err ==  nil{
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
		if err ==  nil {
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
func CompareClickHouseVersion(v1, v2 string)int {
	s1 := strings.Split(v1, ".")
	s2 := strings.Split(v2, ".")
	for i := 0; i < len(s1); i++ {
		if len(s2) <= i {
			break
		}
		if s1[i] == "x" || s2[i] == "x" {
			continue
		}
		f1,_ := strconv.Atoi(s1[i])
		f2,_ := strconv.Atoi(s2[i])
		if f1 > f2 {
			return 1
		} else if f1 < f2 {
			return -1
		}
	}
	return 0
}
