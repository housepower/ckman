package common

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

var ConnectPool sync.Map

const (
	ClickHouseLocalTablePrefix       string = "local_"
	ClickHouseDistributedTablePrefix string = "dist_"
	ClickHouseDistTableOnLogicPrefix string = "dist_logic_"
	ClickHouseLocalViewPrefix        string = "mv_"
	ClickHouseDistributedViewPrefix  string = "dist_mv_"
	ClickHouseLogicViewPrefix        string = "dist_logic_mv_"
	ClickHouseAggregateTablePrefix   string = "agg_"
	ClickHouseAggDistTablePrefix     string = "dist_agg_"
	ClickHouseAggLogicTablePrefix    string = "dist_logic_agg_"

	NetLoopBack string = "127.0.0.1"
)

type Connection struct {
	opts    clickhouse.Options
	orderId string
	conn    *Conn
}

type ConnBase struct {
	Addr []string
	Auth clickhouse.Auth
}

func ConnectClickHouse(host string, database string, opt model.ConnetOption) (*Conn, error) {
	opts := clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, opt.Port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: opt.User,
			Password: opt.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Protocol:         opt.Protocol,
		DialTimeout:      time.Duration(10) * time.Second,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
	}

	if opt.Secure {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: opt.InsecureSkipVerify,
		}
	}

	if opt.Protocol == clickhouse.Native {
		opts.MaxOpenConns = config.GlobalConfig.ClickHouse.MaxOpenConns
		opts.MaxIdleConns = config.GlobalConfig.ClickHouse.MaxIdleConns
		opts.ConnMaxLifetime = time.Duration(config.GlobalConfig.ClickHouse.ConnMaxIdleTime) * time.Minute
	}
	raw, err := json.Marshal(ConnBase{
		Addr: opts.Addr,
		Auth: opts.Auth,
	})
	if err != nil {
		return nil, err
	}
	hashInBytes := sha256.Sum256(raw)
	orderId := hex.EncodeToString(hashInBytes[:])
	if v, ok := ConnectPool.Load(host); ok {
		c := v.(Connection)
		err := c.conn.Ping()
		if err == nil {
			return c.conn, nil
			// } else {
			//_ = c.conn.Close()
		}
	}
	conn := Conn{
		protocol: opt.Protocol,
		ctx:      context.Background(),
	}
	if opt.Protocol == clickhouse.HTTP {
		db := clickhouse.OpenDB(&opts)
		err := db.Ping()
		if err != nil {
			db.Close()
			return nil, err
		}
		db.SetMaxOpenConns(config.GlobalConfig.ClickHouse.MaxOpenConns)
		db.SetMaxIdleConns(config.GlobalConfig.ClickHouse.MaxIdleConns)
		db.SetConnMaxLifetime(time.Duration(config.GlobalConfig.ClickHouse.ConnMaxIdleTime) * time.Minute)

		conn.db = db
	} else {
		c, err := clickhouse.Open(&opts)
		if err != nil {
			return nil, err
		}
		err = c.Ping(conn.ctx)
		if err != nil {
			c.Close()
			return nil, err
		}
		conn.c = c
	}
	ConnectPool.Store(host, Connection{
		opts:    opts,
		orderId: orderId,
		conn:    &conn,
	})
	return &conn, nil
}

func CloseConns(hosts []string) {
	for _, host := range hosts {
		conn, ok := ConnectPool.LoadAndDelete(host)
		if ok {
			_ = conn.(Connection).conn.Close()
		}
	}
}

func GetConnection(host string) *Conn {
	if v, ok := ConnectPool.Load(host); ok {
		conn := v.(Connection).conn
		err := conn.Ping()
		if err == nil {
			return conn
		} else {
			conn.Close()
		}
	}
	return nil
}

func GetMergeTreeTables(engine string, database string, conn *Conn) ([]string, map[string][]string, error) {
	var rows *Rows
	var databases []string
	var err error
	dbtables := make(map[string][]string)
	query := fmt.Sprintf("SELECT DISTINCT  database, name FROM system.tables WHERE (match(engine, '%s')) AND (database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA'))", engine)
	if database != "" {
		query += fmt.Sprintf(" AND database = '%s'", database)
	}
	query += " ORDER BY database"
	log.Logger.Debugf("query: %s", query)
	if rows, err = conn.Query(query); err != nil {
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
			_, err := ConnectClickHouse(replica.Ip, model.ClickHouseDefaultDB, conf.GetConnOption())
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

// echo -n "cyc2010" |sha256sum |tr -d "-"
// a40943925ca51a95de7d39bc8c31757207d53b5e7114e695c04db63b6868f3e1
func sha256sum(plaintext string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(plaintext)))
}

// echo -n "cyc2010" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'
// 812b8fad11eb25a3cf4cc2c54ae10a4948a0c25b
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

func CheckCkInstance(conf *model.CKManClickHouseConfig) error {
	for _, host := range conf.Hosts {
		cmd := "pidof clickhouse-server"
		sshOpts := SshOptions{
			User:             conf.SshUser,
			Password:         conf.SshPassword,
			Port:             conf.SshPort,
			Host:             host,
			NeedSudo:         conf.NeedSudo,
			AuthenticateType: conf.AuthenticateType,
		}
		result, err := RemoteExecute(sshOpts, cmd)
		if err == nil {
			if result != "" {
				return errors.Errorf("host %s already have another clickhouse-server running(pid = %s)", host, result)
			}
		}
	}
	return nil
}

func GetTableNames(conn *Conn, database, local, dist, cluster string, exists bool) (string, string, error) {
	if local == "" && dist == "" {
		return local, dist, fmt.Errorf("local table and dist table both empty")
	}
	if local != "" && dist != "" {
		return local, dist, nil
	}

	if exists {
		// will select distname or localname from database
		query := fmt.Sprintf(`SELECT
    name as dist,
    (extractAllGroups(engine_full, '(Distributed\\(\')(.*)\',\\s+\'(.*)\',\\s+\'(.*)\'(.*)')[1])[2] AS cluster,
    (extractAllGroups(engine_full, '(Distributed\\(\')(.*)\',\\s+\'(.*)\',\\s+\'(.*)\'(.*)')[1])[4] AS local
FROM system.tables
WHERE match(engine, 'Distributed') AND (database = '%s') AND ((dist = '%s') OR (local = '%s')) AND (cluster = '%s')`,
			database, dist, local, cluster)
		log.Logger.Debug(query)
		rows, err := conn.Query(query)
		if err != nil {
			return local, dist, err
		}
		for rows.Next() {
			rows.Scan(&dist, &cluster, &local)
		}
	} else {
		dist = TernaryExpression(local == "", dist, ClickHouseDistributedTablePrefix+local).(string)
		local = TernaryExpression(local == "", ClickHouseLocalTablePrefix+dist, local).(string)
	}

	return local, dist, nil
}

func ClikHouseExceptionDecode(err error) error {
	var e *clickhouse.Exception
	// TCP protocol
	if errors.As(err, &e) {
		return e
	}
	// HTTP protocol
	if strings.HasPrefix(err.Error(), "clickhouse [execute]::") {
		r := regexp.MustCompile(`.*Code:\s+(\d+)\.\s+(.*)`)
		matchs := r.FindStringSubmatch(err.Error())
		if len(matchs) != 3 {
			return err
		}
		code, err2 := strconv.Atoi(matchs[1])
		if err2 != nil {
			return err
		}
		message := matchs[2]
		e = &clickhouse.Exception{
			Code:    int32(code),
			Message: message,
		}
		return e
	}

	return err
}
