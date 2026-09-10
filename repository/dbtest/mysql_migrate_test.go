//go:build dbintegration

// MySQL 建表/迁移的幂等性回归。
//
// 事故背景(2025-11 现场,ckman 3.4.3 + MySQL 5.7.27):重启报
//
//	Error 1005 (HY000): Can't create table '#sql-ac90_7b05' (errno: 28)
//	sql: ALTER TABLE `tbl_cluster` MODIFY COLUMN `cluster_name` varchar(191)
//
// errno 28 是 ENOSPC(MySQL 所在磁盘满),但真正的问题是「为什么每次重启都要 ALTER」:
// gorm v1.23.8 的 smart migrate 逐列比对时有两个稳定误判,导致首次建库之后每次启动都
// 无条件重发 6 条 ALTER TABLE ... MODIFY COLUMN(MySQL 5.7 上多走 copy 算法,要建
// #sql-xxx 临时表):
//
//   - `index:xx,unique` 只设置索引的唯一性,不设置 field.Unique;而 mysql driver 从
//     information_schema 读到 column_key='UNI' 判定列 unique=true → 二者永远不等。
//   - bool 字段 FullDataTypeOf 得到 "boolean",MySQL 实际存 tinyint(1),
//     DatabaseTypeName 返回 "tinyint" → 前缀比对永远不等。
//
// 于是磁盘一满、或任何一次 ALTER 失败,Init 就返回错误、main.go FATAL,ckman 直接
// 起不来 —— 典型的「首次建库能过、重启必崩」。dm8/postgres 早已各自绕开 AutoMigrate,
// 只剩 MySQL 还在用。
//
// 这个测试用 SHOW GLOBAL STATUS 的 Com_alter_table / Com_create_table 计数器,断言
// 「表建好之后再连启,不产生任何 DDL」。
package dbtest

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/housepower/ckman/repository/mysql"
)

func mysqlCfgOrSkip(t *testing.T) mysql.MysqlConfig {
	t.Helper()
	host := os.Getenv("DBTEST_MYSQL_HOST")
	if host == "" {
		t.Skip("DBTEST_MYSQL_HOST 未设置,跳过 MySQL 迁移幂等性测试")
	}
	return mysql.MysqlConfig{
		Host: host, Port: envInt("DBTEST_MYSQL_PORT", 3306),
		User: os.Getenv("DBTEST_MYSQL_USER"), Password: os.Getenv("DBTEST_MYSQL_PASS"),
		DataBase: os.Getenv("DBTEST_MYSQL_DB"),
	}
}

var ddlCounterNames = []string{"Com_alter_table", "Com_create_table", "Com_create_index"}

// rawDB 开一条绕过 ckman 持久层的原生连接,用来造老库结构和核对 information_schema。
func rawDB(t *testing.T, cfg mysql.MysqlConfig) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DataBase)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	return db
}

// ddlCounters 读取全局 DDL 计数器,用来判断一次 Init 到底发没发 DDL。
func ddlCounters(t *testing.T, cfg mysql.MysqlConfig) map[string]int {
	t.Helper()
	db := rawDB(t, cfg)
	defer db.Close()

	out := map[string]int{}
	// SHOW 语句不支持占位符,计数器名是代码内常量,直接拼。
	for _, name := range ddlCounterNames {
		var k string
		var v int
		if err := db.QueryRow("SHOW GLOBAL STATUS LIKE '" + name + "'").Scan(&k, &v); err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		out[name] = v
	}
	return out
}

// TestMysqlMigrateUpgradesOldSchema:老库(缺列、缺表)升级时该补的还得补,补完再启零 DDL。
// 防止「为了幂等而什么都不做」——那样新版本加的列在老库上永远不会出现。
func TestMysqlMigrateUpgradesOldSchema(t *testing.T) {
	cfg := mysqlCfgOrSkip(t)
	db := rawDB(t, cfg)
	defer db.Close()

	// 造一个老库现场:tbl_cluster 只有最早的几列(缺 config)、tbl_user 整张表不存在。
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS tbl_cluster",
		"DROP TABLE IF EXISTS tbl_user",
		"CREATE TABLE tbl_cluster (" +
			"id bigint unsigned AUTO_INCREMENT PRIMARY KEY," +
			"created_at datetime(3) NULL, updated_at datetime(3) NULL, deleted_at datetime(3) NULL," +
			"cluster_name varchar(191), UNIQUE INDEX idx_name (cluster_name), INDEX idx_tbl_cluster_deleted_at (deleted_at)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("准备老库结构失败 (%s): %v", stmt, err)
		}
	}

	if err := mysql.NewMysqlPersistent().Init(cfg); err != nil {
		t.Fatalf("老库升级 Init 失败: %v", err)
	}

	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.columns "+
		"WHERE table_schema = ? AND table_name = 'tbl_cluster' AND column_name = 'config'",
		cfg.DataBase).Scan(&n); err != nil {
		t.Fatalf("查 tbl_cluster.config: %v", err)
	}
	if n != 1 {
		t.Errorf("升级后 tbl_cluster 仍缺 config 列")
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.tables "+
		"WHERE table_schema = ? AND table_name = 'tbl_user'", cfg.DataBase).Scan(&n); err != nil {
		t.Fatalf("查 tbl_user: %v", err)
	}
	if n != 1 {
		t.Errorf("升级后仍缺 tbl_user 表")
	}

	// 补完之后再启,不该再有任何 DDL。
	before := ddlCounters(t, cfg)
	if err := mysql.NewMysqlPersistent().Init(cfg); err != nil {
		t.Fatalf("升级后再次 Init 失败: %v", err)
	}
	after := ddlCounters(t, cfg)
	for _, name := range ddlCounterNames {
		if delta := after[name] - before[name]; delta != 0 {
			t.Errorf("升级后再次 Init 产生了 %d 次 %s,应当零 DDL", delta, name)
		}
	}
}

// TestMysqlMigrateIsIdempotent:表结构就绪后再连启,必须零 DDL。
func TestMysqlMigrateIsIdempotent(t *testing.T) {
	cfg := mysqlCfgOrSkip(t)

	// 第一轮:建表(允许产生 DDL)。
	if err := mysql.NewMysqlPersistent().Init(cfg); err != nil {
		t.Fatalf("首次 Init 失败: %v", err)
	}

	// 之后每一轮都不该再动表结构。跑两轮,确认不是「第二次才收敛」。
	for round := 2; round <= 3; round++ {
		before := ddlCounters(t, cfg)
		if err := mysql.NewMysqlPersistent().Init(cfg); err != nil {
			t.Fatalf("第 %d 次 Init 失败: %v", round, err)
		}
		after := ddlCounters(t, cfg)
		for _, name := range ddlCounterNames {
			if delta := after[name] - before[name]; delta != 0 {
				t.Errorf("第 %d 次 Init 产生了 %d 次 %s,重启应当零 DDL", round, delta, name)
			}
		}
	}
}
