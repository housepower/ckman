//go:build dbintegration

// Package dbtest 是持久化后端的真库集成测试。
//
// 这类 bug(达梦 -6118 零值时间 / -2116 重复加列 / -2140 重复建索引,以及 MySQL
// DATETIME 下界、PG 大小写等)只有「对着真库连启多次 + 真写零值/边界数据」才现形,
// mock 单测一律抓不到。故独立成一个带 build tag 的集成测试,平时不跑,给库就跑:
//
//	DBTEST_MYSQL_HOST=... DBTEST_MYSQL_USER=... DBTEST_MYSQL_PASS=... DBTEST_MYSQL_DB=... \
//	DBTEST_DM_HOST=...    DBTEST_DM_USER=...    DBTEST_DM_PASS=...    DBTEST_DM_SCHEMA=... \
//	DBTEST_PG_HOST=...    DBTEST_PG_USER=...    DBTEST_PG_PASS=...    DBTEST_PG_DB=... \
//	go test -tags dbintegration -v ./repository/dbtest/ -run TestBackends
//
// 未设置对应环境变量的后端会被 Skip。
package dbtest

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/repository/dm8"
	"github.com/housepower/ckman/repository/mysql"
	"github.com/housepower/ckman/repository/postgres"
)

func init() { log.InitLoggerConsole() }

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// backendCase 描述一个待测后端:如何造新实例、连接配置、是否配置齐全。
type backendCase struct {
	name string
	newFn func() repository.PersistentMgr
	cfg  interface{}
	ok   bool
}

func mysqlCase() backendCase {
	host := os.Getenv("DBTEST_MYSQL_HOST")
	return backendCase{
		name:  "mysql",
		newFn: func() repository.PersistentMgr { return mysql.NewMysqlPersistent() },
		cfg: mysql.MysqlConfig{
			Host: host, Port: envInt("DBTEST_MYSQL_PORT", 3306),
			User: os.Getenv("DBTEST_MYSQL_USER"), Password: os.Getenv("DBTEST_MYSQL_PASS"),
			DataBase: os.Getenv("DBTEST_MYSQL_DB"),
		},
		ok: host != "",
	}
}

func dmCase() backendCase {
	host := os.Getenv("DBTEST_DM_HOST")
	return backendCase{
		name:  "dm8",
		newFn: func() repository.PersistentMgr { return dm8.NewDM8Persistent() },
		cfg: dm8.DM8Config{
			Host: host, Port: envInt("DBTEST_DM_PORT", 5236),
			User: os.Getenv("DBTEST_DM_USER"), Password: os.Getenv("DBTEST_DM_PASS"),
			Schema: os.Getenv("DBTEST_DM_SCHEMA"),
		},
		ok: host != "",
	}
}

func pgCase() backendCase {
	host := os.Getenv("DBTEST_PG_HOST")
	return backendCase{
		name:  "postgres",
		newFn: func() repository.PersistentMgr { return postgres.NewPostgresPersistent() },
		cfg: postgres.PostgresConfig{
			Host: host, Port: envInt("DBTEST_PG_PORT", 5432),
			User: os.Getenv("DBTEST_PG_USER"), Password: os.Getenv("DBTEST_PG_PASS"),
			DataBase: os.Getenv("DBTEST_PG_DB"),
		},
		ok: host != "",
	}
}

// TestBackends 对每个配置齐全的后端跑一遍完整套路。
func TestBackends(t *testing.T) {
	for _, bc := range []backendCase{mysqlCase(), dmCase(), pgCase()} {
		bc := bc
		t.Run(bc.name, func(t *testing.T) {
			if !bc.ok {
				t.Skipf("%s 环境变量未配置,跳过", bc.name)
			}
			runBackend(t, bc)
		})
	}
}

func runBackend(t *testing.T, bc backendCase) {
	// ── 雷点 1:连启多次,AutoMigrate 幂等性(达梦 -2116 重复加列 / -2140 重复建索引)──
	// 首次建表 OK 不代表没问题;这类 bug 只在第 2/3 次启动重跑 AutoMigrate 时爆。
	for i := 1; i <= 3; i++ {
		m := bc.newFn()
		if err := m.Init(bc.cfg); err != nil {
			t.Fatalf("boot#%d Init 失败(AutoMigrate 非幂等?): %v", i, err)
		}
	}

	mgr := bc.newFn()
	if err := mgr.Init(bc.cfg); err != nil {
		t.Fatalf("Init 失败: %v", err)
	}
	// 让内部依赖 repository.Ps 的代码路径也指向被测实例
	repository.Ps = mgr

	t.Run("cluster_crud", func(t *testing.T) { testClusterCRUD(t, mgr) })
	t.Run("cluster_tls_conf", func(t *testing.T) { testClusterTLSConf(t, mgr) })
	t.Run("backup_policy_crud", func(t *testing.T) { testPolicyCRUD(t, mgr) })
	t.Run("backup_run_zero_time", func(t *testing.T) { testBackupRunZeroTime(t, mgr) })
	t.Run("user_crud", func(t *testing.T) { testUserCRUD(t, mgr) })
	t.Run("query_history", func(t *testing.T) { testQueryHistory(t, mgr) })
	t.Run("task_crud", func(t *testing.T) { testTaskCRUD(t, mgr) })
}

const tagPrefix = "dbtest_"

func testClusterCRUD(t *testing.T, mgr repository.PersistentMgr) {
	name := tagPrefix + "cluster"
	_ = mgr.DeleteCluster(name)
	conf := model.CKManClickHouseConfig{
		Cluster: name, Port: 9000, User: "default", Password: "secret",
		Hosts: []string{"1.2.3.4"},
	}
	if err := mgr.CreateCluster(conf); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	defer mgr.DeleteCluster(name)

	got, err := mgr.GetClusterbyName(name)
	if err != nil {
		t.Fatalf("GetClusterbyName: %v", err)
	}
	if got.Cluster != name {
		t.Fatalf("round-trip cluster name = %q", got.Cluster)
	}
	conf.Port = 9440
	if err := mgr.UpdateCluster(conf); err != nil {
		t.Fatalf("UpdateCluster: %v", err)
	}
	if got, _ = mgr.GetClusterbyName(name); got.Port != 9440 {
		t.Fatalf("update not persisted, port = %d", got.Port)
	}
}

// testClusterTLSConf 是 ClickHouse TLS 特性动到持久层的核心雷点:
//
//   - repository/helper.go 的 EncodePasswd/DecodePasswd 新增了私钥 base64+AES 加解密
//     （PEM 私钥里的 \n 会被 common.AesDecryptECB 的去填充逻辑当作截断点，用 base64
//     包裹绕开；这条编码往返必须在真库上守住，mock 单测测的是内存里的函数调用，
//     测不出"写进 JSON 列再读出来"这一层）
//   - model.CKManClickHouseConfig 新增 TlsConf 字段，走的是整份 config 序列化成
//     JSON blob 落到 tbl_cluster.config（DM8 是 Configuration Clob）这条老路径，
//     字段新增本身也可能因为列容量、JSON 序列化边界条件出问题
//
// 覆盖点：1) 写入真实 PEM（含换行符）2) 读回 Cert/Key 逐字节一致 3) 库里 Key 是
// 密文、Cert 是明文 4) 换证书后再次往返一致 5) TlsConf=nil 的明文集群不受影响。
func testClusterTLSConf(t *testing.T, mgr repository.PersistentMgr) {
	name := tagPrefix + "tls_cluster"
	_ = mgr.DeleteCluster(name)

	certPEM, keyPEM, err := common.GenSelfSignedCert([]string{"192.168.0.1"})
	if err != nil {
		t.Fatalf("GenSelfSignedCert: %v", err)
	}
	// 换行符是这条编码路径要守住的关键雷点，样本必须真的含有换行符，否则测试
	// 本身就测不到 AesDecryptECB 截断的那个场景。
	if !strings.Contains(certPEM, "\n") || !strings.Contains(keyPEM, "\n") {
		t.Fatalf("生成的 PEM 不含换行符，测试样本失去意义: certPEM=%dB keyPEM=%dB", len(certPEM), len(keyPEM))
	}
	t.Logf("cert=%dB key=%dB (含换行符)", len(certPEM), len(keyPEM))

	conf := model.CKManClickHouseConfig{
		Cluster: name, Port: 9440, User: "default", Password: "secret",
		Hosts:   []string{"192.168.0.1"},
		TlsConf: &model.TlsConf{Cert: certPEM, Key: keyPEM, SelfSigned: true},
	}
	if err := mgr.CreateCluster(conf); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	defer mgr.DeleteCluster(name)

	// 覆盖点 1+2:读回后 Cert/Key 与写入前逐字节一致。
	got, err := mgr.GetClusterbyName(name)
	if err != nil {
		t.Fatalf("GetClusterbyName: %v", err)
	}
	if got.TlsConf == nil {
		t.Fatalf("round-trip TlsConf is nil")
	}
	if got.TlsConf.Cert != certPEM {
		t.Fatalf("cert round-trip 不一致: got %dB, want %dB", len(got.TlsConf.Cert), len(certPEM))
	}
	if got.TlsConf.Key != keyPEM {
		t.Fatalf("key round-trip 不一致(解密后与原文不同,换行符是否被截断?): got %dB, want %dB", len(got.TlsConf.Key), len(keyPEM))
	}
	if !got.TlsConf.SelfSigned {
		t.Fatalf("SelfSigned 标记在往返后丢失")
	}

	// 覆盖点 3:库里存的 Key 必须是密文(不等于明文)，Cert 是明文(证书是公开信息，
	// 设计上不加密)。直接查 tbl_cluster 的原始列，绕开 GetClusterbyName 内置的解密。
	rawConfigJSON := rawStoredConfig(t, mgr, name)
	var stored model.CKManClickHouseConfig
	if err := json.Unmarshal([]byte(rawConfigJSON), &stored); err != nil {
		t.Fatalf("解析原始存储的 config JSON 失败: %v", err)
	}
	if stored.TlsConf == nil {
		t.Fatalf("原始存储的 TlsConf 是 nil")
	}
	if stored.TlsConf.Key == keyPEM {
		t.Fatalf("私钥在库里是明文——EncodePasswd 没有生效!")
	}
	if stored.TlsConf.Cert != certPEM {
		n := min(40, len(stored.TlsConf.Cert))
		t.Fatalf("证书在库里未按明文存储(证书是公开信息，设计上不加密): got prefix %q", stored.TlsConf.Cert[:n])
	}

	// 覆盖点 4:更换证书(常见运维场景:证书到期轮换)后再次往返仍然一致。
	certPEM2, keyPEM2, err := common.GenSelfSignedCert([]string{"192.168.0.2"})
	if err != nil {
		t.Fatalf("GenSelfSignedCert(第二套证书): %v", err)
	}
	conf.TlsConf = &model.TlsConf{Cert: certPEM2, Key: keyPEM2, SelfSigned: true}
	if err := mgr.UpdateCluster(conf); err != nil {
		t.Fatalf("UpdateCluster(换证书): %v", err)
	}
	got2, err := mgr.GetClusterbyName(name)
	if err != nil {
		t.Fatalf("GetClusterbyName(换证书后): %v", err)
	}
	if got2.TlsConf == nil || got2.TlsConf.Cert != certPEM2 || got2.TlsConf.Key != keyPEM2 {
		t.Fatalf("换证书后往返不一致")
	}
	if got2.TlsConf.Cert == certPEM || got2.TlsConf.Key == keyPEM {
		t.Fatalf("换证书后读到的还是旧证书,UpdateCluster 未生效")
	}

	// 覆盖点 5:TlsConf 为 nil 的明文集群不受影响,读写都不出错、也不会平白长出 TlsConf。
	plainName := tagPrefix + "plain_cluster"
	_ = mgr.DeleteCluster(plainName)
	plainConf := model.CKManClickHouseConfig{
		Cluster: plainName, Port: 9000, User: "default", Password: "secret",
		Hosts: []string{"192.168.0.3"},
	}
	if err := mgr.CreateCluster(plainConf); err != nil {
		t.Fatalf("CreateCluster(明文集群): %v", err)
	}
	defer mgr.DeleteCluster(plainName)
	gotPlain, err := mgr.GetClusterbyName(plainName)
	if err != nil {
		t.Fatalf("GetClusterbyName(明文集群): %v", err)
	}
	if gotPlain.TlsConf != nil {
		t.Fatalf("明文集群不应该有 TlsConf,got=%+v", gotPlain.TlsConf)
	}
}

// rawStoredConfig 绕开 GetClusterbyName 内置的 DecodePasswd，直接读 tbl_cluster
// 落库的原始 config JSON 字符串，用来核实"库里到底存的是不是密文"。
//
// 三个后端的表结构字段名/类型不完全一样(postgres/mysql 是 Config string，DM8 是
// Configuration dmSchema.Clob)，这里做一次性的类型判断收敛差异；调用方(测试用例
// 主体)拿到的都是同一种 string，不需要关心具体是哪个后端——DM8 环境就位后这条分支
// 直接能跑，不需要再改测试主体。
func rawStoredConfig(t *testing.T, mgr repository.PersistentMgr, clusterName string) string {
	t.Helper()
	switch m := mgr.(type) {
	case *postgres.PostgresPersistent:
		var table postgres.TblCluster
		if err := m.Client.Where("cluster_name = ?", clusterName).First(&table).Error; err != nil {
			t.Fatalf("rawStoredConfig(postgres): %v", err)
		}
		return table.Config
	case *mysql.MysqlPersistent:
		var table mysql.TblCluster
		if err := m.Client.Where("cluster_name = ?", clusterName).First(&table).Error; err != nil {
			t.Fatalf("rawStoredConfig(mysql): %v", err)
		}
		return table.Config
	case *dm8.DM8Persistent:
		var table dm8.TblCluster
		if err := m.Client.Where("cluster_name = ?", clusterName).First(&table).Error; err != nil {
			t.Fatalf("rawStoredConfig(dm8): %v", err)
		}
		return string(table.Configuration)
	default:
		t.Fatalf("rawStoredConfig: 未知的后端类型 %T", mgr)
		return ""
	}
}

func testPolicyCRUD(t *testing.T, mgr repository.PersistentMgr) {
	id := tagPrefix + "policy"
	_ = mgr.HardDeleteBackupPolicy(id)
	p := model.BackupPolicy{
		PolicyID: id, ClusterName: tagPrefix + "cluster", Database: "db", Table: "t",
		ScheduleType: "scheduled", Instance: "10.0.0.1:8808", BackupType: "daily",
		Enabled: true, CreateTime: time.Now(), UpdateTime: time.Now(),
	}
	if err := mgr.CreateBackupPolicy(p); err != nil {
		t.Fatalf("CreateBackupPolicy: %v", err)
	}
	defer mgr.HardDeleteBackupPolicy(id)
	if _, err := mgr.GetBackupPolicy(id); err != nil {
		t.Fatalf("GetBackupPolicy: %v", err)
	}
	p.Enabled = false
	if err := mgr.UpdateBackupPolicy(p); err != nil {
		t.Fatalf("UpdateBackupPolicy: %v", err)
	}
	if _, err := mgr.GetBackupPoliciesByCluster(p.ClusterName); err != nil {
		t.Fatalf("GetBackupPoliciesByCluster: %v", err)
	}
}

// testBackupRunZeroTime 是核心雷点:queued/skipped 状态的 run 的 StartedAt/FinishedAt
// 是 Go 零值时间 0001-01-01。达梦直接报 -6118;MySQL DATETIME 下界是 1000-01-01,
// 0001-01-01 越界也可能报错/被塞成 0000-00-00;PG 时间范围够大通常没事。
func testBackupRunZeroTime(t *testing.T, mgr repository.PersistentMgr) {
	id := tagPrefix + "run"
	_ = mgr.DeleteBackupRun(id)
	run := model.BackupRun{
		RunID: id, PolicyID: tagPrefix + "policy", ClusterName: tagPrefix + "cluster",
		Database: "db", Table: "t", Operation: "backup", TriggerType: "cron",
		Instance: "10.0.0.1:8808", Status: model.BACKUP_STATUS_QUEUED,
		Partitions: []model.BackupRunPartition{{Partition: "20260101", Status: model.BACKUP_PARTITION_STATUS_WAITING}},
		StartedAt:  time.Time{}, // 零值 —— 雷点
		FinishedAt: time.Time{}, // 零值 —— 雷点
		CreateTime: time.Now(),
	}
	if err := mgr.CreateBackupRun(run); err != nil {
		t.Fatalf("CreateBackupRun(零值时间): %v", err)
	}
	defer mgr.DeleteBackupRun(id)

	got, err := mgr.GetBackupRun(id)
	if err != nil {
		t.Fatalf("GetBackupRun: %v", err)
	}
	if got.RunID != id || len(got.Partitions) != 1 {
		t.Fatalf("round-trip run mismatch: %+v", got)
	}
	// before=零值:测试 started_at < ? 之类分页/过滤路径不被零值时间打挂
	if _, err := mgr.GetRunsByPolicy(run.PolicyID, 10, time.Time{}); err != nil {
		t.Fatalf("GetRunsByPolicy(before=zero): %v", err)
	}
	if _, err := mgr.GetRunsByPolicy(run.PolicyID, 10, time.Now()); err != nil {
		t.Fatalf("GetRunsByPolicy(before=now): %v", err)
	}
	// queued → running:把零值 started_at 更新成真实时间
	ok, err := mgr.MarkRunRunningIfQueued(id, "10.0.0.1:8808", time.Now())
	if err != nil {
		t.Fatalf("MarkRunRunningIfQueued: %v", err)
	}
	if !ok {
		t.Fatalf("MarkRunRunningIfQueued 应把 queued 置为 running")
	}
}

func testUserCRUD(t *testing.T, mgr repository.PersistentMgr) {
	name := tagPrefix + "user"
	_ = mgr.DeleteUser(name)
	u := model.CkmanUser{
		Username: name, PasswordHash: "hash", Policy: "ordinary", Enabled: true,
		CreatedAt: time.Now().Unix(), UpdatedAt: time.Now().Unix(),
	}
	if err := mgr.CreateUser(u); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	defer mgr.DeleteUser(name)
	if !mgr.UserExists(name) {
		t.Fatalf("UserExists=false 刚建的用户")
	}
	if _, err := mgr.GetUserByName(name); err != nil {
		t.Fatalf("GetUserByName: %v", err)
	}
	u.Policy = "guest"
	if err := mgr.UpdateUser(u); err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}
}

func testQueryHistory(t *testing.T, mgr repository.PersistentMgr) {
	cs := tagPrefix + "qh"
	_ = mgr.DeleteQueryHistory(cs)
	qh := model.QueryHistory{Cluster: tagPrefix + "cluster", CheckSum: cs, QuerySql: "SELECT 1", CreateTime: time.Now()}
	if err := mgr.CreateQueryHistory(qh); err != nil {
		t.Fatalf("CreateQueryHistory: %v", err)
	}
	defer mgr.DeleteQueryHistory(cs)
	if _, err := mgr.GetQueryHistoryByCheckSum(cs); err != nil {
		t.Fatalf("GetQueryHistoryByCheckSum: %v", err)
	}
}

func testTaskCRUD(t *testing.T, mgr repository.PersistentMgr) {
	id := tagPrefix + "task"
	_ = mgr.DeleteTask(id)
	task := model.Task{
		TaskId: id, ClusterName: tagPrefix + "cluster", ServerIp: "10.0.0.1",
		TaskType: "deploy", Status: model.TaskStatusWaiting,
		CreateTime: time.Now(), UpdateTime: time.Now(),
	}
	if err := mgr.CreateTask(task); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	defer mgr.DeleteTask(id)
	if _, err := mgr.GetTaskbyTaskId(id); err != nil {
		t.Fatalf("GetTaskbyTaskId: %v", err)
	}
}
