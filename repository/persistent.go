package repository

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
)

var Ps PersistentMgr

// Global registry to mapping adapter name to the adapter factory
var PersistentRegistry map[string]PersistentFactory = make(map[string]PersistentFactory)

type PersistentFactory interface {
	GetPersistentName() string
	// Create an adapter instance
	CreatePersistent() PersistentMgr
}

type PersistentBase interface {
	UnmarshalConfig(configMap map[string]interface{}) interface{}
	Init(config interface{}) error
	Begin() error
	Commit() error
	Rollback() error
}

type PersistentClusterService interface {
	GetClusterbyName(cluster string) (model.CKManClickHouseConfig, error)
	ClusterExists(cluster string) bool
	GetAllClusters() (map[string]model.CKManClickHouseConfig, error)
	CreateCluster(cluster model.CKManClickHouseConfig) error
	UpdateCluster(cluster model.CKManClickHouseConfig) error
	DeleteCluster(clusterName string) error
}

type PersistentLogicService interface {
	GetLogicClusterbyName(logic string) ([]string, error)
	GetAllLogicClusters() (map[string][]string, error)
	CreateLogicCluster(logic string, physics []string) error
	UpdateLogicCluster(logic string, physics []string) error
	DeleteLogicCluster(clusterName string) error
}

type PersistentQueryHistoryService interface {
	GetAllQueryHistory() (map[string]model.QueryHistory, error)
	GetQueryHistoryByCluster(cluster string) ([]model.QueryHistory, error)
	GetQueryHistoryByCheckSum(checksum string) (model.QueryHistory, error)
	CreateQueryHistory(qh model.QueryHistory) error
	UpdateQueryHistory(qh model.QueryHistory) error
	DeleteQueryHistory(checksum string) error
	GetQueryHistoryCount(cluster string) int64
	GetEarliestQuery(cluster string) (model.QueryHistory, error)
}

type PersistentTaskService interface {
	CreateTask(task model.Task) error
	UpdateTask(task model.Task) error
	DeleteTask(id string) error
	GetAllTasks() ([]model.Task, error)
	GetPengdingTasks(serverIp string) ([]model.Task, error)
	GetEffectiveTaskCount() int64
	GetTaskbyTaskId(id string) (model.Task, error)
}

type PersistentBackupService interface {
	CreateBackup(backup model.Backup) error
	UpdateBackup(backup model.Backup) error
	DeleteBackup(id string) error
	GetAllBackups(cluster string) ([]model.Backup, error)
	GetBackupById(id string) (model.Backup, error)
	GetBackupByTable(cluster, database, table string) (model.Backup, error)
	GetbackupByOperation(operation string) ([]model.Backup, error)
	GetBackupByShechuleType(scheduleType string) ([]model.Backup, error)
}

type PersistentBackupPolicyService interface {
	CreateBackupPolicy(p model.BackupPolicy) error
	UpdateBackupPolicy(p model.BackupPolicy) error
	DeleteBackupPolicy(policyID string) error     // 软删：仅置 deleted=true
	HardDeleteBackupPolicy(policyID string) error // 物理删：DELETE FROM，行不保留
	GetBackupPolicy(policyID string) (model.BackupPolicy, error)
	GetBackupPoliciesByCluster(cluster string) ([]model.BackupPolicy, error)
	GetActiveScheduledPolicies(instance string) ([]model.BackupPolicy, error) // enabled+scheduled+!deleted+instance==
	GetAllBackupPolicies() ([]model.BackupPolicy, error)
}

type PersistentBackupRunService interface {
	CreateBackupRun(r model.BackupRun) error
	UpdateBackupRun(r model.BackupRun) error
	DeleteBackupRun(runID string) error
	GetBackupRun(runID string) (model.BackupRun, error)
	GetRunsByPolicy(policyID string, limit int, before time.Time) ([]model.BackupRun, error)
	// GetRunsByTable returns runs for the given table filtered by sinceDays.
	// sinceDays <= 0 means no time limit — returns the full history of runs for this table.
	GetRunsByTable(cluster, database, table string, sinceDays int) ([]model.BackupRun, error)
	GetRunsInFlightByPolicy(policyID string) ([]model.BackupRun, error)
	GetRunsInFlightByInstance(instance string) ([]model.BackupRun, error)
	GetRunsInFlightByCluster(cluster string) ([]model.BackupRun, error)
	MarkRunRunningIfQueued(runID, instance string, startedAt time.Time) (bool, error)
	GetAllBackupRuns() ([]model.BackupRun, error)
}

type PersistentUserService interface {
	GetUserByName(username string) (model.CkmanUser, error)
	UserExists(username string) bool
	GetAllUsers() ([]model.CkmanUser, error)
	CreateUser(u model.CkmanUser) error
	UpdateUser(u model.CkmanUser) error
	DeleteUser(username string) error
}

type PersistentMgr interface {
	PersistentBase
	PersistentClusterService
	PersistentLogicService
	PersistentQueryHistoryService
	PersistentTaskService
	PersistentBackupService       // 老接口保留
	PersistentBackupPolicyService // 新增
	PersistentBackupRunService    // 新增
	PersistentUserService         // 新增 — Phase 1 用户管理
}

func RegistePersistent(fn func() PersistentFactory) {
	if fn == nil {
		return
	}
	factory := fn()
	name := factory.GetPersistentName()
	if name == "" {
		panic("Empty persistent name when registe persistent factory")
	}
	PersistentRegistry[name] = factory
}

func GetPersistentByName(name string) PersistentMgr {
	if factory, ok := PersistentRegistry[name]; ok {
		return factory.CreatePersistent()
	}
	return nil
}

// Older RPM/DEB packages wrote this hash during installation. Consume it once
// so upgrades before the first service start still accept the printed password.
const preparedAdminHashFile = ".ckman-initial-password.hash"

func preparedAdminHashPath() string {
	if config.GlobalConfig.ConfigFile == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(config.GlobalConfig.ConfigFile), preparedAdminHashFile)
}

func readPreparedAdminHash() (string, bool, error) {
	path := preparedAdminHashPath()
	if path == "" {
		return "", false, nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, errors.Wrap(err, "read prepared admin password hash")
	}
	hash := strings.TrimSpace(string(data))
	if _, err := bcrypt.Cost([]byte(hash)); err != nil {
		return "", false, errors.Wrap(err, "invalid prepared admin password hash")
	}
	return hash, true, nil
}

func removePreparedAdminHash() error {
	path := preparedAdminHashPath()
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "remove prepared admin password hash")
	}
	return nil
}

func printInitialAdminPassword(password string) {
	fmt.Fprintf(os.Stdout, "CKMAN initial login: username=%s password=%s\nSave this password now; it will not be printed again. Change it after the first login.\n", common.DefaultAdminName, password)
}

func seedAdminIfAbsent() error {
	if Ps.UserExists(common.DefaultAdminName) {
		return removePreparedAdminHash()
	}
	hash, prepared, err := readPreparedAdminHash()
	if err != nil {
		return err
	}
	password := ""
	if !prepared {
		password, err = common.GenerateInitialAdminPassword()
		if err != nil {
			return err
		}
		hash, err = common.HashPassword(common.Md5CheckSum(password))
		if err != nil {
			return errors.Wrap(err, "hash initial admin password")
		}
	}
	now := time.Now().Unix()
	err = Ps.CreateUser(model.CkmanUser{
		Username:           common.DefaultAdminName,
		PasswordHash:       hash,
		Policy:             common.ADMIN,
		Enabled:            true,
		MustChangePassword: true,
		CreatedAt:          now,
		UpdatedAt:          now,
	})
	if err != nil {
		if errors.Is(err, ErrRecordExists) {
			return removePreparedAdminHash() // another instance won the race
		}
		return err
	}
	if prepared {
		return removePreparedAdminHash()
	}
	printInitialAdminPassword(password)
	return nil
}

func InitPersistent() error {
	if Ps == nil {
		Ps = GetPersistentByName(config.GlobalConfig.Server.PersistentPolicy)
	}
	if Ps == nil {
		return errors.Errorf("persistent policy %s is not regist", config.GlobalConfig.Server.PersistentPolicy)
	}

	var pcfg interface{}
	if config.GlobalConfig.PersistentConfig != nil {
		configMap, ok := config.GlobalConfig.PersistentConfig[config.GlobalConfig.Server.PersistentPolicy]
		if !ok {
			pcfg = nil
		} else {
			pcfg = Ps.UnmarshalConfig(configMap)
		}
	}
	if err := Ps.Init(pcfg); err != nil {
		return err
	}
	return seedAdminIfAbsent()
}
