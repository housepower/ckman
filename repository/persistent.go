package repository

import (
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
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
	GetQueryHistoryCount() int64
	GetEarliestQuery() (model.QueryHistory, error)
}

type PersistentTaskService interface {
	CreateTask(task model.Task) error
	UpdateTask(task model.Task) error
	DeleteTask(id string) error
	GetAllTasks() ([]model.Task, error)
	GetPengdingTasks(serverIp string)([]model.Task, error)
	GetEffectiveTaskCount() int64
	GetTaskbyTaskId(id string) (model.Task, error)
}

type PersistentMgr interface {
	PersistentBase
	PersistentClusterService
	PersistentLogicService
	PersistentQueryHistoryService
	PersistentTaskService
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
	return nil
}
