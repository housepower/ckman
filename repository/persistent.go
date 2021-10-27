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

type PersistentMgr interface{

	UnmarshalConfig(configMap map[string]interface{})interface{}

	Init(config interface{})error

	//start transaction
	Begin()error

	//commit transaction
	Commit()error

	Rollback()error

	//get cluster config by name, return model.CKManClickHouseConfig
	GetClusterbyName(cluster string)(model.CKManClickHouseConfig,error)

	ClusterExists(cluster string)bool

	//get logic cluster by name, return a list
	GetLogicClusterbyName(logic string)([]string,error)

	//get all clusters, return a map
	GetAllClusters()(map[string]model.CKManClickHouseConfig,error)

	//get all logic clusters, return a map
	GetAllLogicClusters()(map[string][]string,error)

	//add a new cluster record
	CreateCluster(cluster model.CKManClickHouseConfig)error

	//add a new logic cluster to list
	CreateLogicCluster(logic string, physics []string)error


	UpdateCluster(cluster model.CKManClickHouseConfig)error
	UpdateLogicCluster(logic string, physics []string)error

	DeleteCluster(clusterName string) error
	DeleteLogicCluster(clusterName string) error
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

func InitPersistent()error{
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
