package local

import (
	"encoding/json"
	"fmt"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
	"os"
	"path"
	"sync"
)

type LocalPersistent struct {
	Config        LocalConfig
	InTransAction bool
	Data          PersistentData
	Snapshot      PersistentData
	lock          sync.RWMutex
}

func (lp *LocalPersistent) UnmarshalConfig(configMap map[string]interface{}) interface{} {
	var config LocalConfig
	data, err := json.Marshal(configMap)
	if err != nil {
		log.Logger.Errorf("marshal local configMap failed:%v", err)
		return nil
	}
	if err = json.Unmarshal(data, &config); err != nil {
		log.Logger.Errorf("unmarshal local config failed:%v", err)
		return nil
	}
	return config
}

func (lp *LocalPersistent) ClusterExists(cluster string) bool {
	_, err := lp.GetClusterbyName(cluster)
	if err != nil {
		return false
	}
	return true
}

func (lp *LocalPersistent) Init(config interface{}) error {
	if config == nil {
		config = LocalConfig{}
	}
	lp.Config = config.(LocalConfig)
	lp.Config.Normalize()
	lp.InTransAction = false
	lp.Data.Clusters = make(map[string]model.CKManClickHouseConfig)
	lp.Snapshot.Clusters = make(map[string]model.CKManClickHouseConfig)
	lp.Data.Logics = make(map[string][]string)
	lp.Snapshot.Logics = make(map[string][]string)

	return lp.load()
}

func (lp *LocalPersistent) Begin() error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if lp.InTransAction {
		return repository.ErrTransActionBegin
	}
	lp.InTransAction = true
	return common.DeepCopyByGob(&lp.Snapshot, &lp.Data)
}

func (lp *LocalPersistent) Commit() error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if !lp.InTransAction {
		return repository.ErrTransActionEnd
	}
	lp.InTransAction = false
	return lp.dump()
}

func (lp *LocalPersistent) Rollback() error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if !lp.InTransAction {
		return repository.ErrTransActionEnd
	}
	lp.InTransAction = false
	return common.DeepCopyByGob(&lp.Data, &lp.Snapshot)
}

func (lp *LocalPersistent) GetClusterbyName(cluster string) (model.CKManClickHouseConfig, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	conf, ok := lp.Data.Clusters[cluster]
	if !ok {
		return model.CKManClickHouseConfig{}, repository.ErrRecordNotFound
	}
	repository.DecodePasswd(&conf)
	return conf, nil
}

func (lp *LocalPersistent) GetLogicClusterbyName(logic string) ([]string, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	physics, ok := lp.Data.Logics[logic]
	if !ok {
		return []string{}, repository.ErrRecordNotFound
	}
	return physics, nil
}

func (lp *LocalPersistent) GetAllClusters() (map[string]model.CKManClickHouseConfig, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	clusterMap := make(map[string]model.CKManClickHouseConfig)
	for key, value := range lp.Data.Clusters {
		repository.DecodePasswd(&value)
		clusterMap[key] = value
	}
	return clusterMap, nil
}

func (lp *LocalPersistent) GetAllLogicClusters() (map[string][]string, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	logicMap := make(map[string][]string)
	for key, value := range lp.Data.Logics {
		logicMap[key] = value
	}
	return logicMap, nil
}

func (lp *LocalPersistent) CreateCluster(conf model.CKManClickHouseConfig) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	repository.EncodePasswd(&conf)
	if _, ok := lp.Data.Clusters[conf.Cluster]; ok {
		return repository.ErrRecordExists
	}
	lp.Data.Clusters[conf.Cluster] = conf
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) CreateLogicCluster(logic string, physics []string) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.Logics[logic]; ok {
		return repository.ErrRecordExists
	}
	lp.Data.Logics[logic] = physics
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) UpdateCluster(conf model.CKManClickHouseConfig) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	repository.EncodePasswd(&conf)
	if _, ok := lp.Data.Clusters[conf.Cluster]; !ok {
		return repository.ErrRecordNotFound
	}
	lp.Data.Clusters[conf.Cluster] = conf
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) UpdateLogicCluster(logic string, physics []string) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.Logics[logic]; !ok {
		return repository.ErrRecordNotFound
	}
	lp.Data.Logics[logic] = physics
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) DeleteCluster(clusterName string) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	delete(lp.Data.Clusters, clusterName)
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) DeleteLogicCluster(clusterName string) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	delete(lp.Data.Logics, clusterName)
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) marshal() ([]byte, error) {
	var data []byte
	var err error
	if lp.Config.Format == FORMAT_JSON {
		data, err = json.MarshalIndent(lp.Data, "", "  ")
	} else if lp.Config.Format == FORMAT_YAML {
		data, err = yaml.Marshal(lp.Data)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "")
	}

	return data, nil
}

func (lp *LocalPersistent) unmarshal(data []byte) error {
	var err error
	if len(data) == 0 {
		return nil
	}

	if lp.Config.Format == FORMAT_JSON {
		err = json.Unmarshal(data, &lp.Data)
	} else if lp.Config.Format == FORMAT_YAML {
		err = yaml.Unmarshal(data, &lp.Data)
	}

	if err != nil {
		return errors.Wrapf(err, "")
	}
	return nil
}

func (lp *LocalPersistent) dump() error {
	data, err := lp.marshal()
	if err != nil {
		return err
	}
	localFile := path.Join(lp.Config.ConfigDir, lp.Config.ConfigFile)
	_ = os.Rename(localFile, fmt.Sprintf("%s.last", localFile))
	localFd, err := os.OpenFile(localFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return errors.Wrapf(err, "")
	}
	defer localFd.Close()

	num, err := localFd.Write(data)
	if err != nil {
		return errors.Wrapf(err, "")
	}

	if num != len(data) {
		return errors.Errorf("didn't write enough data")
	}

	return nil
}

func (lp *LocalPersistent) load() error {
	localFile := path.Join(lp.Config.ConfigDir, lp.Config.ConfigFile)

	_, err := os.Stat(localFile)
	if err != nil {
		// file does not exist
		return nil
	}

	data, err := os.ReadFile(localFile)
	if err != nil {
		return errors.Wrapf(err, "")
	}

	return lp.unmarshal(data)
}

func NewLocalPersistent() *LocalPersistent {
	return &LocalPersistent{}
}
