package local

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type LocalPersistent struct {
	Config        LocalConfig
	InTransAction bool
	Data          PersistentData
	Snapshot      PersistentData
	lock          sync.RWMutex
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
	lp.Data.QueryHistory = make(map[string]model.QueryHistory)
	lp.Snapshot.QueryHistory = make(map[string]model.QueryHistory)
	lp.Data.Task = make(map[string]model.Task)
	lp.Snapshot.Task = make(map[string]model.Task)

	return lp.load()
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

func (lp *LocalPersistent) ClusterExists(cluster string) bool {
	_, err := lp.GetClusterbyName(cluster)
	if err != nil {
		return false
	}
	return true
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
	return common.ArrayDistinct(physics), nil
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
		logicMap[key] = common.ArrayDistinct(value)
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
	lp.Data.Logics[logic] = common.ArrayDistinct(physics)
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
	lp.Data.Logics[logic] = common.ArrayDistinct(physics)
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

func (lp *LocalPersistent) GetAllQueryHistory() (map[string]model.QueryHistory, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	historys := make(map[string]model.QueryHistory)
	for k, v := range lp.Data.QueryHistory {
		historys[k] = v
	}
	return historys, nil
}

func (lp *LocalPersistent) GetQueryHistoryByCluster(cluster string) ([]model.QueryHistory, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var historys Historys
	for _, v := range lp.Data.QueryHistory {
		if v.Cluster == cluster {
			historys = append(historys, v)
		}
	}
	sort.Sort(sort.Reverse(historys))
	return historys, nil
}

func (lp *LocalPersistent) GetQueryHistoryByCheckSum(checksum string) (model.QueryHistory, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	history, ok := lp.Data.QueryHistory[checksum]
	if !ok {
		return model.QueryHistory{}, repository.ErrRecordNotFound
	}
	return history, nil
}

func (lp *LocalPersistent) CreateQueryHistory(qh model.QueryHistory) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.QueryHistory[qh.CheckSum]; ok {
		return repository.ErrRecordExists
	}
	qh.CreateTime = time.Now()
	lp.Data.QueryHistory[qh.CheckSum] = qh
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) UpdateQueryHistory(qh model.QueryHistory) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.QueryHistory[qh.CheckSum]; !ok {
		return repository.ErrRecordNotFound
	}
	qh.CreateTime = time.Now()
	lp.Data.QueryHistory[qh.CheckSum] = qh
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) DeleteQueryHistory(checksum string) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.QueryHistory[checksum]; !ok {
		return repository.ErrRecordNotFound
	}
	delete(lp.Data.QueryHistory, checksum)
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) GetQueryHistoryCount() int64 {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	return int64(len(lp.Data.QueryHistory))
}

func (lp *LocalPersistent) GetEarliestQuery() (model.QueryHistory, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var historys Historys
	if err := common.DeepCopyByGob(&historys, &lp.Data.QueryHistory); err != nil {
		return model.QueryHistory{}, err
	}
	if len(historys) == 0 {
		return model.QueryHistory{}, repository.ErrRecordNotFound
	}
	sort.Sort(historys)
	return historys[0], nil
}

func (lp *LocalPersistent) CreateTask(task model.Task) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	task.CreateTime = time.Now()
	task.UpdateTime = task.CreateTime
	if _, ok := lp.Data.Task[task.TaskId]; ok {
		return repository.ErrRecordExists
	}
	lp.Data.Task[task.TaskId] = task
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) UpdateTask(task model.Task) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.Task[task.TaskId]; !ok {
		return repository.ErrRecordNotFound
	}
	task.UpdateTime = time.Now()
	lp.Data.Task[task.TaskId] = task
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) DeleteTask(id string) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.Task[id]; !ok {
		return repository.ErrRecordExists
	}
	delete(lp.Data.Task, id)
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) GetAllTasks() ([]model.Task, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var tasks []model.Task
	for _, value := range lp.Data.Task {
		tasks = append(tasks, value)
	}
	return tasks, nil
}

func (lp *LocalPersistent) GetEffectiveTaskCount() int64 {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var tasks []model.Task
	for _, value := range lp.Data.Task {
		if value.Status == model.TaskStatusRunning || value.Status == model.TaskStatusWaiting {
			tasks = append(tasks, value)
		}
	}
	return int64(len(tasks))
}

func (lp *LocalPersistent) GetPengdingTasks(serverIp string) ([]model.Task, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var tasks []model.Task
	for _, value := range lp.Data.Task {
		if value.Status == model.TaskStatusWaiting && serverIp == value.ServerIp {
			tasks = append(tasks, value)
		}
	}
	return tasks, nil
}

func (lp *LocalPersistent) GetTaskbyTaskId(id string) (model.Task, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	task, ok := lp.Data.Task[id]
	if !ok {
		return model.Task{}, repository.ErrRecordNotFound
	}
	return task, nil
}

func (lp *LocalPersistent) marshal() ([]byte, error) {
	var data []byte
	var err error
	if lp.Config.Format == config.FORMAT_JSON {
		data, err = json.MarshalIndent(lp.Data, "", "  ")
	} else if lp.Config.Format == config.FORMAT_YAML {
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

	if lp.Config.Format == config.FORMAT_JSON {
		err = json.Unmarshal(data, &lp.Data)
	} else if lp.Config.Format == config.FORMAT_YAML {
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
		log.Logger.Warnf("file [%s] is not exist", localFile)
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
