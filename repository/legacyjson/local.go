package legacyjson

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
	lp.Data.Backup = make(map[string]model.Backup)
	lp.Snapshot.Backup = make(map[string]model.Backup)
	lp.Data.BackupPolicy = make(map[string]model.BackupPolicy)
	lp.Snapshot.BackupPolicy = make(map[string]model.BackupPolicy)
	lp.Data.BackupRun = make(map[string]model.BackupRun)
	lp.Snapshot.BackupRun = make(map[string]model.BackupRun)

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
	n := common.TernaryExpression(len(historys) > 100, 100, len(historys)).(int)
	return historys[:n], nil
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

func (lp *LocalPersistent) GetQueryHistoryCount(cluster string) int64 {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var qhLists Historys
	for _, v := range lp.Data.QueryHistory {
		if v.Cluster == cluster {
			qhLists = append(qhLists, v)
		}
	}
	return int64(len(qhLists))
}

func (lp *LocalPersistent) GetEarliestQuery(cluster string) (model.QueryHistory, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var historys Historys
	var qhLists Historys
	for _, v := range lp.Data.QueryHistory {
		if v.Cluster == cluster {
			qhLists = append(qhLists, v)
		}
	}
	if err := common.DeepCopyByGob(&historys, &qhLists); err != nil {
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

func (lp *LocalPersistent) CreateBackup(backup model.Backup) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	backup.CreateTime = time.Now()
	backup.UpdateTime = backup.CreateTime
	if _, ok := lp.Data.Backup[backup.BackupId]; ok {
		return repository.ErrRecordExists
	}
	lp.Data.Backup[backup.BackupId] = backup
	if !lp.InTransAction {
		return lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) UpdateBackup(backup model.Backup) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	backup.UpdateTime = time.Now()
	if _, ok := lp.Data.Backup[backup.BackupId]; ok {
		lp.Data.Backup[backup.BackupId] = backup
	} else {
		return repository.ErrRecordNotFound
	}

	if !lp.InTransAction {
		return lp.dump()
	}
	return nil
}
func (lp *LocalPersistent) DeleteBackup(id string) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.Backup[id]; !ok {
		return repository.ErrRecordExists
	}
	delete(lp.Data.Backup, id)
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}
func (lp *LocalPersistent) GetAllBackups(cluster string) ([]model.Backup, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var backups Backups
	for _, value := range lp.Data.Backup {
		if value.ClusterName == cluster {
			backups = append(backups, value)
		}
	}
	sort.Sort(backups)
	return backups, nil

}
func (lp *LocalPersistent) GetBackupById(id string) (model.Backup, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	if backup, ok := lp.Data.Backup[id]; ok {
		return backup, nil
	}
	return model.Backup{}, repository.ErrRecordNotFound
}

func (lp *LocalPersistent) GetBackupByTable(cluster, database, table string) (model.Backup, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	for _, value := range lp.Data.Backup {
		if value.ClusterName == cluster && value.Database == database && value.Table == table {
			return value, nil
		}
	}
	return model.Backup{}, repository.ErrRecordNotFound
}

func (lp *LocalPersistent) GetbackupByOperation(operation string) ([]model.Backup, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var backups Backups
	for _, value := range lp.Data.Backup {
		if value.Operation == operation {
			backups = append(backups, value)
		}
	}
	sort.Sort(backups)
	return backups, nil
}

func (lp *LocalPersistent) GetBackupByShechuleType(scheduleType string) ([]model.Backup, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var backups Backups
	for _, value := range lp.Data.Backup {
		if value.ScheduleType == scheduleType {
			backups = append(backups, value)
		}
	}
	sort.Sort(backups)
	return backups, nil
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

// ─── BackupPolicy ────────────────────────────────────────────────────────────

func (lp *LocalPersistent) CreateBackupPolicy(p model.BackupPolicy) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.BackupPolicy[p.PolicyID]; ok {
		return repository.ErrRecordExists
	}
	if p.CreateTime.IsZero() {
		p.CreateTime = time.Now()
	}
	p.UpdateTime = time.Now()
	lp.Data.BackupPolicy[p.PolicyID] = p
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) UpdateBackupPolicy(p model.BackupPolicy) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.BackupPolicy[p.PolicyID]; !ok {
		return repository.ErrRecordNotFound
	}
	p.UpdateTime = time.Now()
	lp.Data.BackupPolicy[p.PolicyID] = p
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) DeleteBackupPolicy(policyID string) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	p, ok := lp.Data.BackupPolicy[policyID]
	if !ok {
		return repository.ErrRecordNotFound
	}
	p.Deleted = true
	p.Enabled = false
	p.UpdateTime = time.Now()
	lp.Data.BackupPolicy[policyID] = p
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) GetBackupPolicy(policyID string) (model.BackupPolicy, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	p, ok := lp.Data.BackupPolicy[policyID]
	if !ok {
		return model.BackupPolicy{}, repository.ErrRecordNotFound
	}
	return p, nil
}

func (lp *LocalPersistent) GetBackupPoliciesByCluster(cluster string) ([]model.BackupPolicy, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var out []model.BackupPolicy
	for _, p := range lp.Data.BackupPolicy {
		if p.ClusterName == cluster && !p.Deleted {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreateTime.Before(out[j].CreateTime) })
	return out, nil
}

func (lp *LocalPersistent) GetActiveScheduledPolicies(instance string) ([]model.BackupPolicy, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var out []model.BackupPolicy
	for _, p := range lp.Data.BackupPolicy {
		if !p.Deleted && p.Enabled && p.ScheduleType == model.BACKUP_SCHEDULED && p.Instance == instance {
			out = append(out, p)
		}
	}
	return out, nil
}

// ─── BackupRun ───────────────────────────────────────────────────────────────

func (lp *LocalPersistent) CreateBackupRun(r model.BackupRun) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.BackupRun[r.RunID]; ok {
		return repository.ErrRecordExists
	}
	if r.CreateTime.IsZero() {
		r.CreateTime = time.Now()
	}
	lp.Data.BackupRun[r.RunID] = r
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) UpdateBackupRun(r model.BackupRun) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.BackupRun[r.RunID]; !ok {
		return repository.ErrRecordNotFound
	}
	lp.Data.BackupRun[r.RunID] = r
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) DeleteBackupRun(runID string) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.BackupRun[runID]; !ok {
		return repository.ErrRecordNotFound
	}
	delete(lp.Data.BackupRun, runID)
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return nil
}

func (lp *LocalPersistent) GetBackupRun(runID string) (model.BackupRun, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	r, ok := lp.Data.BackupRun[runID]
	if !ok {
		return model.BackupRun{}, repository.ErrRecordNotFound
	}
	return r, nil
}

func (lp *LocalPersistent) GetRunsByPolicy(policyID string, limit int, before time.Time) ([]model.BackupRun, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var out []model.BackupRun
	for _, r := range lp.Data.BackupRun {
		if r.PolicyID != policyID {
			continue
		}
		if !before.IsZero() && !r.StartedAt.Before(before) {
			continue
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (lp *LocalPersistent) GetRunsByTable(cluster, database, table string, sinceDays int) ([]model.BackupRun, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	cutoff := time.Now().AddDate(0, 0, -sinceDays)
	var out []model.BackupRun
	for _, r := range lp.Data.BackupRun {
		if r.ClusterName == cluster && r.Database == database && r.Table == table && r.StartedAt.After(cutoff) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out, nil
}

func (lp *LocalPersistent) GetRunsInFlightByPolicy(policyID string) ([]model.BackupRun, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var out []model.BackupRun
	for _, r := range lp.Data.BackupRun {
		if r.PolicyID == policyID && (r.Status == model.BACKUP_STATUS_QUEUED || r.Status == model.BACKUP_STATUS_RUNNING) {
			out = append(out, r)
		}
	}
	return out, nil
}

func (lp *LocalPersistent) GetRunsInFlightByInstance(instance string) ([]model.BackupRun, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var out []model.BackupRun
	for _, r := range lp.Data.BackupRun {
		if r.Instance == instance && (r.Status == model.BACKUP_STATUS_QUEUED || r.Status == model.BACKUP_STATUS_RUNNING) {
			out = append(out, r)
		}
	}
	return out, nil
}

func (lp *LocalPersistent) MarkRunRunningIfQueued(runID, instance string, startedAt time.Time) (bool, error) {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	r, ok := lp.Data.BackupRun[runID]
	if !ok {
		return false, repository.ErrRecordNotFound
	}
	if r.Status != model.BACKUP_STATUS_QUEUED {
		return false, nil
	}
	r.Status = model.BACKUP_STATUS_RUNNING
	r.Instance = instance
	r.StartedAt = startedAt
	lp.Data.BackupRun[runID] = r
	if !lp.InTransAction {
		_ = lp.dump()
	}
	return true, nil
}

func (lp *LocalPersistent) GetAllBackupPolicies() ([]model.BackupPolicy, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	out := make([]model.BackupPolicy, 0, len(lp.Data.BackupPolicy))
	for _, p := range lp.Data.BackupPolicy {
		out = append(out, p)
	}
	return out, nil
}

func (lp *LocalPersistent) GetAllBackupRuns() ([]model.BackupRun, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	out := make([]model.BackupRun, 0, len(lp.Data.BackupRun))
	for _, r := range lp.Data.BackupRun {
		out = append(out, r)
	}
	return out, nil
}
