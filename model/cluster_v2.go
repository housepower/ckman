package model

import "sync"

const CurrentFormatVersion int = 2

type CkClusters struct {
	FormatVersion int                              `json:"FORMAT_VERSION"`
	ConfigVersion int                              `json:"ck_cluster_config_version"`
	Clusters      map[string]CKManClickHouseConfig `json:"clusters"`
	lock          sync.RWMutex                     `json:"-"`
}

func NewCkClusters() *CkClusters {
	return &CkClusters{
		FormatVersion: -1,
		ConfigVersion: -1,
		Clusters:      make(map[string]CKManClickHouseConfig),
	}
}

func (this *CkClusters) GetConfigVersion() int {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.ConfigVersion
}

func (this *CkClusters) SetConfigVersion(version int) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.ConfigVersion = version
}

func (this *CkClusters) GetClusters() map[string]CKManClickHouseConfig {
	this.lock.RLock()
	defer this.lock.RUnlock()
	clusterMap := make(map[string]CKManClickHouseConfig)
	for key, value := range this.Clusters {
		clusterMap[key] = value
	}
	return clusterMap
}

func (this *CkClusters) GetClusterByName(clusterName string) (CKManClickHouseConfig, bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()
	cluster, ok := this.Clusters[clusterName]
	return cluster, ok
}

func (this *CkClusters) SetClusterByName(clusterName string, cluster CKManClickHouseConfig) {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.Clusters[clusterName] = cluster
}

func (this *CkClusters) DeleteClusterByName(clusterName string) {
	this.lock.Lock()
	defer this.lock.Unlock()
	delete(this.Clusters, clusterName)
}

func (this *CkClusters) ClearClusters() {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.Clusters = make(map[string]CKManClickHouseConfig)
}