package model

type DeployCkReq struct {
	Hosts      []string
	Packages   []string
	User       string
	Password   string
	Directory  string
	ClickHouse CkDeployConfig
}

type CkDeployConfig struct {
	Path      string
	User      string
	Password  string
	ZkServers []ZkNode
	Clusters  []CkCluster
}

type ZkNode struct {
	Host string
	Port int
}

type CkCluster struct {
	Name   string
	Shards []CkShard
}

type CkShard struct {
	Replicas []CkReplica
}

type CkReplica struct {
	Host string
	Port int
}
