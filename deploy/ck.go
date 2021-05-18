package deploy

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"github.com/housepower/ckman/service/zookeeper"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/housepower/ckman/config"
	"github.com/pkg/errors"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
)

const (
	TmpWorkDirectory    string = "/tmp/"
	CkClusterNodeAdd    int    = 1
	CkClusterNodeDelete int    = 2
)

type Metrika struct {
	XMLName   xml.Name  `xml:"yandex"`
	ZkServers []Node    `xml:"zookeeper-servers>Node"`
	CkServers []Cluster `xml:"clickhouse_remote_servers>Cluster"`
}

type Node struct {
	XMLName xml.Name `xml:"node"`
	Index   int      `xml:"index,attr"`
	Host    string   `xml:"host"`
	Port    int      `xml:"port"`
}

type Cluster struct {
	XMLName xml.Name
	Shards  []Shard
}

type Shard struct {
	XMLName     xml.Name `xml:"shard"`
	Replication bool     `xml:"internal_replication"`
	Replicas    []Replica
}

type Replica struct {
	XMLName xml.Name `xml:"replica"`
	Host    string   `xml:"host"`
	Port    int      `xml:"port"`
}

type MacroConf struct {
	XMLName xml.Name `xml:"yandex"`
	Macros  Macro    `xml:"macros"`
}

type Macro struct {
	Cluster string `xml:"cluster"`
	Shard   int    `xml:"shard"`
	Replica string `xml:"replica"`
}

type CKDeployFacotry struct{}

func (CKDeployFacotry) Create() Deploy {
	return &CKDeploy{}
}

type CKDeploy struct {
	DeployBase
	Conf      *model.CkDeployConfig
	HostInfos []HostInfo
}

type HostInfo struct {
	MemoryTotal int
}

type CkConfigTemplate struct {
	CkTcpPort         int
	CkHttpPort        int
	Path              string
	User              string
	Password          string
	ClusterName       string
	MaxMemoryPerQuery int64
	MaxMemoryAllQuery int64
	MaxBytesGroupBy   int64
}

type CkUpdateNodeParam struct {
	Ip       string
	Hostname string
	Shard    int
	Op       int
}

func (d *CKDeploy) Init(base *DeployBase, conf interface{}) error {
	c, ok := conf.(*model.CkDeployConfig)
	if !ok {
		return errors.Errorf("value isn't type of CkDeployConfig")
	}

	d.DeployBase = *base
	d.Conf = c
	d.Conf.Normalize()
	d.HostInfos = make([]HostInfo, len(d.Hosts))
	HostNameMap := make(map[string]bool)
	var lock sync.RWMutex
	var lastError error
	for index, host := range d.Hosts {
		innerIndex := index
		innerHost := host
		_ = d.Pool.Submit(func() {
			cmd := "cat /proc/meminfo | grep MemTotal | awk '{print $2}'"
			output, err := common.RemoteExecute(d.User, d.Password, innerHost, d.Port, cmd)
			if err != nil {
				lastError = err
				return
			}

			memory := strings.Trim(output, "\n")
			total, err := strconv.Atoi(memory)
			if err != nil {
				lastError = err
				return
			}

			info := HostInfo{
				MemoryTotal: total,
			}
			d.HostInfos[innerIndex] = info
		})
	}
	d.Pool.StopWait()
	if lastError != nil {
		return lastError
	}

	clusterNodeNum := 0
	lastError = nil
	d.Pool.Restart()
	for shardIndex, shard := range d.Conf.Shards {
		for replicaIndex, replica := range shard.Replicas {
			innerShardIndex := shardIndex
			innerReplicaIndex := replicaIndex
			innerReplica := replica
			_ = d.Pool.Submit(func() {
				cmd := "hostname -f"
				output, err := common.RemoteExecute(d.User, d.Password, innerReplica.Ip, d.Port, cmd)
				if err != nil {
					lastError = err
					return
				}

				hostname := strings.Trim(output, "\n")
				d.Conf.Shards[innerShardIndex].Replicas[innerReplicaIndex].HostName = hostname
				lock.Lock()
				HostNameMap[hostname] = true
				lock.Unlock()
				clusterNodeNum++
			})
		}
	}

	d.Pool.StopWait()
	if lastError != nil {
		return lastError
	}

	if len(HostNameMap) != clusterNodeNum {
		return errors.Errorf("host name are the same")
	}

	if err := ensureHosts(d); err != nil {
		return err
	}
	log.Logger.Infof("init done")

	return nil
}

func (d *CKDeploy) Prepare() error {
	files := make([]string, 0)
	for _, file := range d.Packages {
		files = append(files, path.Join(config.GetWorkDirectory(), "package", file))
	}

	var lastError error
	for _, host := range d.Hosts {
		innerHost := host
		_ = d.Pool.Submit(func() {
			if err := common.ScpFiles(files, TmpWorkDirectory, d.User, d.Password, innerHost, d.Port); err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s prepare done", innerHost)
		})
	}
	d.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("prepare done")
	return nil
}

func (d *CKDeploy) Install() error {
	cmds := make([]string, 0)
	cmds = append(cmds, fmt.Sprintf("cd %s", TmpWorkDirectory))
	cmds = append(cmds, fmt.Sprintf("rpm --force -ivh %s %s %s", d.Packages[0], d.Packages[1], d.Packages[2]))
	cmds = append(cmds, fmt.Sprintf("rm -rf %s", path.Join(d.Conf.Path, "clickhouse")))
	cmds = append(cmds, fmt.Sprintf("mkdir -p %s", path.Join(d.Conf.Path, "clickhouse")))
	cmds = append(cmds, fmt.Sprintf("chown clickhouse.clickhouse %s -R", path.Join(d.Conf.Path, "clickhouse")))

	var lastError error
	for _, host := range d.Hosts {
		innerHost := host
		_ = d.Pool.Submit(func() {
			cmd1 := "systemctl stop clickhouse-server"
			_, _ = common.RemoteExecute(d.User, d.Password, innerHost, d.Port, cmd1)

			cmd2 := strings.Join(cmds, " && ")
			_, err := common.RemoteExecute(d.User, d.Password, innerHost, d.Port, cmd2)
			if err != nil {
				lastError = err
				return
			}

			log.Logger.Debugf("host %s install done", innerHost)
		})
	}
	d.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("install done")
	return nil
}

func (d *CKDeploy) Uninstall() error {
	cmds := make([]string, 0)
	for _, pack := range d.Packages {
		cmds = append(cmds, fmt.Sprintf("rpm -e %s", pack))
	}
	cmds = append(cmds, fmt.Sprintf("rm -rf %s", path.Join(d.Conf.Path, "clickhouse")))
	cmds = append(cmds, "rm -rf /etc/clickhouse-server")
	cmds = append(cmds, "rm -rf /etc/clickhouse-client")

	var lastError error
	for _, host := range d.Hosts {
		innerHost := host
		_ = d.Pool.Submit(func() {
			cmd := strings.Join(cmds, " && ")
			_, err := common.RemoteExecute(d.User, d.Password, innerHost, d.Port, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s uninstall done", innerHost)
		})
	}
	d.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("uninstall done")
	return nil
}

func (d *CKDeploy) Upgrade() error {
	cmd := fmt.Sprintf("cd %s && rpm --force -Uvh %s %s %s", TmpWorkDirectory, d.Packages[0], d.Packages[1], d.Packages[2])
	var lastError error
	for _, host := range d.Hosts {
		innerHost := host
		_ = d.Pool.Submit(func() {
			_, err := common.RemoteExecute(d.User, d.Password, innerHost, d.Port, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s upgrade done", innerHost)
		})
	}
	d.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("upgrade done")
	return nil
}

func (d *CKDeploy) Config() error {
	configTemplate := CkConfigTemplate{
		CkTcpPort:   d.Conf.CkTcpPort,
		CkHttpPort:  d.Conf.CkHttpPort,
		Path:        d.Conf.Path,
		User:        d.Conf.User,
		Password:    d.Conf.Password,
		ClusterName: d.Conf.ClusterName,
	}
	conf, err := ParseConfigTemplate("config.xml", configTemplate)
	if err != nil {
		return err
	}

	var lastError error
	for index, host := range d.Hosts {
		innerIndex := index
		innerHost := host
		_ = d.Pool.Submit(func() {
			files := make([]string, 4)
			files[0] = conf

			configTemplate.MaxMemoryPerQuery = int64((d.HostInfos[innerIndex].MemoryTotal / 2) * 1e3)
			configTemplate.MaxMemoryAllQuery = int64(((d.HostInfos[innerIndex].MemoryTotal * 3) / 4) * 1e3)
			configTemplate.MaxBytesGroupBy = int64((d.HostInfos[innerIndex].MemoryTotal / 4) * 1e3)
			user, err := ParseConfigTemplate("users.xml", configTemplate)
			if err != nil {
				lastError = err
				return
			}
			files[1] = user

			metrika, err := GenerateMetrikaTemplate("metrika.xml", d.Conf, innerHost)
			if err != nil {
				lastError = err
				return
			}
			files[2] = metrika

			macrosFile := fmt.Sprintf("macros_%s.xml", innerHost)
			macros, err := GenerateMacrosTemplate(macrosFile, d.Conf, innerHost)
			if err != nil {
				lastError = err
				return
			}
			files[3] = macros

			if err := common.ScpFiles(files, "/etc/clickhouse-server/", d.User, d.Password, innerHost, d.Port); err != nil {
				lastError = err
				return
			}

			cmd := fmt.Sprintf("rm -rf /etc/clickhouse-server/config.d/* && mv /etc/clickhouse-server/%s /etc/clickhouse-server/config.d/macros.xml", macrosFile)
			if _, err = common.RemoteExecute(d.User, d.Password, innerHost, d.Port, cmd); err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s config done", innerHost)
		})
	}
	d.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("config done")
	return nil
}

func (d *CKDeploy) Start() error {
	var lastError error
	for _, host := range d.Hosts {
		innerHost := host
		_ = d.Pool.Submit(func() {
			cmd := "systemctl start clickhouse-server"
			_, err := common.RemoteExecute(d.User, d.Password, innerHost, d.Port, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s start done", innerHost)
		})
	}
	d.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("start done")
	return nil
}

func (d *CKDeploy) Stop() error {
	var lastError error
	for _, host := range d.Hosts {
		innerHost := host
		_ = d.Pool.Submit(func() {
			cmd := "systemctl stop clickhouse-server"
			_, err := common.RemoteExecute(d.User, d.Password, innerHost, d.Port, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s stop done", innerHost)
		})
	}
	d.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("stop done")
	return nil
}

func (d *CKDeploy) Check() error {
	time.Sleep(5 * time.Second)

	var lastError error
	for _, host := range d.Hosts {
		innerHost := host
		_ = d.Pool.Submit(func() {
			db, err := common.ConnectClickHouse(innerHost, d.Conf.CkTcpPort, model.ClickHouseDefaultDB, d.Conf.User, d.Conf.Password)
			if err != nil {
				lastError = err
				return
			}
			if err = db.Ping(); err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s check done", innerHost)
		})
	}

	d.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("check done")
	return nil
}

func ParseConfigTemplate(templateFile string, conf CkConfigTemplate) (string, error) {
	localPath := path.Join(config.GetWorkDirectory(), "template", templateFile)

	data, err := ioutil.ReadFile(localPath)
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	tmpl, err := template.New("tmpl").Parse(string(data))
	if err != nil {
		return "", err
	}
	if err := tmpl.Execute(buf, conf); err != nil {
		return "", err
	}

	tmplFile := path.Join(config.GetWorkDirectory(), "package", templateFile)
	localFd, err := os.OpenFile(tmplFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return "", err
	}
	defer localFd.Close()

	if _, err := localFd.Write(buf.Bytes()); err != nil {
		return "", err
	}

	return tmplFile, nil
}

func GenerateMetrikaTemplate(templateFile string, conf *model.CkDeployConfig, hostIp string) (string, error) {
	zkServers := make([]Node, 0)
	ckServers := make([]Cluster, 0)

	// zookeeper-servers
	for index, host := range conf.ZkNodes {
		node := Node{
			Index: index + 1,
			Host:  host,
			Port:  conf.ZkPort,
		}
		zkServers = append(zkServers, node)
	}

	// clickhouse_remote_servers
	shards := make([]Shard, 0)
	for _, shard := range conf.Shards {
		replicas := make([]Replica, 0)

		for _, replica := range shard.Replicas {
			rp := Replica{
				Host: replica.HostName,
				Port: conf.CkTcpPort,
			}
			replicas = append(replicas, rp)
		}

		sh := Shard{
			Replication: conf.IsReplica,
			Replicas:    replicas,
		}
		shards = append(shards, sh)
	}
	ck := Cluster{
		XMLName: xml.Name{Local: conf.ClusterName},
		Shards:  shards,
	}
	ckServers = append(ckServers, ck)

	metrika := Metrika{
		ZkServers: zkServers,
		CkServers: ckServers,
	}
	output, err := xml.MarshalIndent(metrika, "", "  ")
	if err != nil {
		return "", err
	}

	tmplFile := path.Join(config.GetWorkDirectory(), "package", templateFile)
	localFd, err := os.OpenFile(tmplFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return "", err
	}
	defer localFd.Close()

	if _, err := localFd.Write(output); err != nil {
		return "", err
	}

	return tmplFile, nil
}

func GenerateMacrosTemplate(templateFile string, conf *model.CkDeployConfig, hostIp string) (string, error) {
	shardIndex := 0
	hostName := ""
	for i, shard := range conf.Shards {
		for _, replica := range shard.Replicas {
			if hostIp == replica.Ip {
				shardIndex = i + 1
				hostName = replica.HostName
				break
			}
		}
	}

	// macros
	macros := Macro{
		Cluster: conf.ClusterName,
		Shard:   shardIndex,
		Replica: hostName,
	}

	macroConf := MacroConf{
		Macros: macros,
	}

	output, err := xml.MarshalIndent(macroConf, "", "  ")
	if err != nil {
		return "", err
	}

	tmplFile := path.Join(config.GetWorkDirectory(), "package", templateFile)
	localFd, err := os.OpenFile(tmplFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return "", err
	}
	defer localFd.Close()

	if _, err := localFd.Write(output); err != nil {
		return "", err
	}

	return tmplFile, nil
}

func UpgradeCkCluster(conf *model.CKManClickHouseConfig, version string) error {
	packages := make([]string, 3)
	packages[0] = fmt.Sprintf("%s-%s-%s", model.CkCommonPackagePrefix, version, model.CkCommonPackageSuffix)
	packages[1] = fmt.Sprintf("%s-%s-%s", model.CkServerPackagePrefix, version, model.CkServerPackageSuffix)
	packages[2] = fmt.Sprintf("%s-%s-%s", model.CkClientPackagePrefix, version, model.CkClientPackageSuffix)
	deploy := &CKDeploy{
		DeployBase: DeployBase{
			Hosts:    conf.Hosts,
			User:     conf.SshUser,
			Password: conf.SshPassword,
			Port:     conf.SshPort,
			Packages: packages,
			Pool:     common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault),
		},
		Conf: &model.CkDeployConfig{
			CkHttpPort: conf.HttpPort,
			CkTcpPort:  conf.Port,
			User:       conf.User,
			Password:   conf.Password,
		},
	}
	if err := deploy.Stop(); err != nil {
		return err
	}
	log.Logger.Infof("cluster stopped succeed ")
	if err := deploy.Prepare(); err != nil {
		return err
	}
	log.Logger.Infof("cluster prepared succeed ")
	if err := deploy.Upgrade(); err != nil {
		return err
	}
	log.Logger.Infof("cluster upgrade succeed ")
	if err := deploy.Config(); err != nil {
		return err
	}
	log.Logger.Infof("cluster config succeed ")
	if err := deploy.Start(); err != nil {
		return err
	}
	log.Logger.Infof("cluster start succeed ")
	if err := deploy.Check(); err != nil {
		return err
	}
	log.Logger.Infof("cluster checked succeed ")

	return nil
}

func StartCkCluster(conf *model.CKManClickHouseConfig) error {

	statusList := clickhouse.GetCkClusterStatus(conf)
	var chHosts []string
	for _, status := range statusList {
		if status.Status == model.CkStatusRed {
			chHosts = append(chHosts, status.Ip)
		}
	}

	if len(chHosts) == 0 {
		return nil
	}

	deploy := &CKDeploy{
		DeployBase: DeployBase{
			Hosts:    chHosts,
			User:     conf.SshUser,
			Password: conf.SshPassword,
			Port:     conf.SshPort,
			Pool:     common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault),
		},
	}

	if err := deploy.Start(); err != nil {
		return err
	}
	if err := deploy.Check(); err != nil {
		return err
	}
	return nil
}

func StopCkCluster(conf *model.CKManClickHouseConfig) error {
	deploy := &CKDeploy{
		DeployBase: DeployBase{
			Hosts:    conf.Hosts,
			User:     conf.SshUser,
			Password: conf.SshPassword,
			Port:     conf.SshPort,
			Pool:     common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault),
		},
	}

	return deploy.Stop()
}

func DestroyCkCluster(conf *model.CKManClickHouseConfig) error {
	packages := make([]string, 3)
	packages[0] = fmt.Sprintf("%s-%s", model.CkClientPackagePrefix, conf.Version)
	packages[1] = fmt.Sprintf("%s-%s", model.CkServerPackagePrefix, conf.Version)
	packages[2] = fmt.Sprintf("%s-%s", model.CkCommonPackagePrefix, conf.Version)
	deploy := &CKDeploy{
		DeployBase: DeployBase{
			Hosts:    conf.Hosts,
			User:     conf.SshUser,
			Password: conf.SshPassword,
			Port:     conf.SshPort,
			Packages: packages,
			Pool:     common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault),
		},
		Conf: &model.CkDeployConfig{
			Path: conf.Path,
		},
	}
	if err := deploy.Stop(); err != nil {
		return err
	}
	if err := deploy.Uninstall(); err != nil {
		return err
	}

	//clear zkNode
	service, err := zookeeper.NewZkService(conf.ZkNodes, conf.ZkPort)
	if err != nil {
		return err
	}
	//delete from standard path
	stdZooPath := fmt.Sprintf("/clickhouse/tables/%s", conf.Cluster)
	if err := service.DeleteAll(stdZooPath); err != nil {
		return err
	}
	zooPaths := clickhouse.ConvertZooPath(conf)
	if len(zooPaths) > 0 {
		for _, zooPath := range zooPaths {
			if err := service.DeleteAll(zooPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func AddCkClusterNode(conf *model.CKManClickHouseConfig, req *model.AddNodeReq) error {
	maxShardNum := len(conf.Shards)
	if !conf.IsReplica && req.Shard != maxShardNum+1 {
		return errors.Errorf("It's not allow to add replica node for shard%d while IsReplica is false", req.Shard)
	}

	// add the node to conf struct
	for _, host := range conf.Hosts {
		if host == req.Ip {
			return errors.Errorf("node ip %s is duplicate", req.Ip)
		}
	}

	replicaIndex := 0
	shards := make([]model.CkShard, len(conf.Shards))
	copy(shards, conf.Shards)
	if len(shards) >= req.Shard {
		replica := model.CkReplica{
			Ip: req.Ip,
		}
		replicaIndex = len(shards[req.Shard-1].Replicas)
		shards[req.Shard-1].Replicas = append(shards[req.Shard-1].Replicas, replica)
	} else if len(shards)+1 == req.Shard {
		replica := model.CkReplica{
			Ip: req.Ip,
		}
		shard := model.CkShard{
			Replicas: []model.CkReplica{replica},
		}
		shards = append(shards, shard)
	} else {
		return errors.Errorf("shard number %d is incorrect", req.Shard)
	}

	// install clickhouse and start service on the new node
	packages := make([]string, 3)
	packages[0] = fmt.Sprintf("%s-%s-%s", model.CkCommonPackagePrefix, conf.Version, model.CkCommonPackageSuffix)
	packages[1] = fmt.Sprintf("%s-%s-%s", model.CkServerPackagePrefix, conf.Version, model.CkServerPackageSuffix)
	packages[2] = fmt.Sprintf("%s-%s-%s", model.CkClientPackagePrefix, conf.Version, model.CkClientPackageSuffix)
	deploy := &CKDeploy{}
	base := &DeployBase{
		Hosts:    []string{req.Ip},
		User:     conf.SshUser,
		Password: conf.SshPassword,
		Port:     conf.SshPort,
		Packages: packages,
		Pool:     common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault),
	}
	con := &model.CkDeployConfig{
		Path:           conf.Path,
		User:           conf.User,
		Password:       conf.Password,
		ZkNodes:        conf.ZkNodes,
		ZkPort:         conf.ZkPort,
		ClusterName:    conf.Cluster,
		Shards:         shards,
		PackageVersion: conf.Version,
		CkTcpPort:      conf.Port,
		CkHttpPort:     conf.HttpPort,
		IsReplica:      conf.IsReplica,
	}
	if err := deploy.Init(base, con); err != nil {
		return err
	}
	if err := deploy.Prepare(); err != nil {
		return err
	}
	if err := deploy.Install(); err != nil {
		return err
	}
	if err := deploy.Config(); err != nil {
		return err
	}
	if err := deploy.Start(); err != nil {
		return err
	}
	if err := deploy.Check(); err != nil {
		return err
	}

	// update other nodes config
	deploy = &CKDeploy{}
	base = &DeployBase{
		Hosts:    conf.Hosts,
		User:     conf.SshUser,
		Password: conf.SshPassword,
		Port:     conf.SshPort,
		Pool:     common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault),
	}
	con = &model.CkDeployConfig{
		Path:           conf.Path,
		User:           conf.User,
		Password:       conf.Password,
		ZkNodes:        conf.ZkNodes,
		ZkPort:         conf.ZkPort,
		ClusterName:    conf.Cluster,
		Shards:         shards,
		PackageVersion: conf.Version,
		CkTcpPort:      conf.Port,
		CkHttpPort:     conf.HttpPort,
		IsReplica:      conf.IsReplica,
	}
	if err := deploy.Init(base, con); err != nil {
		return err
	}
	if err := deploy.Config(); err != nil {
		return err
	}

	conf.Shards = shards
	conf.Hosts = append(conf.Hosts, req.Ip)
	conf.Names = append(conf.Names, shards[req.Shard-1].Replicas[replicaIndex].HostName)
	return nil
}

func DeleteCkClusterNode(conf *model.CKManClickHouseConfig, ip string) error {
	// If the cluster just have 1 replica in shard, and the shard number not the biggest, we don't allow to delete it.
	available := false
	ifDeleteShard := false
	shardNum := 0
	replicaNum := 0
	var err error
	for i, shard := range conf.Shards {
		for j, replica := range shard.Replicas {
			if replica.Ip == ip {
				shardNum = i
				replicaNum = j
				available = true
				if i+1 == len(conf.Shards) {
					if len(shard.Replicas) == 1 {
						ifDeleteShard = true
					}
				} else {
					if len(shard.Replicas) == 1 {
						err = fmt.Errorf("can't delete node which only 1 replica in shard")
					}
				}
				break
			}
		}
	}

	if !available {
		err = fmt.Errorf("can't find this ip in cluster")
	}

	if err != nil {
		log.Logger.Errorf("can't delete this node: %v", err)
		return err
	}

	//delete zookeeper path if need
	service, err := zookeeper.NewZkService(conf.ZkNodes, conf.ZkPort)
	if err != nil {
		return err
	}
	_ = clickhouse.GetReplicaZkPath(conf)
	var zooPaths []string
	for _, path := range conf.ZooPath {
		zooPath := strings.Replace(path, "{cluster}", conf.Cluster, -1)
		zooPath = strings.Replace(zooPath, "{shard}", fmt.Sprintf("%d", shardNum+1), -1)
		zooPaths = append(zooPaths, zooPath)
	}

	for _, path := range zooPaths {
		if ifDeleteShard {
			//delete the shard
			shardNode := fmt.Sprintf("%d", shardNum+1)
			err := service.DeletePathUntilNode(path, shardNode)
			if err != nil {
				return err
			}
		} else {
			// delete replica path
			replicaName := conf.Shards[shardNum].Replicas[replicaNum].HostName
			replicaPath := fmt.Sprintf("%s/replicas/%s", path, replicaName)
			log.Logger.Debugf("replicaPath: %s", replicaPath)
			err := service.DeleteAll(replicaPath)
			if err != nil {
				return err
			}
		}
	}

	index := 0
	for index < len(conf.Hosts) {
		if conf.Hosts[index] == ip {
			break
		}
		index++
	}

	// stop the node
	deploy := &CKDeploy{
		DeployBase: DeployBase{
			Hosts:    []string{ip},
			User:     conf.SshUser,
			Password: conf.SshPassword,
			Port:     conf.SshPort,
			Pool:     common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault),
		},
	}
	if err := deploy.Stop(); err != nil {
		log.Logger.Warnf("can't stop node %s, ignore it", ip)
	}

	// remove the node from conf struct
	hosts := append(conf.Hosts[:index], conf.Hosts[index+1:]...)
	names := append(conf.Names[:index], conf.Names[index+1:]...)
	shards := make([]model.CkShard, len(conf.Shards))
	copy(shards, conf.Shards)
	for i, shard := range shards {
		found := false
		for j, replica := range shard.Replicas {
			if replica.Ip == ip {
				found = true
				if len(shard.Replicas) > 1 {
					shards[i].Replicas = append(shards[i].Replicas[:j], shards[i].Replicas[j+1:]...)
				} else {
					shards = append(shards[:i], shards[i+1:]...)
				}
				break
			}
		}
		if found {
			break
		}
	}

	// update other nodes config
	deploy = &CKDeploy{}
	base := &DeployBase{
		Hosts:    hosts,
		User:     conf.SshUser,
		Password: conf.SshPassword,
		Port:     conf.SshPort,
		Pool:     common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault),
	}
	con := &model.CkDeployConfig{
		Path:           conf.Path,
		User:           conf.User,
		Password:       conf.Password,
		ZkNodes:        conf.ZkNodes,
		ZkPort:         conf.ZkPort,
		ClusterName:    conf.Cluster,
		Shards:         shards,
		PackageVersion: conf.Version,
		CkTcpPort:      conf.Port,
		CkHttpPort:     conf.HttpPort,
		IsReplica:      conf.IsReplica,
	}
	if err := deploy.Init(base, con); err != nil {
		return err
	}
	if err := deploy.Config(); err != nil {
		return err
	}

	conf.Hosts = hosts
	conf.Names = names
	conf.Shards = shards
	return nil
}

//func updateMetrikaconfig(user, password, host, clusterName string, port, sshPort int, param CkUpdateNodeParam) error {
//	templateFile := "metrika.xml"
//	confFile := "/etc/clickhouse-server/metrika.xml"
//
//	client, err := common.SSHConnect(user, password, host, sshPort)
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//
//	cmd := fmt.Sprintf("cat %s", confFile)
//	output, err := common.SSHRun(client, cmd)
//	if err != nil {
//		log.Logger.Errorf("run '%s' on host %s fail: %s", cmd, host, output)
//		return err
//	}
//
//	metrika := Metrika{}
//	err = xml.Unmarshal([]byte(output), &metrika)
//	if err != nil {
//		return err
//	}
//
//	found := false
//	for index, cluster := range metrika.CkServers {
//		if cluster.XMLName.Local == clusterName {
//			found = true
//			shards := metrika.CkServers[index].Shards
//			if param.Op == CkClusterNodeAdd {
//				if len(shards) <= param.Shard {
//					shard := shards[param.Shard-1]
//					shard.Replication = true
//					shard.Replicas = append(shard.Replicas, Replica{
//						Host: param.Ip,
//						Port: port,
//					})
//				} else {
//					replicas := make([]Replica, 1)
//					replica := Replica{
//						Host: param.Ip,
//						Port: port,
//					}
//					replicas[0] = replica
//					shard := Shard{
//						Replication: false,
//						Replicas:    replicas,
//					}
//					shards = append(shards, shard)
//				}
//			} else if param.Op == CkClusterNodeDelete {
//				for i, shard := range shards {
//					found := false
//					for j, replica := range shard.Replicas {
//						if replica.Host == param.Hostname || replica.Host == param.Ip {
//							found = true
//							if len(shard.Replicas) > 1 {
//								shards[i].Replicas = append(shard.Replicas[:j], shard.Replicas[j+1:]...)
//								if len(shards[i].Replicas) == 1 {
//									shards[i].Replication = false
//								}
//							} else {
//								metrika.CkServers[index].Shards = append(shards[:i], shards[i+1:]...)
//							}
//							break
//						}
//					}
//					if found {
//						break
//					}
//				}
//			} else {
//				return errors.Errorf("unsupported operate %d", param.Op)
//			}
//			break
//		}
//	}
//	if !found {
//		return errors.Errorf("can't find cluster")
//	}
//
//	data, err := xml.MarshalIndent(metrika, "", "  ")
//	if err != nil {
//		return err
//	}
//	tmplFile := path.Join(config.GetWorkDirectory(), "package", templateFile)
//	localFd, err := os.OpenFile(tmplFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
//	if err != nil {
//		return err
//	}
//	defer localFd.Close()
//	if _, err := localFd.Write(data); err != nil {
//		return err
//	}
//	if err := common.ScpFiles([]string{tmplFile}, confFile, user, password, host, sshPort); err != nil {
//		return err
//	}
//
//	return nil
//}

func ensureHosts(d *CKDeploy) error {
	addresses := make([]string, 0)
	hosts := make([]string, 0)
	tmplFile := path.Join(config.GetWorkDirectory(), "package", "hosts")

	for _, shard := range d.Conf.Shards {
		for _, replica := range shard.Replicas {
			addresses = append(addresses, replica.Ip)
			hosts = append(hosts, replica.HostName)
		}
	}

	var lastError error
	d.Pool.Restart()
	for _, host := range d.Hosts {
		innerHost := host
		_ = d.Pool.Submit(func() {
			if err := common.ScpDownloadFiles([]string{"/etc/hosts"}, path.Join(config.GetWorkDirectory(), "package"), d.User, d.Password, innerHost, d.Port); err != nil {
				lastError = err
				return
			}
			h, err := common.NewHosts(tmplFile, tmplFile)
			if err != nil {
				lastError = err
				return
			}
			if err := common.AddHosts(h, addresses, hosts); err != nil {
				lastError = err
				return
			}
			_ = common.Save(h)
			if err := common.ScpFiles([]string{tmplFile}, "/etc/", d.User, d.Password, innerHost, d.Port); err != nil {
				lastError = err
				return
			}
		})
	}
	d.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	return nil
}
