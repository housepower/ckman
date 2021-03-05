package deploy

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"github.com/housepower/ckman/config"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

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
	Macros    Macro     `xml:"macros"`
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
		return fmt.Errorf("value isn't type of CkDeployConfig")
	}

	d.DeployBase = *base
	d.Conf = c
	if d.Conf.User == "" {
		d.Conf.User = clickhouse.ClickHouseDefaultUser
	}
	if d.Conf.Password == "" {
		d.Conf.Password = clickhouse.ClickHouseDefaultPassword
	}
	if d.Conf.CkTcpPort == 0 {
		d.Conf.CkTcpPort = clickhouse.ClickHouseDefaultPort
	}
	d.HostInfos = make([]HostInfo, len(d.Hosts))
	HostNameMap := make(map[string]bool)

	for index, host := range d.Hosts {
		err := func() error {
			client, err := common.SSHConnect(d.User, d.Password, host, 22)
			if err != nil {
				return err
			}
			defer client.Close()

			cmd := "cat /proc/meminfo | grep MemTotal | awk '{print $2}'"
			output, err := common.SSHRun(client, cmd)
			if err != nil {
				log.Logger.Errorf("run '%s' on host %s fail: %s", cmd, host, output)
				return err
			}

			memory := strings.Trim(output, "\n")
			total, err := strconv.Atoi(memory)
			if err != nil {
				return err
			}

			info := HostInfo{
				MemoryTotal: total,
			}
			d.HostInfos[index] = info
			return nil
		}()
		if err != nil {
			return err
		}
	}

	clusterNodeNum := 0
	for i, shard := range d.Conf.Shards {
		for j, replica := range shard.Replicas {
			err := func(shardIndex, replicaIndex int) error {
				client, err := common.SSHConnect(d.User, d.Password, replica.Ip, 22)
				if err != nil {
					return err
				}
				defer client.Close()

				cmd := "hostname -f"
				output, err := common.SSHRun(client, cmd)
				if err != nil {
					log.Logger.Errorf("run '%s' on host %s fail: %s", cmd, replica.Ip, output)
					return err
				}

				hostname := strings.Trim(output, "\n")
				d.Conf.Shards[shardIndex].Replicas[replicaIndex].HostName = hostname
				HostNameMap[hostname] = true
				clusterNodeNum++
				return nil
			}(i, j)
			if err != nil {
				return err
			}
		}
	}

	if len(HostNameMap) != clusterNodeNum {
		return fmt.Errorf("host name are the same")
	}

	if err := ensureHosts(d); err != nil {
		return err
	}

	return nil
}

func (d *CKDeploy) Prepare() error {
	files := make([]string, 0)
	for _, file := range d.Packages {
		files = append(files, path.Join(config.GetWorkDirectory(), "package", file))
	}

	var wg sync.WaitGroup
	wg.Add(len(d.Hosts))
	ch := make(chan error, len(d.Hosts))
	for _, host := range d.Hosts {
		go func(host string, ch chan error) {
			defer wg.Done()
			if err := common.ScpFiles(files, TmpWorkDirectory, d.User, d.Password, host); err != nil {
				ch <- err
			}
		}(host, ch)
	}
	wg.Wait()
	close(ch)
	for err := range ch {
		return err
	}

	return nil
}

func (d *CKDeploy) Install() error {
	cmds := make([]string, 0)
	cmds = append(cmds, fmt.Sprintf("cd %s", TmpWorkDirectory))
	cmds = append(cmds, fmt.Sprintf("rpm --force -ivh %s %s %s", d.Packages[0], d.Packages[1], d.Packages[2]))
	cmds = append(cmds, fmt.Sprintf("rm -rf %s", path.Join(d.Conf.Path, "clickhouse")))
	cmds = append(cmds, fmt.Sprintf("mkdir -p %s", path.Join(d.Conf.Path, "clickhouse")))
	cmds = append(cmds, fmt.Sprintf("chown clickhouse.clickhouse %s -R", path.Join(d.Conf.Path, "clickhouse")))

	var wg sync.WaitGroup
	wg.Add(len(d.Hosts))
	ch := make(chan error, len(d.Hosts))
	for _, host := range d.Hosts {
		go func(host string, ch chan error) {
			defer wg.Done()
			client, err := common.SSHConnect(d.User, d.Password, host, 22)
			if err != nil {
				ch <- err
				return
			}
			defer client.Close()

			common.SSHRun(client, "systemctl stop clickhouse-server")
			cmd := strings.Join(cmds, " && ")
			if output, err := common.SSHRun(client, cmd); err != nil {
				log.Logger.Errorf("run '%s' on host %s fail: %s", cmd, host, output)
				ch <- err
				return
			}
		}(host, ch)
	}
	wg.Wait()
	close(ch)
	for err := range ch {
		return err
	}

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

	for _, host := range d.Hosts {
		err := func() error {
			client, err := common.SSHConnect(d.User, d.Password, host, 22)
			if err != nil {
				return err
			}
			defer client.Close()

			cmd := strings.Join(cmds, " && ")
			if output, err := common.SSHRun(client, cmd); err != nil {
				log.Logger.Errorf("run '%s' on host %s fail: %s", cmd, host, output)
				return err
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *CKDeploy) Upgrade() error {
	cmd := fmt.Sprintf("cd %s && rpm --force -Uvh %s %s %s", TmpWorkDirectory, d.Packages[0], d.Packages[1], d.Packages[2])

	for _, host := range d.Hosts {
		err := func() error {
			client, err := common.SSHConnect(d.User, d.Password, host, 22)
			if err != nil {
				return err
			}
			defer client.Close()

			if output, err := common.SSHRun(client, cmd); err != nil {
				log.Logger.Errorf("run '%s' on host %s fail: %s", cmd, host, output)
				return err
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *CKDeploy) Config() error {
	configTemplate := CkConfigTemplate{
		CkTcpPort:   d.Conf.CkTcpPort,
		Path:        d.Conf.Path,
		User:        d.Conf.User,
		Password:    d.Conf.Password,
		ClusterName: d.Conf.ClusterName,
	}
	conf, err := ParseConfigTemplate("config.xml", configTemplate)
	if err != nil {
		return err
	}

	for index, host := range d.Hosts {
		files := make([]string, 3)
		files[0] = conf

		configTemplate.MaxMemoryPerQuery = int64((d.HostInfos[index].MemoryTotal / 2) * 1e3)
		configTemplate.MaxMemoryAllQuery = int64(((d.HostInfos[index].MemoryTotal * 3) / 4) * 1e3)
		configTemplate.MaxBytesGroupBy = int64((d.HostInfos[index].MemoryTotal / 4) * 1e3)
		user, err := ParseConfigTemplate("users.xml", configTemplate)
		if err != nil {
			return err
		}
		files[1] = user

		metrika, err := GenerateMetrikaTemplate("metrika.xml", d.Conf, host)
		if err != nil {
			return err
		}
		files[2] = metrika

		if err := common.ScpFiles(files, "/etc/clickhouse-server/", d.User, d.Password, host); err != nil {
			return err
		}
	}

	return nil
}

func (d *CKDeploy) Start() error {
	for _, host := range d.Hosts {
		err := func() error {
			client, err := common.SSHConnect(d.User, d.Password, host, 22)
			if err != nil {
				return err
			}
			defer client.Close()

			cmd := "systemctl start clickhouse-server"
			if output, err := common.SSHRun(client, cmd); err != nil {
				log.Logger.Errorf("run '%s' on host %s fail: %s", cmd, host, output)
				return err
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *CKDeploy) Stop() error {
	for _, host := range d.Hosts {
		err := func() error {
			client, err := common.SSHConnect(d.User, d.Password, host, 22)
			if err != nil {
				return err
			}
			defer client.Close()

			cmd := "systemctl stop clickhouse-server"
			if output, err := common.SSHRun(client, cmd); err != nil {
				log.Logger.Errorf("run '%s' on host %s fail: %s", cmd, host, output)
				return err
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *CKDeploy) Restart() error {
	for _, host := range d.Hosts {
		err := func() error {
			client, err := common.SSHConnect(d.User, d.Password, host, 22)
			if err != nil {
				return err
			}
			defer client.Close()

			cmd := "systemctl restart clickhouse-server"
			if output, err := common.SSHRun(client, cmd); err != nil {
				log.Logger.Errorf("run '%s' on host %s fail: %s", cmd, host, output)
				return err
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *CKDeploy) Check() error {
	time.Sleep(5 * time.Second)

	conf := model.CKManClickHouseConfig{
		Hosts:    d.Hosts,
		Port:     d.Conf.CkTcpPort,
		User:     d.Conf.User,
		Password: d.Conf.Password,
		DB:       "default",
	}

	svr := clickhouse.NewCkService(&conf)
	defer svr.Stop()

	if err := svr.InitCkService(); err != nil {
		return err
	}

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
	shardIndex := 0
	hostName := ""
	for i, shard := range conf.Shards {
		isReplica := false
		replicas := make([]Replica, 0)
		if len(shard.Replicas) > 1 {
			isReplica = true
		}

		for _, replica := range shard.Replicas {
			if hostIp == replica.Ip {
				shardIndex = i + 1
				hostName = replica.HostName
			}
			rp := Replica{
				Host: replica.HostName,
				Port: conf.CkTcpPort,
			}
			replicas = append(replicas, rp)
		}

		sh := Shard{
			Replication: isReplica,
			Replicas:    replicas,
		}
		shards = append(shards, sh)
	}
	ck := Cluster{
		XMLName: xml.Name{Local: conf.ClusterName},
		Shards:  shards,
	}
	ckServers = append(ckServers, ck)

	// macros
	macros := Macro{
		Cluster: conf.ClusterName,
		Shard:   shardIndex,
		Replica: hostName,
	}

	metrika := Metrika{
		ZkServers: zkServers,
		CkServers: ckServers,
		Macros:    macros,
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
			Packages: packages,
		},
	}
	if err := deploy.Stop(); err != nil {
		return err
	}
	if err := deploy.Prepare(); err != nil {
		return err
	}
	if err := deploy.Upgrade(); err != nil {
		return err
	}
	if err := deploy.Start(); err != nil {
		return err
	}
	if err := deploy.Check(); err != nil {
		return err
	}

	return nil
}

func StartCkCluster(conf *model.CKManClickHouseConfig) error {
	deploy := &CKDeploy{
		DeployBase: DeployBase{
			Hosts:    conf.Hosts,
			User:     conf.SshUser,
			Password: conf.SshPassword,
		},
	}

	return deploy.Start()
}

func StopCkCluster(conf *model.CKManClickHouseConfig) error {
	deploy := &CKDeploy{
		DeployBase: DeployBase{
			Hosts:    conf.Hosts,
			User:     conf.SshUser,
			Password: conf.SshPassword,
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
			Packages: packages,
		},
		Conf: &model.CkDeployConfig{
			Path:conf.Path,
		},
	}
	if err := deploy.Stop(); err != nil {
		return err
	}
	if err := deploy.Uninstall(); err != nil {
		return err
	}

	return nil
}

func AddCkClusterNode(conf *model.CKManClickHouseConfig, req *model.AddNodeReq) error {
	// add the node to conf struct
	for _, host := range conf.Hosts {
		if host == req.Ip {
			return fmt.Errorf("node ip %s is duplicate", req.Ip)
		}
	}

	replicaIndex := 0
	isReplica := conf.IsReplica
	shards := make([]model.CkShard, len(conf.Shards))
	copy(shards, conf.Shards)
	if len(shards) >= req.Shard {
		isReplica = true
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
		return fmt.Errorf("shard number %d is incorrect", req.Shard)
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
		Packages: packages,
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

	// update other nodes config and restart clickhouse service
	deploy = &CKDeploy{}
	base = &DeployBase{
		Hosts:    conf.Hosts,
		User:     conf.SshUser,
		Password: conf.SshPassword,
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
	}
	if err := deploy.Init(base, con); err != nil {
		return err
	}
	if err := deploy.Config(); err != nil {
		return err
	}
	if err := deploy.Restart(); err != nil {
		return err
	}
	if err := deploy.Check(); err != nil {
		return err
	}

	conf.Shards = shards
	conf.Hosts = append(conf.Hosts, req.Ip)
	conf.Names = append(conf.Names, shards[req.Shard-1].Replicas[replicaIndex].HostName)
	conf.IsReplica = isReplica
	return nil
}

func DeleteCkClusterNode(conf *model.CKManClickHouseConfig, ip string) error {
	// find the node index
	index := 0
	for index < len(conf.Hosts) {
		if conf.Hosts[index] == ip {
			break
		}
		index++
	}
	if index >= len(conf.Hosts) {
		return fmt.Errorf("can'f find node %s on cluster %s", ip, conf.Cluster)
	}

	// stop the node
	deploy := &CKDeploy{
		DeployBase: DeployBase{
			Hosts:    []string{ip},
			User:     conf.SshUser,
			Password: conf.SshPassword,
		},
	}
	if err := deploy.Stop(); err != nil {
		return err
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
	isReplica := false
	for _, shard := range shards {
		if len(shard.Replicas) > 1 {
			isReplica = true
			break
		}
	}

	// update other nodes config and restart clickhouse service
	deploy = &CKDeploy{}
	base := &DeployBase{
		Hosts:    hosts,
		User:     conf.SshUser,
		Password: conf.SshPassword,
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
	}
	if err := deploy.Init(base, con); err != nil {
		return err
	}
	if err := deploy.Config(); err != nil {
		return err
	}
	if err := deploy.Restart(); err != nil {
		return err
	}
	if err := deploy.Check(); err != nil {
		return err
	}

	conf.Hosts = hosts
	conf.Names = names
	conf.Shards = shards
	conf.IsReplica = isReplica
	return nil
}

func updateMetrikaconfig(user, password, host, clusterName string, port int, param CkUpdateNodeParam) error {
	templateFile := "metrika.xml"
	confFile := "/etc/clickhouse-server/metrika.xml"

	client, err := common.SSHConnect(user, password, host, 22)
	if err != nil {
		return err
	}
	defer client.Close()

	cmd := fmt.Sprintf("cat %s", confFile)
	output, err := common.SSHRun(client, cmd)
	if err != nil {
		log.Logger.Errorf("run '%s' on host %s fail: %s", cmd, host, output)
		return err
	}

	metrika := Metrika{}
	err = xml.Unmarshal([]byte(output), &metrika)
	if err != nil {
		return err
	}

	found := false
	for index, cluster := range metrika.CkServers {
		if cluster.XMLName.Local == clusterName {
			found = true
			shards := metrika.CkServers[index].Shards
			if param.Op == CkClusterNodeAdd {
				if len(shards) <= param.Shard {
					shard := shards[param.Shard-1]
					shard.Replication = true
					shard.Replicas = append(shard.Replicas, Replica{
						Host: param.Ip,
						Port: port,
					})
				} else {
					replicas := make([]Replica, 1)
					replica := Replica{
						Host: param.Ip,
						Port: port,
					}
					replicas[0] = replica
					shard := Shard{
						Replication: false,
						Replicas:    replicas,
					}
					shards = append(shards, shard)
				}
			} else if param.Op == CkClusterNodeDelete {
				for i, shard := range shards {
					found := false
					for j, replica := range shard.Replicas {
						if replica.Host == param.Hostname || replica.Host == param.Ip {
							found = true
							if len(shard.Replicas) > 1 {
								shards[i].Replicas = append(shard.Replicas[:j], shard.Replicas[j+1:]...)
								if len(shards[i].Replicas) == 1 {
									shards[i].Replication = false
								}
							} else {
								metrika.CkServers[index].Shards = append(shards[:i], shards[i+1:]...)
							}
							break
						}
					}
					if found {
						break
					}
				}
			} else {
				return fmt.Errorf("unsupported operate %d", param.Op)
			}
			break
		}
	}
	if !found {
		return fmt.Errorf("can't find cluster")
	}

	data, err := xml.MarshalIndent(metrika, "", "  ")
	if err != nil {
		return err
	}
	tmplFile := path.Join(config.GetWorkDirectory(), "package", templateFile)
	localFd, err := os.OpenFile(tmplFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer localFd.Close()
	if _, err := localFd.Write(data); err != nil {
		return err
	}
	if err := common.ScpFiles([]string{tmplFile}, confFile, user, password, host); err != nil {
		return err
	}

	return nil
}

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

	for _, host := range d.Hosts {
		if err := common.ScpDownloadFiles([]string{"/etc/hosts"}, path.Join(config.GetWorkDirectory(), "package"), d.User, d.Password, host); err != nil {
			return err
		}
		h, err := common.NewHosts(tmplFile, tmplFile)
		if err != nil {
			return err
		}
		if err := common.AddHosts(h, addresses, hosts); err != nil {
			return err
		}
		common.Save(h)
		if err := common.ScpFiles([]string{tmplFile}, "/etc/", d.User, d.Password, host); err != nil {
			return err
		}
	}

	return nil
}
