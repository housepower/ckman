package deploy

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"text/template"
	"time"

	"gitlab.eoitek.net/EOI/ckman/common"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/log"
	"gitlab.eoitek.net/EOI/ckman/model"
	"gitlab.eoitek.net/EOI/ckman/service/clickhouse"
)

const (
	TmpWorkDirectory string = "/tmp/"
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
	Repicas     []Repica
}

type Repica struct {
	XMLName xml.Name `xml:"replica"`
	Host    string   `xml:"host"`
	Port    int      `xml:"port"`
}

type Macro struct {
	Cluster string `xml:"cluster"`
	Shard   int    `xml:"shard"`
	Repica  string `xml:"replica"`
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
	Ip          string
	HostName    string
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
	if d.Conf.IsReplica && len(d.Hosts)%2 != 0 {
		return fmt.Errorf("hosts length is not even number")
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

			cmd := "hostname -f && free -g"
			output, err := common.SSHRun(client, cmd)
			if err != nil {
				log.Logger.Errorf("run '%s' on host %s fail: %s", cmd, host, output)
				return err
			}

			hostname := strings.Split(output, "\n")[0]
			memory := strings.Replace(strings.Split(output, "\n")[2][7:19], " ", "", -1)
			total, err := strconv.Atoi(memory)
			if err != nil {
				return err
			}

			info := HostInfo{
				Ip:          host,
				HostName:    hostname,
				MemoryTotal: total,
			}
			d.HostInfos[index] = info
			HostNameMap[hostname] = true
			return nil
		}()
		if err != nil {
			return err
		}
	}

	if len(HostNameMap) != len(d.Hosts) {
		return fmt.Errorf("host name are the same")
	}

	return nil
}

func (d *CKDeploy) Prepare() error {
	files := make([]string, 0)
	for _, file := range d.Packages {
		files = append(files, path.Join(common.GetWorkDirectory(), "package", file))
	}

	for _, host := range d.Hosts {
		if err := common.ScpFiles(files, TmpWorkDirectory, d.User, d.Password, host); err != nil {
			return err
		}
	}

	return nil
}

func (d *CKDeploy) Install() error {
	cmds := make([]string, 0)
	cmds = append(cmds, fmt.Sprintf("cd %s", TmpWorkDirectory))
	for _, pack := range d.Packages {
		cmds = append(cmds, fmt.Sprintf("rpm -ivh %s", pack))
	}
	cmds = append(cmds, fmt.Sprintf("mkdir -p %s", path.Join(d.Conf.Path, "clickhouse")))
	cmds = append(cmds, fmt.Sprintf("chown clickhouse.clickhouse %s -R", path.Join(d.Conf.Path, "clickhouse")))

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

		configTemplate.MaxMemoryPerQuery = int64((d.HostInfos[index].MemoryTotal / 2) * 1e9)
		configTemplate.MaxMemoryAllQuery = int64(((d.HostInfos[index].MemoryTotal * 3) / 4) * 1e9)
		configTemplate.MaxBytesGroupBy = int64((d.HostInfos[index].MemoryTotal / 4) * 1e9)
		user, err := ParseConfigTemplate("users.xml", configTemplate)
		if err != nil {
			return err
		}
		files[1] = user

		metrika, err := GenerateMetrikaTemplate("metrika.xml", *d.Conf, d.HostInfos, index)
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

func (d *CKDeploy) Check() error {
	time.Sleep(5 * time.Second)

	hosts := make([]string, 0)
	for _, host := range d.Hosts {
		hosts = append(hosts, fmt.Sprintf("%s:%d", host, d.Conf.CkTcpPort))
	}

	conf := config.CKManClickHouseConfig{
		Hosts:    hosts,
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
	localPath := path.Join(common.GetWorkDirectory(), "bin", templateFile)

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

	tmplFile := path.Join(common.GetWorkDirectory(), "package", templateFile)
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

func GenerateMetrikaTemplate(templateFile string, conf model.CkDeployConfig, hostInfos []HostInfo, hostIndex int) (string, error) {
	zkServers := make([]Node, 0)
	ckServers := make([]Cluster, 0)
	shard := 0
	hostName := ""

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
	index := 0
	shardIndex := 0
	for index < len(hostInfos) {
		shardIndex++
		number := 1
		if conf.IsReplica {
			number = 2
		}
		replicas := make([]Repica, number)

		for i := 0; i < number; i++ {
			if hostIndex == index {
				shard = shardIndex
				hostName = hostInfos[index].HostName
			}
			rp := Repica{
				Host: hostInfos[index].HostName,
				Port: conf.CkTcpPort,
			}
			replicas[i] = rp
			index++
		}

		sh := Shard{
			Replication: conf.IsReplica,
			Repicas:     replicas,
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
		Shard:   shard,
		Repica:  hostName,
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

	tmplFile := path.Join(common.GetWorkDirectory(), "package", templateFile)
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
