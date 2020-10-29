package deploy

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"gitlab.eoitek.net/EOI/ckman/common"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/log"
	"gitlab.eoitek.net/EOI/ckman/model"
	"gitlab.eoitek.net/EOI/ckman/service/clickhouse"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"text/template"
	"time"
)

const (
	TmpWorkDirectory      string = "/tmp/"
	DefaultCkUser         string = "clickhouse"
	DefaultCkPassword     string = "Ck123456!"
	CkCommonPackagePrefix string = "clickhouse-common-static"
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
	XMLName xml.Name `xml:"shard"`
	Repicas []Repica
}

type Repica struct {
	XMLName xml.Name `xml:"replica"`
	Host    string   `xml:"host"`
	Port    int      `xml:"port"`
}

type CKDeployFacotry struct{}

func (CKDeployFacotry) Create() Deploy {
	return &CKDeploy{}
}

type CKDeploy struct {
	DeployBase
	Conf *model.CkDeployConfig
}

func (d *CKDeploy) Init(base *DeployBase, conf interface{}) error {
	c, ok := conf.(*model.CkDeployConfig)
	if !ok {
		return fmt.Errorf("value isn't type of CkDeployConfig")
	}

	d.DeployBase = *base
	d.Conf = c
	if d.Conf.User == "" {
		d.Conf.User = DefaultCkUser
	}
	if d.Conf.Password == "" {
		d.Conf.Password = DefaultCkPassword
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
		if strings.HasPrefix(pack, CkCommonPackagePrefix) {
			cmds = append(cmds, fmt.Sprintf("rpm -ivh %s", pack))
			break
		}
	}
	for _, pack := range d.Packages {
		if !strings.HasPrefix(pack, CkCommonPackagePrefix) {
			cmds = append(cmds, fmt.Sprintf("rpm -ivh %s", pack))
		}
	}
	cmds = append(cmds, fmt.Sprintf("mkdir -p %s", path.Join(d.Conf.Path, "clickhouse")))
	cmds = append(cmds, fmt.Sprintf("chown clickhouse.clickhouse %s -R", path.Join(d.Conf.Path, "clickhouse")))

	for _, host := range d.Hosts {
		err := func() error {
			session, err := common.SSHConnect(d.User, d.Password, host, 22)
			if err != nil {
				return err
			}
			defer session.Close()

			cmd := strings.Join(cmds, " && ")
			if output, err := common.SSHRun(session, cmd); err != nil {
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
	files := make([]string, 0)

	conf, err := ParseConfigTemplate("config.xml", *d.Conf)
	if err != nil {
		return err
	}
	files = append(files, conf)

	user, err := ParseConfigTemplate("users.xml", *d.Conf)
	if err != nil {
		return err
	}
	files = append(files, user)

	metrika, err := GenerateMetrikaTemplate("metrika.xml", *d.Conf)
	if err != nil {
		return err
	}
	files = append(files, metrika)

	for _, host := range d.Hosts {
		if err := common.ScpFiles(files, "/etc/clickhouse-server/", d.User, d.Password, host); err != nil {
			return err
		}
	}

	return nil
}

func (d *CKDeploy) Start() error {
	for _, host := range d.Hosts {
		err := func() error {
			session, err := common.SSHConnect(d.User, d.Password, host, 22)
			if err != nil {
				return err
			}
			defer session.Close()

			cmd := "systemctl start clickhouse-server"
			if output, err := common.SSHRun(session, cmd); err != nil {
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
		hosts = append(hosts, fmt.Sprintf("%s:%d", host, 9000))
	}

	conf := &config.CKManClickHouseConfig{
		Hosts:    hosts,
		User:     d.Conf.User,
		Password: d.Conf.Password,
		DB:       "default",
	}

	svr := clickhouse.NewCkService(conf)
	defer svr.Stop()

	if err := svr.InitCkService(); err != nil {
		return err
	}

	return nil
}

func ParseConfigTemplate(templateFile string, conf model.CkDeployConfig) (string, error) {
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

func GenerateMetrikaTemplate(templateFile string, conf model.CkDeployConfig) (string, error) {
	zkServers := make([]Node, 0)
	ckServers := make([]Cluster, 0)

	for index, zk := range conf.ZkServers {
		node := Node{
			Index: index + 1,
			Host:  zk.Host,
			Port:  zk.Port,
		}
		zkServers = append(zkServers, node)
	}

	for _, cluster := range conf.Clusters {
		shards := make([]Shard, 0)
		for _, shard := range cluster.Shards {
			repicas := make([]Repica, 0)
			for _, repica := range shard.Replicas {
				rp := Repica{
					Host: repica.Host,
					Port: repica.Port,
				}
				repicas = append(repicas, rp)
			}
			sh := Shard{
				Repicas: repicas,
			}
			shards = append(shards, sh)
		}
		ck := Cluster{
			XMLName: xml.Name{Local: cluster.Name},
			Shards:  shards,
		}
		ckServers = append(ckServers, ck)
	}

	metrika := Metrika{
		ZkServers: zkServers,
		CkServers: ckServers,
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
