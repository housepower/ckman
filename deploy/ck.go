package deploy

import (
	"encoding/gob"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/housepower/ckman/ckconfig"
	"github.com/housepower/ckman/repository"

	"github.com/housepower/ckman/config"
	"github.com/pkg/errors"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

func init() {
	gob.Register(CKDeploy{})
}

const (
	CkSvrName string = "clickhouse-server"
)

type CKDeploy struct {
	DeployBase
	Conf      *model.CKManClickHouseConfig
	HostInfos []ckconfig.HostInfo
	Ext       model.CkDeployExt
}

func NewCkDeploy(conf model.CKManClickHouseConfig) *CKDeploy {
	return &CKDeploy{
		Conf:       &conf,
		DeployBase: DeployBase{},
	}
}

func (d *CKDeploy) Init() error {
	d.Conf.Normalize()
	d.HostInfos = make([]ckconfig.HostInfo, len(d.Conf.Hosts))
	HostNameMap := make(map[string]bool)
	var lock sync.RWMutex
	var lastError error
	d.Ext.Ipv6Enable = true
	for index, host := range d.Conf.Hosts {
		innerIndex := index
		innerHost := host
		_ = common.Pool.Submit(func() {
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			cmd := "cat /proc/meminfo | grep MemTotal | awk '{print $2}'"
			output, err := common.RemoteExecute(sshOpts, cmd)
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

			info := ckconfig.HostInfo{
				MemoryTotal: total,
			}
			d.HostInfos[innerIndex] = info
			if d.Ext.Ipv6Enable {
				cmd2 := "grep lo /proc/net/if_inet6 >/dev/null 2>&1; echo $?"
				output, err = common.RemoteExecute(sshOpts, cmd2)
				if err != nil {
					lastError = err
					return
				}

				ipv6Enable := strings.Trim(output, "\n")
				if ipv6Enable != "0" {
					//file not exists, return 2, file exists but empty, return 1
					d.Ext.Ipv6Enable = false
				}
			}
		})
	}
	common.Pool.Wait()
	if lastError != nil {
		return lastError
	}

	clusterNodeNum := 0
	lastError = nil
	for shardIndex, shard := range d.Conf.Shards {
		for replicaIndex, replica := range shard.Replicas {
			innerShardIndex := shardIndex
			innerReplicaIndex := replicaIndex
			innerReplica := replica
			_ = common.Pool.Submit(func() {
				sshOpts := common.SshOptions{
					User:             d.Conf.SshUser,
					Password:         d.Conf.SshPassword,
					Port:             d.Conf.SshPort,
					Host:             innerReplica.Ip,
					NeedSudo:         d.Conf.NeedSudo,
					AuthenticateType: d.Conf.AuthenticateType,
				}
				cmd := "hostname"
				output, _ := common.RemoteExecute(sshOpts, cmd)

				hostname := strings.Trim(output, "\n")
				if hostname == "" {
					hostname = innerReplica.Ip
				}
				d.Conf.Shards[innerShardIndex].Replicas[innerReplicaIndex].HostName = hostname
				lock.Lock()
				HostNameMap[hostname] = true
				lock.Unlock()
				clusterNodeNum++
			})
		}
	}
	common.Pool.Wait()
	if lastError != nil {
		return lastError
	}

	if len(HostNameMap) != clusterNodeNum {
		return errors.Errorf("host name are the same")
	}
	log.Logger.Infof("init done")
	return nil
}

func (d *CKDeploy) Prepare() error {
	d.Conf.Normalize()
	files := make([]string, 0)
	for _, file := range d.Packages.PkgLists {
		files = append(files, path.Join(config.GetWorkDirectory(), common.DefaultPackageDirectory, file))
	}

	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = common.Pool.Submit(func() {
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			if err := common.ScpUploadFiles(files, common.TmpWorkDirectory, sshOpts); err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s prepare done", innerHost)
		})
	}
	common.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("prepare done")
	return nil
}

func (d *CKDeploy) Install() error {
	d.Conf.Normalize()
	cmdIns := GetSuitableCmdAdpt(d.Conf.PkgType)
	cmds := make([]string, 0)
	cmds = append(cmds, cmdIns.InstallCmd(d.Packages))
	cmds = append(cmds, fmt.Sprintf("rm -rf %s", path.Join(d.Conf.Path, "clickhouse")))
	cmds = append(cmds, fmt.Sprintf("mkdir -p %s", path.Join(d.Conf.Path, "clickhouse")))
	if d.Conf.NeedSudo {
		cmds = append(cmds, fmt.Sprintf("chown clickhouse.clickhouse %s -R", path.Join(d.Conf.Path, "clickhouse")))
	}
	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = common.Pool.Submit(func() {
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			cmd1 := cmdIns.StopCmd(CkSvrName, d.Conf.Cwd)
			_, _ = common.RemoteExecute(sshOpts, cmd1)

			cmd2 := strings.Join(cmds, ";")
			_, err := common.RemoteExecute(sshOpts, cmd2)
			if err != nil {
				lastError = err
				return
			}
			if !d.Conf.NeedSudo {
				//tgz deployment, try to add auto start
				extractDir := ""
				for _, pkg := range d.Packages.PkgLists {
					if strings.Contains(pkg, common.PkgModuleServer) {
						lastIndex := strings.LastIndex(pkg, "-")
						extractDir = pkg[:lastIndex]
						break
					}
				}

				cmd3 := fmt.Sprintf("cp /tmp/%s/etc/init.d/clickhouse-server /etc/init.d/;", extractDir)
				cmd3 += fmt.Sprintf("cp /tmp/%s/lib/systemd/system/clickhouse-server.service /etc/systemd/system/", extractDir)
				sshOpts.NeedSudo = true
				_, err = common.RemoteExecute(sshOpts, cmd3)
				if err != nil {
					log.Logger.Warnf("try to config autorestart failed:%v", err)
				}
			}

			log.Logger.Debugf("host %s install done", innerHost)
		})
	}
	common.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("install done")
	return nil
}

func (d *CKDeploy) Uninstall() error {
	d.Conf.Normalize()
	cmdIns := GetSuitableCmdAdpt(d.Conf.PkgType)
	cmds := make([]string, 0)
	cmds = append(cmds, cmdIns.Uninstall(d.Packages))
	cmds = append(cmds, fmt.Sprintf("rm -rf %s", path.Join(d.Conf.Path, "clickhouse")))
	if d.Conf.NeedSudo {
		cmds = append(cmds, "rm -rf /etc/clickhouse-server")
		cmds = append(cmds, "rm -rf /etc/clickhouse-client")
	}
	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = common.Pool.Submit(func() {
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			cmd := strings.Join(cmds, ";")
			_, err := common.RemoteExecute(sshOpts, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s uninstall done", innerHost)
		})
	}
	common.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("uninstall done")
	return nil
}

func (d *CKDeploy) Upgrade() error {
	d.Conf.Normalize()
	cmdIns := GetSuitableCmdAdpt(d.Conf.PkgType)
	cmd := cmdIns.UpgradeCmd(d.Packages)
	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = common.Pool.Submit(func() {
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			_, err := common.RemoteExecute(sshOpts, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s upgrade done", innerHost)
		})
	}
	common.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("upgrade done")
	return nil
}

func (d *CKDeploy) Config() error {
	d.Conf.Normalize()
	confFiles := make([]string, 0)

	if d.Conf.LogicCluster == nil {
		metrika, err := ckconfig.GenerateMetrikaXML(path.Join(config.GetWorkDirectory(), "package", "metrika.xml"), d.Conf)
		if err != nil {
			return err
		}
		confFiles = append(confFiles, metrika)
	}

	custom, err := ckconfig.GenerateCustomXML(path.Join(config.GetWorkDirectory(), "package", "custom.xml"), d.Conf, d.Ext.Ipv6Enable)
	if err != nil {
		return err
	}
	confFiles = append(confFiles, custom)

	var remotePath string
	if d.Conf.NeedSudo {
		remotePath = "/etc/clickhouse-server"
	} else {
		remotePath = path.Join(d.Conf.Cwd, "etc", "clickhouse-server")
	}
	var lastError error
	for index, host := range d.Conf.Hosts {
		innerIndex := index
		innerHost := host
		confFiles := confFiles
		_ = common.Pool.Submit(func() {
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			if d.Conf.NeedSudo {
				//clear config first
				cmd := "rm -rf /etc/clickhouse-server/config.d/*.xml /etc/clickhouse-server/users.d/*.xml"
				if _, err = common.RemoteExecute(sshOpts, cmd); err != nil {
					lastError = err
					return
				}
			}

			usersFile, err := common.NewTempFile(path.Join(config.GetWorkDirectory(), "package"), "users")
			if err != nil {
				lastError = err
				return
			}
			defer os.Remove(usersFile.FullName)
			usersXml, err := ckconfig.GenerateUsersXML(usersFile.FullName, d.Conf, d.HostInfos[innerIndex])
			if err != nil {
				lastError = err
				return
			}

			hostFile, err := common.NewTempFile(path.Join(config.GetWorkDirectory(), "package"), "host")
			if err != nil {
				lastError = err
				return
			}
			defer os.Remove(hostFile.FullName)
			hostXml, err := ckconfig.GenerateHostXML(hostFile.FullName, d.Conf, innerHost)
			if err != nil {
				lastError = err
				return
			}
			confFiles = append(confFiles, hostXml)

			if err := common.ScpUploadFiles(confFiles, path.Join(remotePath, "config.d"), sshOpts); err != nil {
				lastError = err
				return
			}
			if err := common.ScpUploadFiles([]string{usersXml}, path.Join(remotePath, "users.d"), sshOpts); err != nil {
				lastError = err
				return
			}

			cmds := make([]string, 0)
			cmds = append(cmds, fmt.Sprintf("mv %s %s", path.Join(remotePath, "config.d", hostFile.BaseName), path.Join(remotePath, "config.d", "host.xml")))
			cmds = append(cmds, fmt.Sprintf("mv %s %s", path.Join(remotePath, "users.d", usersFile.BaseName), path.Join(remotePath, "users.d", "users.xml")))
			if d.Conf.NeedSudo {
				cmds = append(cmds, "chown -R clickhouse:clickhouse /etc/clickhouse-server")
			}
			cmd := strings.Join(cmds, ";")
			if _, err = common.RemoteExecute(sshOpts, cmd); err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s config done", innerHost)
		})
	}
	common.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	if d.Conf.LogicCluster != nil {
		logicMetrika, deploys := GenLogicMetrika(d)
		for _, deploy := range deploys {
			deploy.Conf.Normalize()
			metrikaFile, err := common.NewTempFile(path.Join(config.GetWorkDirectory(), "package"), "metrika")
			if err != nil {
				return err
			}
			defer os.Remove(metrikaFile.FullName)
			m, err := ckconfig.GenerateMetrikaXMLwithLogic(metrikaFile.FullName, deploy.Conf, logicMetrika)
			if err != nil {
				return err
			}
			for _, host := range deploy.Conf.Hosts {
				innerHost := host
				deploy := deploy
				_ = common.Pool.Submit(func() {
					sshOpts := common.SshOptions{
						User:             deploy.Conf.SshUser,
						Password:         deploy.Conf.SshPassword,
						Port:             deploy.Conf.SshPort,
						Host:             innerHost,
						NeedSudo:         deploy.Conf.NeedSudo,
						AuthenticateType: deploy.Conf.AuthenticateType,
					}
					if err := common.ScpUploadFile(m, path.Join(remotePath, "config.d", "metrika.xml"), sshOpts); err != nil {
						lastError = err
						return
					}
					if d.Conf.NeedSudo {
						cmd := "chown -R clickhouse:clickhouse /etc/clickhouse-server"
						if _, err = common.RemoteExecute(sshOpts, cmd); err != nil {
							lastError = err
							return
						}
					}
				})
			}
			common.Pool.Wait()
			if lastError != nil {
				return lastError
			}
		}
	}
	log.Logger.Infof("config done")
	return nil
}

func (d *CKDeploy) Start() error {
	d.Conf.Normalize()
	cmdIns := GetSuitableCmdAdpt(d.Conf.PkgType)
	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = common.Pool.Submit(func() {
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			cmd := cmdIns.StartCmd(CkSvrName, d.Conf.Cwd)
			_, err := common.RemoteExecute(sshOpts, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s start done", innerHost)
		})
	}
	common.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("start done")
	return nil
}

func (d *CKDeploy) Stop() error {
	d.Conf.Normalize()
	cmdIns := GetSuitableCmdAdpt(d.Conf.PkgType)
	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = common.Pool.Submit(func() {
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			cmd := cmdIns.StopCmd(CkSvrName, d.Conf.Cwd)
			_, err := common.RemoteExecute(sshOpts, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s stop done", innerHost)
		})
	}
	common.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("stop done")
	return nil
}

func (d *CKDeploy) Restart() error {
	d.Conf.Normalize()
	cmdIns := GetSuitableCmdAdpt(d.Conf.PkgType)
	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = common.Pool.Submit(func() {
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			cmd := cmdIns.RestartCmd(CkSvrName, d.Conf.Cwd)
			_, err := common.RemoteExecute(sshOpts, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s restart done", innerHost)
		})
	}
	common.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("restart done")
	return nil
}

func (d *CKDeploy) Check(timeout int) error {
	d.Conf.Normalize()
	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = common.Pool.Submit(func() {
			// Golang <-time.After() is not garbage collected before expiry.
			ticker := time.NewTicker(5 * time.Second)
			ticker2 := time.NewTicker(time.Duration(timeout) * time.Second)
			defer ticker.Stop()
			defer ticker2.Stop()
			for {
				select {
				case <-ticker.C:
					db, err := common.ConnectClickHouse(innerHost, d.Conf.Port, model.ClickHouseDefaultDB, d.Conf.User, d.Conf.Password)
					if err != nil {
						log.Logger.Errorf("connect error: %v", err)
						continue
					}
					if err = db.Ping(); err != nil {
						log.Logger.Errorf("ping error: %v", err)
						continue
					}
					if err == nil {
						log.Logger.Debugf("host %s check done", innerHost)
						return
					}
				case <-ticker2.C:
					lastError = errors.Wrapf(model.CheckTimeOutErr, "clickhouse-server may start failed, please check the clickhouse-server log")
					return
				}
			}
		})
	}

	common.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("check done")
	return nil
}

func StartCkCluster(conf *model.CKManClickHouseConfig) error {
	var chHosts []string
	for _, host := range conf.Hosts {
		_, err := common.ConnectClickHouse(host, conf.Port, model.ClickHouseDefaultDB, conf.User, conf.Password)
		if err != nil {
			chHosts = append(chHosts, host)
		}
	}

	if len(chHosts) == 0 {
		return nil
	}

	deploy := NewCkDeploy(*conf)
	deploy.Conf.Hosts = chHosts

	if err := deploy.Start(); err != nil {
		return err
	}
	if err := deploy.Check(model.MaxTimeOut); err != nil {
		return err
	}
	return nil
}

func StopCkCluster(conf *model.CKManClickHouseConfig) error {
	var chHosts []string
	for _, host := range conf.Hosts {
		_, err := common.ConnectClickHouse(host, conf.Port, model.ClickHouseDefaultDB, conf.User, conf.Password)
		if err == nil {
			chHosts = append(chHosts, host)
		}
	}

	if len(chHosts) == 0 {
		return nil
	}

	deploy := NewCkDeploy(*conf)
	deploy.Conf.Hosts = chHosts

	return deploy.Stop()
}

func ConfigLogicOtherCluster(clusterName string) error {
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		return fmt.Errorf("can't find cluster %s", clusterName)
	}
	d := NewCkDeploy(conf)
	d.Conf.Cluster = clusterName
	metrika, deploys := GenLogicMetrika(d)
	for _, deploy := range deploys {
		deploy.Conf.Normalize()
		logicFile, err := common.NewTempFile(path.Join(config.GetWorkDirectory(), "package"), "metrika")
		if err != nil {
			return err
		}
		defer os.Remove(logicFile.FullName)
		m, _ := ckconfig.GenerateMetrikaXMLwithLogic(logicFile.FullName, deploy.Conf, metrika)
		var lastError error
		var remotePath string
		if d.Conf.NeedSudo {
			remotePath = "/etc/clickhouse-server"
		} else {
			remotePath = path.Join(d.Conf.Cwd, "etc", "clickhouse-server")
		}
		for _, host := range d.Conf.Hosts {
			host := host
			deploy := deploy
			_ = common.Pool.Submit(func() {
				sshOpts := common.SshOptions{
					User:             deploy.Conf.SshUser,
					Password:         deploy.Conf.SshPassword,
					Port:             deploy.Conf.SshPort,
					Host:             host,
					NeedSudo:         deploy.Conf.NeedSudo,
					AuthenticateType: deploy.Conf.AuthenticateType,
				}
				if err := common.ScpUploadFile(m, path.Join(remotePath, "metrika.xml"), sshOpts); err != nil {
					lastError = err
					return
				}
				if deploy.Conf.NeedSudo {
					cmd := "chown -R clickhouse:clickhouse /etc/clickhouse-server"
					if _, err := common.RemoteExecute(sshOpts, cmd); err != nil {
						lastError = err
						return
					}
				}
			})
		}
		common.Pool.Wait()
		if lastError != nil {
			return lastError
		}
	}
	return nil
}

func GenLogicMetrika(d *CKDeploy) (string, []*CKDeploy) {
	var deploys []*CKDeploy
	xml := common.NewXmlFile("")
	xml.SetIndent(2)
	xml.Begin(*d.Conf.LogicCluster)
	secret := true
	if common.CompareClickHouseVersion(d.Conf.Version, "20.10.3.30") < 0 {
		secret = false
	}
	logics, err := repository.Ps.GetLogicClusterbyName(*d.Conf.LogicCluster)
	if err == nil {
		for _, logic := range logics {
			if logic == d.Conf.Cluster {
				// if the operation is addNode or deleteNode, we do not use global config
				continue
			}
			c, _ := repository.Ps.GetClusterbyName(logic)
			deploy := NewCkDeploy(c)
			if secret && common.CompareClickHouseVersion(d.Conf.Version, "20.10.3.30") < 0 {
				secret = false
			}
			deploys = append(deploys, deploy)
		}
	}
	deploys = append(deploys, d)
	if secret {
		xml.Write("secret", "foo")
	}
	for _, deploy := range deploys {
		for _, shard := range deploy.Conf.Shards {
			xml.Begin("shard")
			xml.Write("internal_replication", deploy.Conf.IsReplica)
			for _, replica := range shard.Replicas {
				xml.Begin("replica")
				xml.Write("host", replica.Ip)
				xml.Write("port", deploy.Conf.Port)
				if !secret {
					xml.Write("user", deploy.Conf.User)
					xml.Write("password", deploy.Conf.Password)
				}
				xml.End("replica")
			}
			xml.End("shard")
		}
	}
	xml.End(*d.Conf.LogicCluster)
	return xml.GetContext(), deploys
}

func ClearLogicCluster(cluster, logic string, reconf bool) error {
	var newPhysics []string
	physics, err := repository.Ps.GetLogicClusterbyName(logic)
	if err == nil {
		// need delete logic cluster and reconf other cluster
		for _, physic := range physics {
			if physic == cluster {
				continue
			}
			newPhysics = append(newPhysics, physic)
		}
	}
	if len(newPhysics) == 0 {
		if err = repository.Ps.DeleteLogicCluster(logic); err != nil {
			return err
		}
	} else {
		if err = repository.Ps.UpdateLogicCluster(logic, newPhysics); err != nil {
			return err
		}
		if reconf {
			for _, newLogic := range newPhysics {
				if err = ConfigLogicOtherCluster(newLogic); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func BuildPackages(version, pkgType, cwd string) Packages {
	if pkgType == "" {
		pkgType = model.PkgTypeDefault
	}
	pkgLists := make([]string, 3)
	pkgs, ok := common.CkPackages.Load(pkgType)
	if !ok {
		return Packages{}
	}
	for _, pkg := range pkgs.(common.CkPackageFiles) {
		if pkg.Version == version {
			if pkg.Module == common.PkgModuleCommon {
				pkgLists[0] = pkg.PkgName
			} else if pkg.Module == common.PkgModuleServer {
				pkgLists[1] = pkg.PkgName
			} else if pkg.Module == common.PkgModuleClient {
				pkgLists[2] = pkg.PkgName
			}
		}
	}

	return Packages{
		PkgLists: pkgLists,
		Cwd:      cwd,
	}
}
