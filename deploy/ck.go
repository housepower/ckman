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

func init(){
	gob.Register(CKDeploy{})
}

type CKDeploy struct {
	DeployBase
	Conf      *model.CKManClickHouseConfig
	HostInfos []ckconfig.HostInfo
	Ext       model.CkDeployExt
}



func NewCkDeploy(conf model.CKManClickHouseConfig)*CKDeploy{
	return &CKDeploy{
		Conf: &conf,
		DeployBase:DeployBase{
			pool: common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault),
		},
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
		_ = d.pool.Submit(func() {
			cmd := "cat /proc/meminfo | grep MemTotal | awk '{print $2}'"
			output, err := common.RemoteExecute(d.Conf.SshUser, d.Conf.SshPassword, innerHost, d.Conf.SshPort, cmd)
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
				output, err = common.RemoteExecute(d.Conf.SshUser, d.Conf.SshPassword, innerHost, d.Conf.SshPort, cmd2)
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
	d.pool.StopWait()
	if lastError != nil {
		return lastError
	}

	clusterNodeNum := 0
	lastError = nil
	d.pool.Restart()
	for shardIndex, shard := range d.Conf.Shards {
		for replicaIndex, replica := range shard.Replicas {
			innerShardIndex := shardIndex
			innerReplicaIndex := replicaIndex
			innerReplica := replica
			_ = d.pool.Submit(func() {
				cmd := "hostname -f"
				output, err := common.RemoteExecute(d.Conf.SshUser, d.Conf.SshPassword, innerReplica.Ip, d.Conf.SshPort, cmd)
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

	d.pool.Wait()
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
	files := make([]string, 0)
	for _, file := range d.Packages {
		files = append(files, path.Join(config.GetWorkDirectory(), "package", file))
	}

	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = d.pool.Submit(func() {
			if err := common.ScpUploadFiles(files, common.TmpWorkDirectory, d.Conf.SshUser, d.Conf.SshPassword, innerHost, d.Conf.SshPort); err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s prepare done", innerHost)
		})
	}
	d.pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("prepare done")
	return nil
}

func (d *CKDeploy) Install() error {
	cmds := make([]string, 0)
	cmds = append(cmds, fmt.Sprintf("DEBIAN_FRONTEND=noninteractive rpm --force --nosignature -ivh %s %s %s", path.Join(common.TmpWorkDirectory, d.Packages[0]), path.Join(common.TmpWorkDirectory, d.Packages[1]), path.Join(common.TmpWorkDirectory, d.Packages[2])))
	cmds = append(cmds, fmt.Sprintf("rm -rf %s", path.Join(d.Conf.Path, "clickhouse")))
	cmds = append(cmds, fmt.Sprintf("mkdir -p %s", path.Join(d.Conf.Path, "clickhouse")))
	cmds = append(cmds, fmt.Sprintf("chown clickhouse.clickhouse %s -R", path.Join(d.Conf.Path, "clickhouse")))

	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = d.pool.Submit(func() {
			cmd1 := "systemctl stop clickhouse-server"
			_, _ = common.RemoteExecute(d.Conf.SshUser, d.Conf.SshPassword, innerHost, d.Conf.SshPort, cmd1)

			cmd2 := strings.Join(cmds, ";")
			_, err := common.RemoteExecute(d.Conf.SshUser, d.Conf.SshPassword, innerHost, d.Conf.SshPort, cmd2)
			if err != nil {
				lastError = err
				return
			}

			log.Logger.Debugf("host %s install done", innerHost)
		})
	}
	d.pool.Wait()
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
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = d.pool.Submit(func() {
			cmd := strings.Join(cmds, ";")
			_, err := common.RemoteExecute(d.Conf.SshUser, d.Conf.SshPassword, innerHost, d.Conf.SshPort, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s uninstall done", innerHost)
		})
	}
	d.pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("uninstall done")
	return nil
}

func (d *CKDeploy) Upgrade() error {
	cmd := fmt.Sprintf("DEBIAN_FRONTEND=noninteractive rpm --force --nosignature -Uvh %s %s %s", path.Join(common.TmpWorkDirectory, d.Packages[0]), path.Join(common.TmpWorkDirectory, d.Packages[1]), path.Join(common.TmpWorkDirectory, d.Packages[2]))
	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = d.pool.Submit(func() {
			_, err := common.RemoteExecute(d.Conf.SshUser, d.Conf.SshPassword, innerHost, d.Conf.SshPort, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s upgrade done", innerHost)
		})
	}
	d.pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("upgrade done")
	return nil
}

func (d *CKDeploy) Config() error {
	confFiles := make([]string, 0)
	userFiles := make([]string, 0)

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

	users, err := ckconfig.GenerateUsersXML(path.Join(config.GetWorkDirectory(), "package", fmt.Sprintf("users_%s.xml", d.Conf.Cluster)), d.Conf)
	if err != nil {
		return err
	}
	userFiles = append(userFiles, users)

	var lastError error
	for index, host := range d.Conf.Hosts {
		innerIndex := index
		innerHost := host
		_ = d.pool.Submit(func() {
			confFiles := confFiles
			userFiles := userFiles
			profilesFile, err := common.NewTempFile(path.Join(config.GetWorkDirectory(), "package"), "profiles")
			if err != nil {
				lastError = err
				return
			}
			defer os.Remove(profilesFile.FullName)
			profiles, err := ckconfig.GenerateProfilesXML(profilesFile.FullName, d.HostInfos[innerIndex])
			if err != nil {
				lastError = err
				return
			}
			userFiles = append(userFiles, profiles)

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

			_, _ = common.RemoteExecute(d.Conf.SshUser, d.Conf.SshPassword, host, d.Conf.SshPort, "rm -rf /etc/clickhouse-server/config.d/* /etc/clickhouse-server/users.d/*")

			if err := common.ScpUploadFiles(confFiles, "/etc/clickhouse-server/config.d/", d.Conf.SshUser, d.Conf.SshPassword, innerHost, d.Conf.SshPort); err != nil {
				lastError = err
				return
			}
			if err := common.ScpUploadFiles(userFiles, "/etc/clickhouse-server/users.d/", d.Conf.SshUser, d.Conf.SshPassword, innerHost, d.Conf.SshPort); err != nil {
				lastError = err
				return
			}

			cmds := make([]string, 0)
			cmds = append(cmds, fmt.Sprintf("mv /etc/clickhouse-server/config.d/%s /etc/clickhouse-server/config.d/host.xml", hostFile.BaseName))
			cmds = append(cmds, fmt.Sprintf("mv /etc/clickhouse-server/users.d/%s /etc/clickhouse-server/users.d/profiles.xml", profilesFile.BaseName))
			cmds = append(cmds, "chown -R clickhouse:clickhouse /etc/clickhouse-server")

			cmd := strings.Join(cmds, ";")
			if _, err = common.RemoteExecute(d.Conf.SshUser, d.Conf.SshPassword, innerHost, d.Conf.SshPort, cmd); err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s config done", innerHost)
		})
	}
	d.pool.Wait()
	if lastError != nil {
		return lastError
	}
	if d.Conf.LogicCluster != nil {
		logicMetrika, deploys := GenLogicMetrika(d)
		for _, deploy := range deploys {
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
				_ = d.pool.Submit(func() {
					if err := common.ScpUploadFile(m, "/etc/clickhouse-server/config.d/metrika.xml", deploy.Conf.SshUser, deploy.Conf.SshPassword, innerHost, deploy.Conf.SshPort); err != nil {
						lastError = err
						return
					}
				})
			}
			d.pool.Wait()
			if lastError != nil {
				return lastError
			}
		}
	}
	log.Logger.Infof("config done")
	return nil
}

func (d *CKDeploy) Start() error {
	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = d.pool.Submit(func() {
			cmd := "systemctl start clickhouse-server"
			_, err := common.RemoteExecute(d.Conf.SshUser, d.Conf.SshPassword, innerHost, d.Conf.SshPort, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s start done", innerHost)
		})
	}
	d.pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("start done")
	return nil
}

func (d *CKDeploy) Stop() error {
	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = d.pool.Submit(func() {
			cmd := "systemctl stop clickhouse-server"
			_, err := common.RemoteExecute(d.Conf.SshUser, d.Conf.SshPassword, innerHost, d.Conf.SshPort, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s stop done", innerHost)
		})
	}
	d.pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("stop done")
	return nil
}

func (d *CKDeploy) Restart() error {
	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = d.pool.Submit(func() {
			cmd := "systemctl restart clickhouse-server"
			_, err := common.RemoteExecute(d.Conf.SshUser, d.Conf.SshPassword, innerHost, d.Conf.SshPort, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s restart done", innerHost)
		})
	}
	d.pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("restart done")
	return nil
}

func (d *CKDeploy) Check(timeout int) error {
	var lastError error
	for _, host := range d.Conf.Hosts {
		innerHost := host
		_ = d.pool.Submit(func() {
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
						continue
					}
					if err = db.Ping(); err != nil {
						continue
					}
					if err == nil {
						log.Logger.Debugf("host %s check done", innerHost)
						return
					}
				case <-ticker2.C:
					lastError = model.CheckTimeOutErr
					return
				}
			}
		})
	}

	d.pool.Wait()
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
		logicFile, err := common.NewTempFile(path.Join(config.GetWorkDirectory(), "package"), "metrika")
		if err != nil {
			return err
		}
		defer os.Remove(logicFile.FullName)
		m, _ := ckconfig.GenerateMetrikaXMLwithLogic(logicFile.FullName, deploy.Conf, metrika)
		var lastError error
		for _, host := range d.Conf.Hosts {
			host := host
			deploy := deploy
			_ = d.pool.Submit(func() {
				if err := common.ScpUploadFile(m, "/etc/clickhouse-server/config.d/metrika.xml", deploy.Conf.SshUser, deploy.Conf.SshPassword, host, deploy.Conf.SshPort); err != nil {
					lastError = err
					return
				}
				cmd := "chown -R clickhouse:clickhouse /etc/clickhouse-server"
				if _, err := common.RemoteExecute(deploy.Conf.SshUser, deploy.Conf.SshPassword, host, deploy.Conf.SshPort, cmd); err != nil {
					lastError = err
					return
				}
			})
		}
		d.pool.Wait()
		if lastError != nil {
			return nil
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

func BuildPackages(version string)[]string {
	packages := make([]string, 3)
	packages[0] = fmt.Sprintf("%s-%s-%s", model.CkCommonPackagePrefix, version, model.CkCommonPackageSuffix)
	packages[1] = fmt.Sprintf("%s-%s-%s", model.CkServerPackagePrefix, version, model.CkServerPackageSuffix)
	packages[2] = fmt.Sprintf("%s-%s-%s", model.CkClientPackagePrefix, version, model.CkClientPackageSuffix)
	return packages
}
