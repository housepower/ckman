package deploy

import (
	"fmt"
	"github.com/housepower/ckman/ckconfig"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/housepower/ckman/service/zookeeper"

	"github.com/housepower/ckman/config"
	"github.com/pkg/errors"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
)

type CKDeploy struct {
	DeployBase
	Conf       *model.CkDeployConfig
	HostInfos  []ckconfig.HostInfo
}

type CkUpdateNodeParam struct {
	Ip       string
	Hostname string
	Shard    int
	Op       int
}

var (
	CheckTimeOutErr = errors.New("check clickhouse timeout error")
)

func (d *CKDeploy) Init() error {
	d.Conf.Normalize()
	d.HostInfos = make([]ckconfig.HostInfo, len(d.Hosts))
	HostNameMap := make(map[string]bool)
	var lock sync.RWMutex
	var lastError error
	d.Conf.Ipv6Enable = true
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

			info := ckconfig.HostInfo{
				MemoryTotal: total,
			}
			d.HostInfos[innerIndex] = info
			if d.Conf.Ipv6Enable {
				cmd2 := "test -f /proc/net/if_inet6; echo $?"
				output, err = common.RemoteExecute(d.User, d.Password, innerHost, d.Port, cmd2)
				if err != nil {
					lastError = err
					return
				}

				ipv6Enable := strings.Trim(output, "\n")
				if ipv6Enable == "1" {
					d.Conf.Ipv6Enable = false
				}
			}
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

	d.Pool.Wait()
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
	for _, host := range d.Hosts {
		innerHost := host
		_ = d.Pool.Submit(func() {
			if err := common.ScpUploadFiles(files, common.TmpWorkDirectory, d.User, d.Password, innerHost, d.Port); err != nil {
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
	cmds = append(cmds, fmt.Sprintf("DEBIAN_FRONTEND=noninteractive rpm --force --nosignature -ivh %s %s %s", path.Join(common.TmpWorkDirectory, d.Packages[0]), path.Join(common.TmpWorkDirectory, d.Packages[1]), path.Join(common.TmpWorkDirectory, d.Packages[2])))
	cmds = append(cmds, fmt.Sprintf("rm -rf %s", path.Join(d.Conf.Path, "clickhouse")))
	cmds = append(cmds, fmt.Sprintf("mkdir -p %s", path.Join(d.Conf.Path, "clickhouse")))
	cmds = append(cmds, fmt.Sprintf("chown clickhouse.clickhouse %s -R", path.Join(d.Conf.Path, "clickhouse")))

	var lastError error
	for _, host := range d.Hosts {
		innerHost := host
		_ = d.Pool.Submit(func() {
			cmd1 := "systemctl stop clickhouse-server"
			_, _ = common.RemoteExecute(d.User, d.Password, innerHost, d.Port, cmd1)

			cmd2 := strings.Join(cmds, ";")
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
			cmd := strings.Join(cmds, ";")
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
	cmd := fmt.Sprintf("DEBIAN_FRONTEND=noninteractive rpm --force --nosignature -Uvh %s %s %s", path.Join(common.TmpWorkDirectory, d.Packages[0]), path.Join(common.TmpWorkDirectory, d.Packages[1]), path.Join(common.TmpWorkDirectory, d.Packages[2]))
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
	confFiles := make([]string, 0)
	userFiles := make([]string, 0)

	if d.Conf.LogicCluster == nil {
		metrika, err := ckconfig.GenerateMetrikaXML(path.Join(config.GetWorkDirectory(), "package", "metrika.xml"), d.Conf)
		if err != nil {
			return err
		}
		confFiles = append(confFiles, metrika)
	}

	custom, err := ckconfig.GenerateCustomXML(path.Join(config.GetWorkDirectory(), "package", "custom.xml"), d.Conf)
	if err != nil {
		return err
	}
	confFiles = append(confFiles, custom)

	users,err := ckconfig.GenerateUsersXML(path.Join(config.GetWorkDirectory(), "package", fmt.Sprintf("users_%s.xml", d.Conf.ClusterName)), d.Conf)
	if err != nil {
		return err
	}
	userFiles = append(userFiles, users)

	var lastError error
	for index, host := range d.Hosts {
		innerIndex := index
		innerHost := host
		_ = d.Pool.Submit(func() {
			confFiles := confFiles
			userFiles := userFiles
			profilesFile, err := common.NewTempFile(path.Join(config.GetWorkDirectory(), "package"), "profiles")
			if err != nil {
				lastError = err
				return
			}
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

			_, _ = common.RemoteExecute(d.User, d.Password, host, d.Port, "rm -rf /etc/clickhouse-server/config.d/* /etc/clickhouse-server/users.d/*")

			if err := common.ScpUploadFiles(confFiles, "/etc/clickhouse-server/config.d/", d.User, d.Password, innerHost, d.Port); err != nil {
				lastError = err
				return
			}
			if err := common.ScpUploadFiles(userFiles, "/etc/clickhouse-server/users.d/", d.User, d.Password, innerHost, d.Port); err != nil {
				lastError = err
				return
			}

			cmds := make([]string, 0)
			cmds = append(cmds, fmt.Sprintf("mv /etc/clickhouse-server/config.d/%s /etc/clickhouse-server/config.d/host.xml", hostFile.BaseName))
			cmds = append(cmds, fmt.Sprintf("mv /etc/clickhouse-server/users.d/%s /etc/clickhouse-server/users.d/profiles.xml", profilesFile.BaseName))
			cmds = append(cmds, "chown -R clickhouse:clickhouse /etc/clickhouse-server")

			cmd := strings.Join(cmds, ";")
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
			for _, host := range deploy.Hosts {
				innerHost := host
				deploy := deploy
				_ = d.Pool.Submit(func() {
					if err := common.ScpUploadFile(m, "/etc/clickhouse-server/config.d/metrika.xml", deploy.User, deploy.Password, innerHost, deploy.Port); err != nil {
						lastError = err
						return
					}
				})
			}
			d.Pool.Wait()
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

func (d *CKDeploy) Restart() error {
	var lastError error
	for _, host := range d.Hosts {
		innerHost := host
		_ = d.Pool.Submit(func() {
			cmd := "systemctl restart clickhouse-server"
			_, err := common.RemoteExecute(d.User, d.Password, innerHost, d.Port, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s restart done", innerHost)
		})
	}
	d.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("restart done")
	return nil
}

func (d *CKDeploy) Check(timeout int) error {
	var lastError error
	for _, host := range d.Hosts {
		innerHost := host
		_ = d.Pool.Submit(func() {
			ticker := time.NewTicker(5*time.Second)
			for {
				select {
				case <-ticker.C:
					db, err := common.ConnectClickHouse(innerHost, d.Conf.CkTcpPort, model.ClickHouseDefaultDB, d.Conf.User, d.Conf.Password)
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
				case <-time.After(time.Duration(timeout)*time.Second):
					lastError = CheckTimeOutErr
					return
				}
			}
		})
	}

	d.Pool.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("check done")
	return nil
}

func UpgradeCkCluster(conf *model.CKManClickHouseConfig, req model.CkUpgradeCkReq) error {
	packages := make([]string, 3)
	packages[0] = fmt.Sprintf("%s-%s-%s", model.CkCommonPackagePrefix, req.PackageVersion, model.CkCommonPackageSuffix)
	packages[1] = fmt.Sprintf("%s-%s-%s", model.CkServerPackagePrefix, req.PackageVersion, model.CkServerPackageSuffix)
	packages[2] = fmt.Sprintf("%s-%s-%s", model.CkClientPackagePrefix, req.PackageVersion, model.CkClientPackageSuffix)

	var chHosts []string
	if req.SkipSameVersion {
		for _, host := range conf.Hosts {
			version, err := clickhouse.GetCKVersion(conf, host)
			if err == nil && version == req.PackageVersion {
				continue
			}
			chHosts = append(chHosts, host)
		}
	} else {
		chHosts = conf.Hosts
	}

	if len(chHosts) == 0 {
		log.Logger.Infof("there is nothing to be upgrade")
		return nil
	}

	switch req.Policy {
	case model.UpgradePolicyRolling:
		for _, host := range chHosts {
			if err := upgradePackage(conf, []string{host}, packages, model.MaxTimeOut); err != nil {
				return err
			}
		}
	case model.UpgradePolicyFull:
		err := upgradePackage(conf, chHosts, packages, 5)
		if err != CheckTimeOutErr {
			return err
		}
	default:
		return fmt.Errorf("not support policy %s yet", req.Policy)
	}

	return nil
}

func upgradePackage(conf *model.CKManClickHouseConfig, hosts []string, packages []string, timeout int) error {
	deploy := ConvertCKDeploy(conf)
	deploy.Hosts = hosts
	deploy.Packages = packages
	if err := deploy.Init(); err != nil {
		return err
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
	if err := deploy.Check(timeout); err != nil {
		return err
	}
	log.Logger.Infof("cluster checked succeed ")

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

	deploy := ConvertCKDeploy(conf)
	deploy.Hosts = chHosts

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

	deploy := ConvertCKDeploy(conf)
	deploy.Hosts = chHosts

	return deploy.Stop()
}

func DestroyCkCluster(conf *model.CKManClickHouseConfig) error {
	packages := make([]string, 3)
	packages[0] = fmt.Sprintf("%s-%s", model.CkClientPackagePrefix, conf.Version)
	packages[1] = fmt.Sprintf("%s-%s", model.CkServerPackagePrefix, conf.Version)
	packages[2] = fmt.Sprintf("%s-%s", model.CkCommonPackagePrefix, conf.Version)

	deploy := ConvertCKDeploy(conf)
	deploy.Packages = packages
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

	if !conf.IsReplica && len(req.Ips) > 1 {
		return errors.Errorf("import mode can only add 1 node once")
	}

	// add the node to conf struct
	for _, ip := range req.Ips {
		for _, host := range conf.Hosts {
			if host == ip {
				return errors.Errorf("node ip %s is duplicate", ip)
			}
		}
	}

	shards := make([]model.CkShard, len(conf.Shards))
	copy(shards, conf.Shards)
	if len(shards) >= req.Shard {
		for _, ip := range req.Ips {
			replica := model.CkReplica{
				Ip: ip,
			}
			shards[req.Shard-1].Replicas = append(shards[req.Shard-1].Replicas, replica)
		}
	} else if len(shards)+1 == req.Shard {
		var replicas []model.CkReplica
		for _, ip := range req.Ips {
			replica := model.CkReplica{
				Ip: ip,
			}
			replicas = append(replicas, replica)
		}
		shard := model.CkShard{
			Replicas: replicas,
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

	deploy := ConvertCKDeploy(conf)
	deploy.Hosts = req.Ips
	deploy.Packages = packages
	deploy.Conf.Shards = shards
	if err := deploy.Init(); err != nil {
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
	if err := deploy.Check(5); err != nil {
		return err
	}

	// update other nodes config
	deploy = ConvertCKDeploy(conf)
	deploy.Conf.Shards =shards
	if err := deploy.Init(); err != nil {
		return err
	}
	if err := deploy.Config(); err != nil {
		return err
	}

	conf.Shards = shards
	conf.Hosts = append(conf.Hosts, req.Ips...)
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
			replicaName := conf.Shards[shardNum].Replicas[replicaNum].Ip
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
	deploy := ConvertCKDeploy(conf)
	packages := make([]string, 3)
	packages[0] = fmt.Sprintf("%s-%s", model.CkClientPackagePrefix, conf.Version)
	packages[1] = fmt.Sprintf("%s-%s", model.CkServerPackagePrefix, conf.Version)
	packages[2] = fmt.Sprintf("%s-%s", model.CkCommonPackagePrefix, conf.Version)
	deploy.Packages = packages
	deploy.Hosts = []string{ip}
	if err := deploy.Stop(); err != nil {
		log.Logger.Warnf("can't stop node %s, ignore it", ip)
	}

	if err := deploy.Uninstall(); err != nil {
		return err
	}

	// remove the node from conf struct
	hosts := append(conf.Hosts[:index], conf.Hosts[index+1:]...)
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
	deploy = ConvertCKDeploy(conf)
	deploy.Hosts = hosts
	deploy.Conf.Shards = shards
	if err := deploy.Init(); err != nil {
		return err
	}
	if err := deploy.Config(); err != nil {
		return err
	}

	conf.Hosts = hosts
	conf.Shards = shards
	return nil
}

func ConfigCkCluster(conf *model.CKManClickHouseConfig, restart bool)error {
	d := ConvertCKDeploy(conf)
	if err := d.Init(); err != nil {
		return err
	}
	if err := d.Config(); err != nil {
		return err
	}

	if restart {
		if err := d.Restart(); err != nil {
			return err
		}
		_ = d.Check(5)
	}
	return nil
}

func ConfigLogicOtherCluster(clusterName string) error {
	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		return fmt.Errorf("can't find cluster %s", clusterName)
	}
	d := ConvertCKDeploy(&conf)
	d.Conf.ClusterName = clusterName
	metrika, deploys := GenLogicMetrika(d)
	for _, deploy := range deploys {
		logicFile, err := common.NewTempFile(path.Join(config.GetWorkDirectory(), "package"), "metrika")
		if err != nil {
			return err
		}
		defer os.Remove(logicFile.FullName)
		m, _ := ckconfig.GenerateMetrikaXMLwithLogic(logicFile.FullName, deploy.Conf, metrika)
		var lastError error
		for _, host := range d.Hosts {
			host := host
			deploy := deploy
			_ = d.Pool.Submit(func() {
				if err := common.ScpUploadFile(m, "/etc/clickhouse-server/metrika.xml", deploy.User, deploy.Password, host, deploy.Port); err != nil {
					lastError = err
					return
				}
			})
		}
		d.Pool.Wait()
		if lastError != nil {
			return nil
		}
	}
	return nil
}

func ConvertCKDeploy(conf *model.CKManClickHouseConfig) *CKDeploy {
	deploy := &CKDeploy{
		DeployBase: DeployBase{
			Hosts:            conf.Hosts,
			User:             conf.SshUser,
			Password:         conf.SshPassword,
			AuthenticateType: conf.AuthenticateType,
			Port:             conf.SshPort,
			Pool:             common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault),
		},
		Conf: &model.CkDeployConfig{
			Path:           conf.Path,
			User:           conf.User,
			Password:       conf.Password,
			ZkNodes:        conf.ZkNodes,
			ZkPort:         conf.ZkPort,
			ZkStatusPort:   conf.ZkStatusPort,
			ClusterName:    conf.Cluster,
			Shards:         conf.Shards,
			PackageVersion: conf.Version,
			CkTcpPort:      conf.Port,
			CkHttpPort:     conf.HttpPort,
			IsReplica:      conf.IsReplica,
			LogicCluster:   conf.LogicCluster,
			Storage:        conf.Storage,
			MergeTreeConf:  conf.MergeTreeConf,
			UserConf:       conf.UsersConf,
		},
	}

	return deploy
}

func GenLogicMetrika(d *CKDeploy)(string, []*CKDeploy) {
	var deploys []*CKDeploy
	xml := common.NewXmlFile("")
	xml.SetIndent(2)
	xml.Begin(*d.Conf.LogicCluster)
	logics, ok := clickhouse.CkClusters.GetLogicClusterByName(*d.Conf.LogicCluster)
	if ok {
		for _, logic := range logics {
			if logic == d.Conf.ClusterName {
				// if the operation is addNode or deleteNode, we do not use global config
				continue
			}
			c, _ := clickhouse.CkClusters.GetClusterByName(logic)
			deploy := ConvertCKDeploy(&c)
			deploys = append(deploys, deploy)
		}
	}
	deploys = append(deploys, d)
	for _, deploy := range deploys {
		for _, shard := range deploy.Conf.Shards {
			xml.Begin("shard")
			xml.Write("internal_replication", d.Conf.IsReplica)
			for _, replica := range shard.Replicas {
				xml.Begin("replica")
				xml.Write("host", replica.Ip)
				xml.Write("port", d.Conf.CkTcpPort)
				xml.End("replica")
			}
			xml.End("shard")
		}
	}
	xml.End(*d.Conf.LogicCluster)
	return xml.GetContext(), deploys
}