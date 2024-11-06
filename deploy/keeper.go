package deploy

import (
	"encoding/gob"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/housepower/ckman/ckconfig"
	"github.com/housepower/ckman/service/zookeeper"

	"github.com/housepower/ckman/config"
	"github.com/pkg/errors"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

func init() {
	gob.Register(KeeperDeploy{})
}

const (
	KeeperSvrName string = "clickhouse-keeper"
)

type KeeperDeploy struct {
	DeployBase
	Conf      *model.CKManClickHouseConfig
	HostInfos []ckconfig.HostInfo
	Ext       model.CkDeployExt
}

func NewKeeperDeploy(conf model.CKManClickHouseConfig, packages Packages) *KeeperDeploy {
	return &KeeperDeploy{
		Conf: &conf,
		DeployBase: DeployBase{
			Packages: packages,
		},
	}
}

func (d *KeeperDeploy) Init() error {
	d.Conf.Normalize()
	d.HostInfos = make([]ckconfig.HostInfo, len(d.Conf.KeeperConf.KeeperNodes))
	var lastError error
	var wg sync.WaitGroup
	d.Ext.Ipv6Enable = true
	for _, host := range d.Conf.KeeperConf.KeeperNodes {
		innerHost := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			kpath := ""
			changeuser := ""
			if !d.Conf.NeedSudo {
				kpath = path.Join(d.Conf.Cwd, d.Conf.Path, "clickhouse-keeper")
			} else {
				kpath = path.Join(d.Conf.Path, "clickhouse-keeper")
				changeuser = fmt.Sprintf("; chown -R clickhouse:clickhouse %s", kpath)
			}
			cmd1 := fmt.Sprintf("mkdir -p %s %s", kpath, changeuser)
			_, err := common.RemoteExecute(sshOpts, cmd1)
			if err != nil {
				lastError = err
				return
			}
			if d.Ext.Ipv6Enable {
				cmd2 := "grep lo /proc/net/if_inet6 >/dev/null 2>&1; echo $?"
				output, err := common.RemoteExecute(sshOpts, cmd2)
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
	wg.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("init done")
	return nil
}

func (d *KeeperDeploy) Prepare() error {
	d.Conf.Normalize()
	file := path.Join(config.GetWorkDirectory(), common.DefaultPackageDirectory, d.Packages.Keeper)

	var lastError error
	var wg sync.WaitGroup
	for _, host := range d.Conf.KeeperConf.KeeperNodes {
		innerHost := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			if err := common.ScpUploadFiles([]string{file}, common.TmpWorkDirectory, sshOpts); err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s prepare done", innerHost)
		})
	}
	wg.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("prepare done")
	return nil
}

func (d *KeeperDeploy) Install() error {
	d.Conf.Normalize()
	cmdIns := GetSuitableCmdAdpt(d.Conf.PkgType)
	cmds := make([]string, 0)
	cmds = append(cmds, cmdIns.InstallCmd(KeeperSvrName, d.Packages))
	cmds = append(cmds, fmt.Sprintf("rm -rf %s/* %s/*", d.Conf.KeeperConf.LogPath, d.Conf.KeeperConf.SnapshotPath))
	if d.Conf.NeedSudo {
		cmds = append(cmds, fmt.Sprintf("chown clickhouse.clickhouse %s %s -R", d.Conf.KeeperConf.LogPath, d.Conf.KeeperConf.SnapshotPath))
	}
	var lastError error
	var wg sync.WaitGroup
	for _, host := range d.Conf.KeeperConf.KeeperNodes {
		innerHost := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			cmd1 := cmdIns.StopCmd(KeeperSvrName, d.Conf.Cwd)
			_, _ = common.RemoteExecute(sshOpts, cmd1)

			cmd2 := strings.Join(cmds, ";")
			_, err := common.RemoteExecute(sshOpts, cmd2)
			if err != nil {
				lastError = err
				return
			}
			if d.Conf.Cwd != "" {
				//tgz deployment, try to add auto start
				pkg := d.Packages.Keeper
				lastIndex := strings.LastIndex(pkg, "-")
				extractDir := pkg[:lastIndex]

				cmd3 := fmt.Sprintf("cp /tmp/%s/lib/systemd/system/clickhouse-keeper.service /etc/systemd/system/", extractDir)
				sshOpts.NeedSudo = true
				_, err = common.RemoteExecute(sshOpts, cmd3)
				if err != nil {
					log.Logger.Warnf("try to config autorestart failed:%v", err)
				}
			}

			log.Logger.Debugf("host %s install done", innerHost)
		})
	}
	wg.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("install done")
	return nil
}

func (d *KeeperDeploy) Uninstall() error {
	d.Conf.Normalize()
	cmdIns := GetSuitableCmdAdpt(d.Conf.PkgType)
	cmds := make([]string, 0)
	cmds = append(cmds, cmdIns.Uninstall(KeeperSvrName, d.Packages, d.Conf.Version))
	cmds = append(cmds, fmt.Sprintf("rm -rf %s/* %s/*", d.Conf.KeeperConf.LogPath, d.Conf.KeeperConf.SnapshotPath))
	if d.Conf.NeedSudo {
		cmds = append(cmds, "rm -rf /etc/clickhouse-keeper")
	}
	var lastError error
	var wg sync.WaitGroup
	for _, host := range d.Conf.KeeperConf.KeeperNodes {
		innerHost := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
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
	wg.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("uninstall done")
	return nil
}

func (d *KeeperDeploy) Upgrade() error {
	d.Conf.Normalize()
	cmdIns := GetSuitableCmdAdpt(d.Conf.PkgType)
	cmd := cmdIns.UpgradeCmd(KeeperSvrName, d.Packages)
	var lastError error
	var wg sync.WaitGroup
	for _, host := range d.Conf.KeeperConf.KeeperNodes {
		innerHost := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
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
	wg.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("upgrade done")
	return nil
}

func (d *KeeperDeploy) Config() error {
	d.Conf.Normalize()
	confFiles := make([]string, 0)

	var remotePath string
	if d.Conf.NeedSudo {
		remotePath = "/etc/clickhouse-keeper"
	} else {
		remotePath = path.Join(d.Conf.Cwd, "etc", "clickhouse-keeper")
	}
	var lastError error
	var wg sync.WaitGroup
	for index, host := range d.Conf.KeeperConf.KeeperNodes {
		innerIndex := index
		innerHost := host
		confFiles := confFiles
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
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
				cmd := "cp /etc/clickhouse-keeper/keeper_config.xml /etc/clickhouse-keeper/keeper_config.xml.last"
				if _, err := common.RemoteExecute(sshOpts, cmd); err != nil {
					lastError = err
					return
				}
			}

			keeperFile, err := common.NewTempFile(path.Join(config.GetWorkDirectory(), "package"), "keeper_config")
			if err != nil {
				lastError = err
				return
			}
			defer os.Remove(keeperFile.FullName)
			keeperXml, err := ckconfig.GenerateKeeperXML(keeperFile.FullName, d.Conf, d.Ext.Ipv6Enable, innerIndex+1)
			if err != nil {
				lastError = err
				return
			}
			confFiles = append(confFiles, keeperXml)

			if err := common.ScpUploadFiles(confFiles, remotePath, sshOpts); err != nil {
				lastError = err
				return
			}

			cmds := make([]string, 0)
			cmds = append(cmds, fmt.Sprintf("mv %s %s", path.Join(remotePath, keeperFile.BaseName), path.Join(remotePath, "keeper_config.xml")))
			if d.Conf.NeedSudo {
				cmds = append(cmds, "chown -R clickhouse:clickhouse /etc/clickhouse-keeper")
			}
			cmds = append(cmds, "rm -rf /tmp/keeper_config*")
			cmd := strings.Join(cmds, ";")
			if _, err = common.RemoteExecute(sshOpts, cmd); err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s config done", innerHost)
		})
	}
	wg.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("config done")
	return nil
}

func (d *KeeperDeploy) Start() error {
	d.Conf.Normalize()
	cmdIns := GetSuitableCmdAdpt(d.Conf.PkgType)
	var lastError error
	var wg sync.WaitGroup
	for _, host := range d.Conf.KeeperConf.KeeperNodes {
		innerHost := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			// if strings.HasSuffix(d.Conf.PkgType, common.PkgSuffixTgz) {
			// 	// try to modify ulimit nofiles
			// 	sshOpts.NeedSudo = true
			// 	cmds := []string{
			// 		fmt.Sprintf("sed -i '/%s soft nofile/d' /etc/security/limits.conf", d.Conf.SshUser),
			// 		fmt.Sprintf("sed -i '/%s hard nofile/d' /etc/security/limits.conf", d.Conf.SshUser),
			// 		fmt.Sprintf("echo \"%s soft nofile 500000\" >> /etc/security/limits.conf", d.Conf.SshUser),
			// 		fmt.Sprintf("echo \"%s hard nofile 500000\" >> /etc/security/limits.conf", d.Conf.SshUser),
			// 	}
			// 	_, err := common.RemoteExecute(sshOpts, strings.Join(cmds, ";"))
			// 	if err != nil {
			// 		log.Logger.Warnf("[%s] set ulimit -n failed: %v", host, err)
			// 	}
			// 	sshOpts.NeedSudo = d.Conf.NeedSudo
			// }

			cmd := cmdIns.StartCmd(KeeperSvrName, d.Conf.Cwd)
			_, err := common.RemoteExecute(sshOpts, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s start done", innerHost)
		})
	}
	wg.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("start done")
	return nil
}

func (d *KeeperDeploy) Stop() error {
	d.Conf.Normalize()
	cmdIns := GetSuitableCmdAdpt(d.Conf.PkgType)
	var lastError error
	var wg sync.WaitGroup
	for _, host := range d.Conf.KeeperConf.KeeperNodes {
		innerHost := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			cmd := cmdIns.StopCmd(KeeperSvrName, d.Conf.Cwd)
			_, err := common.RemoteExecute(sshOpts, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s stop done", innerHost)
		})
	}
	wg.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("stop done")
	return nil
}

func (d *KeeperDeploy) Restart() error {
	d.Conf.Normalize()
	cmdIns := GetSuitableCmdAdpt(d.Conf.PkgType)
	var lastError error
	var wg sync.WaitGroup
	for _, host := range d.Conf.KeeperConf.KeeperNodes {
		innerHost := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			sshOpts := common.SshOptions{
				User:             d.Conf.SshUser,
				Password:         d.Conf.SshPassword,
				Port:             d.Conf.SshPort,
				Host:             innerHost,
				NeedSudo:         d.Conf.NeedSudo,
				AuthenticateType: d.Conf.AuthenticateType,
			}
			cmd := cmdIns.RestartCmd(KeeperSvrName, d.Conf.Cwd)
			_, err := common.RemoteExecute(sshOpts, cmd)
			if err != nil {
				lastError = err
				return
			}
			log.Logger.Debugf("host %s restart done", innerHost)
		})
	}
	wg.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("restart done")
	return nil
}

func (d *KeeperDeploy) Check(timeout int) error {
	d.Conf.Normalize()
	var lastError error
	var wg sync.WaitGroup
	for _, host := range d.Conf.KeeperConf.KeeperNodes {
		innerHost := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			// Golang <-time.After() is not garbage collected before expiry.
			ticker := time.NewTicker(5 * time.Second)
			ticker2 := time.NewTicker(time.Duration(timeout) * time.Second)
			defer ticker.Stop()
			defer ticker2.Stop()
			for {
				select {
				case <-ticker.C:
					res, err := zookeeper.ZkMetric(innerHost, d.Conf.KeeperConf.TcpPort, "ruok")
					if err == nil && string(res) == "imok" {
						log.Logger.Debugf("host %s check done", innerHost)
						return
					}
				case <-ticker2.C:
					lastError = errors.Wrapf(model.CheckTimeOutErr, "clickhouse-keeper may start failed, please check the clickhouse-keeper log")
					return
				}
			}
		})
	}

	wg.Wait()
	if lastError != nil {
		return lastError
	}
	log.Logger.Infof("check done")
	return nil
}
