package clickhouse

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

const NodeOverrideFileName = "node_override.xml"

// RemoteOverridePath returns the absolute path of node_override.xml on a remote host.
func RemoteOverridePath(needSudo bool, cwd string) string {
	if needSudo {
		return path.Join("/etc/clickhouse-server/config.d", NodeOverrideFileName)
	}
	return path.Join(cwd, "etc/clickhouse-server/config.d", NodeOverrideFileName)
}

// ApplyNodeOverride writes the given xml content to <host>:<remote>/node_override.xml
// and triggers SYSTEM RELOAD CONFIG. The xml MUST already be validated/pretty-printed.
// Returns (reloaded, warning, err). On scp failure err is non-nil; on reload failure
// err is nil but warning is set so the caller can persist + warn the user.
func ApplyNodeOverride(conf *model.CKManClickHouseConfig, host, xmlContent string) (bool, string, error) {
	tmp, err := common.NewTempFile(path.Join(config.GetWorkDirectory(), "package"), "node_override")
	if err != nil {
		return false, "", errors.Wrap(err, "create temp file")
	}
	defer os.Remove(tmp.FullName)
	if err := os.WriteFile(tmp.FullName, []byte(xmlContent), 0644); err != nil {
		return false, "", errors.Wrap(err, "write temp file")
	}

	sshOpts := common.SshOptions{
		User:             conf.SshUser,
		Password:         conf.SshPassword,
		Port:             conf.SshPort,
		Host:             host,
		NeedSudo:         conf.NeedSudo,
		AuthenticateType: conf.AuthenticateType,
	}
	remote := RemoteOverridePath(conf.NeedSudo, conf.Cwd)
	if err := common.ScpUploadFile(tmp.FullName, remote, sshOpts); err != nil {
		return false, "", errors.Wrap(err, "scp upload")
	}
	if conf.NeedSudo {
		if _, err := common.RemoteExecute(sshOpts, fmt.Sprintf("chown clickhouse:clickhouse %s", remote)); err != nil {
			log.Logger.Warnf("chown node_override.xml on %s failed: %v", host, err)
		}
	}

	if err := reloadConfigOnHost(conf, host); err != nil {
		return false, fmt.Sprintf("RELOAD CONFIG failed on %s: %v", host, err), nil
	}
	return true, "", nil
}

// RemoveNodeOverride deletes node_override.xml on <host> and triggers RELOAD.
// Returns (reloaded, warning, err). If the file does not exist, ssh rm -f is a no-op.
func RemoveNodeOverride(conf *model.CKManClickHouseConfig, host string) (bool, string, error) {
	sshOpts := common.SshOptions{
		User:             conf.SshUser,
		Password:         conf.SshPassword,
		Port:             conf.SshPort,
		Host:             host,
		NeedSudo:         conf.NeedSudo,
		AuthenticateType: conf.AuthenticateType,
	}
	remote := RemoteOverridePath(conf.NeedSudo, conf.Cwd)
	// When NeedSudo is true, RemoteExecute prepends sudo via genFinalScript, so this rm can target /etc paths.
	if _, err := common.RemoteExecute(sshOpts, fmt.Sprintf("rm -f %s", remote)); err != nil {
		return false, "", errors.Wrap(err, "rm node_override")
	}
	if err := reloadConfigOnHost(conf, host); err != nil {
		return false, fmt.Sprintf("RELOAD CONFIG failed on %s: %v", host, err), nil
	}
	return true, "", nil
}

func reloadConfigOnHost(conf *model.CKManClickHouseConfig, host string) error {
	tmp := &model.CKManClickHouseConfig{
		Hosts:    []string{host},
		Port:     conf.Port,
		HttpPort: conf.HttpPort,
		Protocol: conf.Protocol,
		Secure:   conf.Secure,
		Cluster:  conf.Cluster,
		User:     conf.User,
		Password: conf.Password,
	}
	svc := NewCkService(tmp)
	if err := svc.InitCkService(); err != nil {
		return err
	}
	return svc.Conn.Exec("SYSTEM RELOAD CONFIG")
}

// HasOverride is a small helper used by callers that want to short-circuit work.
func HasOverride(conf *model.CKManClickHouseConfig, host string) bool {
	if conf.NodeOverrides == nil {
		return false
	}
	return strings.TrimSpace(conf.NodeOverrides[host]) != ""
}
