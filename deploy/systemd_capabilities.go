package deploy

import (
	"fmt"
	"os"
	"strings"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
)

const (
	clickHouseSystemdDropInDir  = "/etc/systemd/system/clickhouse-server.service.d"
	clickHouseSystemdDropInPath = clickHouseSystemdDropInDir + "/ckman-no-ambient-capabilities.conf"
	clickHouseSystemdDropIn     = `[Service]
# Managed by ckman: this kernel does not expose ambient capabilities.
AmbientCapabilities=
`
)

var (
	remoteExecute = common.RemoteExecute
	scpUploadFile = common.ScpUploadFile
)

func usesSystemdPackage(pkgType string) bool {
	return strings.HasSuffix(pkgType, "."+common.PkgSuffixRpm) ||
		strings.HasSuffix(pkgType, "."+common.PkgSuffixDeb)
}

func hasCapAmbField(procStatus string) bool {
	for _, line := range strings.Split(procStatus, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "CapAmb:") {
			return true
		}
	}
	return false
}

// reconcileSystemdCapabilities keeps a ckman-owned systemd drop-in in sync
// with the target kernel. The value of CapAmb is irrelevant: the field's
// presence means that the kernel implements ambient capabilities.
func (d *CKDeploy) reconcileSystemdCapabilities(sshOpts common.SshOptions) error {
	if !usesSystemdPackage(d.Conf.PkgType) {
		return nil
	}

	probeOpts := sshOpts
	probeOpts.NeedSudo = false
	systemdState, err := remoteExecute(probeOpts, "test -d /run/systemd/system && echo systemd || true")
	if err != nil {
		return fmt.Errorf("detect systemd on host %s: %w", sshOpts.Host, err)
	}
	if strings.TrimSpace(systemdState) != "systemd" {
		return nil
	}

	procStatus, err := remoteExecute(probeOpts, "cat /proc/self/status")
	if err != nil {
		return fmt.Errorf("detect ambient capabilities on host %s: %w", sshOpts.Host, err)
	}

	if hasCapAmbField(procStatus) {
		dropInState, probeErr := remoteExecute(probeOpts, "test -f "+clickHouseSystemdDropInPath+" && echo present || true")
		if probeErr != nil {
			return fmt.Errorf("detect managed systemd capability drop-in on host %s: %w", sshOpts.Host, probeErr)
		}
		if strings.TrimSpace(dropInState) != "present" {
			return nil
		}
		if _, err = remoteExecute(sshOpts, "rm -f "+clickHouseSystemdDropInPath); err != nil {
			return fmt.Errorf("remove obsolete systemd capability drop-in on host %s: %w", sshOpts.Host, err)
		}
	} else {
		if _, err = remoteExecute(sshOpts, "mkdir -p "+clickHouseSystemdDropInDir); err != nil {
			return fmt.Errorf("create systemd drop-in directory on host %s: %w", sshOpts.Host, err)
		}

		tmp, err := os.CreateTemp("", "ckman-no-ambient-capabilities-*.conf")
		if err != nil {
			return fmt.Errorf("create temporary systemd drop-in: %w", err)
		}
		tmpName := tmp.Name()
		defer os.Remove(tmpName)

		if err = tmp.Chmod(0644); err != nil {
			tmp.Close()
			return fmt.Errorf("set temporary systemd drop-in permissions: %w", err)
		}
		if _, err = tmp.WriteString(clickHouseSystemdDropIn); err != nil {
			tmp.Close()
			return fmt.Errorf("write temporary systemd drop-in: %w", err)
		}
		if err = tmp.Close(); err != nil {
			return fmt.Errorf("close temporary systemd drop-in: %w", err)
		}
		if err = scpUploadFile(tmpName, clickHouseSystemdDropInPath, sshOpts); err != nil {
			return fmt.Errorf("install systemd capability drop-in on host %s: %w", sshOpts.Host, err)
		}
		log.Logger.Infof("[%s] kernel has no CapAmb field; disabled AmbientCapabilities for clickhouse-server", sshOpts.Host)
	}

	if _, err = remoteExecute(sshOpts, "systemctl daemon-reload"); err != nil {
		return fmt.Errorf("reload systemd configuration on host %s: %w", sshOpts.Host, err)
	}
	return nil
}
