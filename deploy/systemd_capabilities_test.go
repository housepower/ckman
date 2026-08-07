package deploy

import (
	"os"
	"reflect"
	"testing"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
)

func TestHasCapAmbField(t *testing.T) {
	tests := []struct {
		name       string
		procStatus string
		want       bool
	}{
		{
			name:       "supported with empty ambient set",
			procStatus: "Name:\tcat\nCapInh:\t0000000000000000\nCapAmb:\t0000000000000000\n",
			want:       true,
		},
		{
			name:       "supported with non-empty ambient set",
			procStatus: "CapAmb:\t0000000000000400\n",
			want:       true,
		},
		{
			name:       "unsupported",
			procStatus: "Name:\tcat\nCapInh:\t0000000000000000\nCapBnd:\t0000001fffffffff\n",
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasCapAmbField(tt.procStatus); got != tt.want {
				t.Fatalf("hasCapAmbField() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUsesSystemdPackage(t *testing.T) {
	tests := map[string]bool{
		"x86_64.rpm":  true,
		"aarch64.rpm": true,
		"amd64.deb":   true,
		"x86_64.tgz":  false,
		"":            false,
	}

	for pkgType, want := range tests {
		if got := usesSystemdPackage(pkgType); got != want {
			t.Errorf("usesSystemdPackage(%q) = %v, want %v", pkgType, got, want)
		}
	}
}

func TestReconcileSystemdCapabilitiesUnsupportedKernel(t *testing.T) {
	originalRemoteExecute := remoteExecute
	originalScpUploadFile := scpUploadFile
	t.Cleanup(func() {
		remoteExecute = originalRemoteExecute
		scpUploadFile = originalScpUploadFile
	})

	var commands []string
	remoteExecute = func(opts common.SshOptions, cmd string) (string, error) {
		commands = append(commands, cmd)
		switch cmd {
		case "test -d /run/systemd/system && echo systemd || true":
			return "systemd", nil
		case "cat /proc/self/status":
			return "Name:\tcat\nCapBnd:\t0000001fffffffff\n", nil
		default:
			return "", nil
		}
	}

	var uploadedPath, uploadedContent string
	scpUploadFile = func(localFile, remoteFile string, opts common.SshOptions) error {
		content, err := os.ReadFile(localFile)
		if err != nil {
			return err
		}
		uploadedPath = remoteFile
		uploadedContent = string(content)
		return nil
	}

	d := NewCkDeploy(model.CKManClickHouseConfig{PkgType: "x86_64.rpm"})
	if err := d.reconcileSystemdCapabilities(common.SshOptions{Host: "old-kernel", NeedSudo: true}); err != nil {
		t.Fatalf("reconcileSystemdCapabilities() error = %v", err)
	}

	wantCommands := []string{
		"test -d /run/systemd/system && echo systemd || true",
		"cat /proc/self/status",
		"mkdir -p " + clickHouseSystemdDropInDir,
		"systemctl daemon-reload",
	}
	if !reflect.DeepEqual(commands, wantCommands) {
		t.Fatalf("commands = %#v, want %#v", commands, wantCommands)
	}
	if uploadedPath != clickHouseSystemdDropInPath {
		t.Fatalf("uploaded path = %q, want %q", uploadedPath, clickHouseSystemdDropInPath)
	}
	if uploadedContent != clickHouseSystemdDropIn {
		t.Fatalf("uploaded content = %q, want %q", uploadedContent, clickHouseSystemdDropIn)
	}
}

func TestReconcileSystemdCapabilitiesSupportedKernelRemovesDropIn(t *testing.T) {
	originalRemoteExecute := remoteExecute
	originalScpUploadFile := scpUploadFile
	t.Cleanup(func() {
		remoteExecute = originalRemoteExecute
		scpUploadFile = originalScpUploadFile
	})

	var commands []string
	remoteExecute = func(opts common.SshOptions, cmd string) (string, error) {
		commands = append(commands, cmd)
		switch cmd {
		case "test -d /run/systemd/system && echo systemd || true":
			return "systemd", nil
		case "cat /proc/self/status":
			return "CapAmb:\t0000000000000000\n", nil
		case "test -f " + clickHouseSystemdDropInPath + " && echo present || true":
			return "present", nil
		default:
			return "", nil
		}
	}
	scpUploadFile = func(localFile, remoteFile string, opts common.SshOptions) error {
		t.Fatal("supported kernel must not upload a drop-in")
		return nil
	}

	d := NewCkDeploy(model.CKManClickHouseConfig{PkgType: "amd64.deb"})
	if err := d.reconcileSystemdCapabilities(common.SshOptions{Host: "new-kernel", NeedSudo: true}); err != nil {
		t.Fatalf("reconcileSystemdCapabilities() error = %v", err)
	}

	wantCommands := []string{
		"test -d /run/systemd/system && echo systemd || true",
		"cat /proc/self/status",
		"test -f " + clickHouseSystemdDropInPath + " && echo present || true",
		"rm -f " + clickHouseSystemdDropInPath,
		"systemctl daemon-reload",
	}
	if !reflect.DeepEqual(commands, wantCommands) {
		t.Fatalf("commands = %#v, want %#v", commands, wantCommands)
	}
}

func TestReconcileSystemdCapabilitiesSupportedKernelWithoutDropInIsNoop(t *testing.T) {
	originalRemoteExecute := remoteExecute
	t.Cleanup(func() { remoteExecute = originalRemoteExecute })

	var commands []string
	remoteExecute = func(opts common.SshOptions, cmd string) (string, error) {
		commands = append(commands, cmd)
		switch cmd {
		case "test -d /run/systemd/system && echo systemd || true":
			return "systemd", nil
		case "cat /proc/self/status":
			return "CapAmb:\t0000000000000000\n", nil
		default:
			return "", nil
		}
	}

	d := NewCkDeploy(model.CKManClickHouseConfig{PkgType: "x86_64.rpm"})
	if err := d.reconcileSystemdCapabilities(common.SshOptions{Host: "new-kernel"}); err != nil {
		t.Fatalf("reconcileSystemdCapabilities() error = %v", err)
	}

	wantCommands := []string{
		"test -d /run/systemd/system && echo systemd || true",
		"cat /proc/self/status",
		"test -f " + clickHouseSystemdDropInPath + " && echo present || true",
	}
	if !reflect.DeepEqual(commands, wantCommands) {
		t.Fatalf("commands = %#v, want %#v", commands, wantCommands)
	}
}

func TestReconcileSystemdCapabilitiesSkipsNonSystemdHost(t *testing.T) {
	originalRemoteExecute := remoteExecute
	t.Cleanup(func() { remoteExecute = originalRemoteExecute })

	var commands []string
	remoteExecute = func(opts common.SshOptions, cmd string) (string, error) {
		commands = append(commands, cmd)
		return "", nil
	}

	d := NewCkDeploy(model.CKManClickHouseConfig{PkgType: "x86_64.rpm"})
	if err := d.reconcileSystemdCapabilities(common.SshOptions{Host: "container"}); err != nil {
		t.Fatalf("reconcileSystemdCapabilities() error = %v", err)
	}

	wantCommands := []string{"test -d /run/systemd/system && echo systemd || true"}
	if !reflect.DeepEqual(commands, wantCommands) {
		t.Fatalf("commands = %#v, want %#v", commands, wantCommands)
	}
}
