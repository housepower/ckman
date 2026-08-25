package controller

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/housepower/ckman/deploy"
)

// mkPkgDir 在测试临时目录里造一个包目录，并写入给定的包文件。
// 目标路径一律来自 t.TempDir()，测试绝不触碰真实的 /etc/ckman/package。
func mkPkgDir(t *testing.T, names ...string) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("rpm"), 0644); err != nil {
			t.Fatalf("prepare %s: %v", name, err)
		}
	}
	return dir
}

func assertGone(t *testing.T, dir, name string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
		t.Errorf("%s should have been deleted, stat err = %v", name, err)
	}
}

func assertExists(t *testing.T, dir, name string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
		t.Errorf("%s should still exist, stat err = %v", name, err)
	}
}

// keeper 包过去被 BuildPackages 分流到 Packages.Keeper，而删除逻辑只遍历
// PkgLists，导致 keeper 残留成孤儿包。
func TestRemovePackageFilesDeletesKeeper(t *testing.T) {
	dir := mkPkgDir(t,
		"clickhouse-common-static-24.8.9.95.x86_64.rpm",
		"clickhouse-server-24.8.9.95.x86_64.rpm",
		"clickhouse-client-24.8.9.95.x86_64.rpm",
		"clickhouse-keeper-24.8.9.95.x86_64.rpm",
	)

	err := removePackageFiles(dir, deploy.Packages{
		PkgLists: []string{
			"clickhouse-common-static-24.8.9.95.x86_64.rpm",
			"clickhouse-server-24.8.9.95.x86_64.rpm",
			"clickhouse-client-24.8.9.95.x86_64.rpm",
		},
		Keeper: "clickhouse-keeper-24.8.9.95.x86_64.rpm",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertGone(t, dir, "clickhouse-common-static-24.8.9.95.x86_64.rpm")
	assertGone(t, dir, "clickhouse-server-24.8.9.95.x86_64.rpm")
	assertGone(t, dir, "clickhouse-client-24.8.9.95.x86_64.rpm")
	assertGone(t, dir, "clickhouse-keeper-24.8.9.95.x86_64.rpm")
}

// BuildPackages 预分配 pkgLists := make([]string, 3)，某个 module 缺失时槽位是
// 空字符串。path.Join(dir, "") == dir，直接 os.Remove 会删到包目录本身。
func TestRemovePackageFilesSkipsEmptySlotsAndKeepsDir(t *testing.T) {
	dir := mkPkgDir(t, "clickhouse-keeper-25.9.3.48.x86_64.rpm")

	// 孤儿版本：只剩 keeper，PkgLists 三个槽位全是空字符串。
	err := removePackageFiles(dir, deploy.Packages{
		PkgLists: []string{"", "", ""},
		Keeper:   "clickhouse-keeper-25.9.3.48.x86_64.rpm",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertGone(t, dir, "clickhouse-keeper-25.9.3.48.x86_64.rpm")

	info, statErr := os.Stat(dir)
	if statErr != nil {
		t.Fatalf("package directory must survive, stat err = %v", statErr)
	}
	if !info.IsDir() {
		t.Fatalf("package directory must stay a directory")
	}
}

// 单个文件删除失败不应中断整批，否则会留下残缺版本。
func TestRemovePackageFilesContinuesAfterFailure(t *testing.T) {
	dir := mkPkgDir(t,
		"clickhouse-common-static-23.8.9.54.x86_64.rpm",
		"clickhouse-client-23.8.9.54.x86_64.rpm",
	)

	// 用一个子目录冒充包文件：非空目录 os.Remove 必然失败。
	blocked := "clickhouse-server-23.8.9.54.x86_64.rpm"
	if err := os.Mkdir(filepath.Join(dir, blocked), 0755); err != nil {
		t.Fatalf("prepare blocked entry: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, blocked, "inner"), []byte("x"), 0644); err != nil {
		t.Fatalf("prepare blocked entry content: %v", err)
	}

	err := removePackageFiles(dir, deploy.Packages{
		PkgLists: []string{
			"clickhouse-common-static-23.8.9.54.x86_64.rpm",
			blocked,
			"clickhouse-client-23.8.9.54.x86_64.rpm",
		},
	})
	if err == nil {
		t.Fatalf("expected an error for the undeletable entry")
	}

	assertGone(t, dir, "clickhouse-common-static-23.8.9.54.x86_64.rpm")
	assertGone(t, dir, "clickhouse-client-23.8.9.54.x86_64.rpm")
	assertExists(t, dir, blocked)
}

// 版本没有 keeper 包时 Keeper 为空；已经不在磁盘上的文件也不该算失败。
func TestRemovePackageFilesToleratesMissingFiles(t *testing.T) {
	dir := mkPkgDir(t, "clickhouse-common-static-23.3.9.55.x86_64.rpm")

	err := removePackageFiles(dir, deploy.Packages{
		PkgLists: []string{
			"clickhouse-common-static-23.3.9.55.x86_64.rpm",
			"clickhouse-server-23.3.9.55.x86_64.rpm", // 不在磁盘上
			"",
		},
		Keeper: "", // 该版本没有 keeper
	})
	if err != nil {
		t.Fatalf("missing files should not be an error, got: %v", err)
	}

	assertGone(t, dir, "clickhouse-common-static-23.3.9.55.x86_64.rpm")
}
