package common

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/housepower/ckman/config"
	"github.com/pkg/errors"
)

const (
	DefaultPackageDirectory string = "package/clickhouse"

	PkgModuleCommon string = "common"
	PkgModuleClient string = "client"
	PkgModuleServer string = "server"
	PkgModuleKeeper string = "keeper"

	PkgSuffixRpm string = "rpm"
	PkgSuffixTgz string = "tgz"
	PkgSuffixDeb string = "deb"
)

type CkPackageFile struct {
	PkgName string
	Module  string
	Version string
	Arch    string
	Suffix  string
}

type CkPackageFiles []CkPackageFile

func (v CkPackageFiles) Len() int      { return len(v) }
func (v CkPackageFiles) Swap(i, j int) { v[i], v[j] = v[j], v[i] }
func (v CkPackageFiles) Less(i, j int) bool {
	return CompareClickHouseVersion(v[i].Version, v[j].Version) < 0
}

var CkPackages sync.Map

func parsePkgName(fname string) CkPackageFile {
	var module, version, arch, suffix string
	if !strings.HasPrefix(fname, "clickhouse") {
		return CkPackageFile{}
	}
	suffix = fname[strings.LastIndex(fname, ".")+1:]
	switch suffix {
	case PkgSuffixRpm:
		fields := strings.Split(fname, "-")
		module = fields[1]
		vidx := 0
		if module == PkgModuleCommon {
			vidx = 3
		} else {
			vidx = 2
		}
		if len(fields) == vidx+2 {
			/*
				clickhouse-common-static-22.3.2.2-2.x86_64.rpm
				clickhouse-client-22.3.2.2-2.noarch.rpm
				clickhouse-server-22.3.2.2-2.noarch.rpm
			*/
			version = fields[vidx]
			leftover := fields[vidx+1]
			arch = strings.Split(leftover, ".")[1]
		} else if len(fields) == vidx+1 {
			/*
				clickhouse-client-22.3.3.44.noarch.rpm
				clickhouse-common-static-22.3.3.44.x86_64.rpm
				clickhouse-server-22.3.3.44.noarch.rpm
			*/
			leftover := fields[vidx]
			leftover = strings.TrimSuffix(leftover, "."+suffix)
			idx := strings.LastIndex(leftover, ".")
			version = leftover[:idx]
			arch = leftover[idx+1:]
		}
	case PkgSuffixDeb:
		/*
			clickhouse-client_22.3.3.44_all.deb
			clickhouse-common-static_22.3.3.44_amd64.deb
			clickhouse-server_22.3.3.44_all.deb
		*/
		fields := strings.Split(fname, "_")
		prefix := strings.Split(fields[0], "-")
		module = prefix[1]
		version = fields[1]
		suffix := strings.Split(fields[2], ".")
		arch = suffix[0]
	case PkgSuffixTgz:
		fields := strings.Split(fname, "-")
		module = fields[1]
		vidx := 0
		if module == PkgModuleCommon {
			vidx = 3
		} else {
			vidx = 2
		}
		if len(fields) == vidx+2 {
			/*
				clickhouse-common-static-22.3.3.44-amd64.tgz
				clickhouse-client-22.3.3.44-amd64.tgz
				clickhouse-server-22.3.3.44-amd64.tgz
			*/
			version = fields[vidx]
			arch = strings.Split(fields[vidx+1], ".")[0]
		} else if len(fields) == vidx+1 {
			/*
				clickhouse-common-static-22.3.2.2.tgz
				clickhouse-client-22.3.2.2.tgz
				clickhouse-server-22.3.2.2.tgz
			*/
			leftover := fields[vidx]
			idx := strings.LastIndex(leftover, ".")
			version = leftover[:idx]
			arch = ""
		}
	default:
		return CkPackageFile{}
	}
	file := CkPackageFile{
		PkgName: fname,
		Module:  module,
		Version: version,
		Arch:    arch,
		Suffix:  suffix,
	}
	return file
}

func LoadPackages() error {
	var files CkPackageFiles
	CkPackages.Range(func(k, v interface{}) bool {
		CkPackages.Delete(k)
		return true
	})
	dir, err := os.ReadDir(path.Join(config.GetWorkDirectory(), DefaultPackageDirectory))
	if err != nil {
		return errors.Wrap(err, "")
	}

	for _, fi := range dir {
		if !fi.IsDir() {
			fname := fi.Name()
			file := parsePkgName(fname)
			files = append(files, file)
		}
	}

	sort.Sort(sort.Reverse(files))
	commpkgs := make([]CkPackageFile, 0)
	for _, file := range files {
		if file.Module == PkgModuleCommon {
			commpkgs = append(commpkgs, file)
		}
	}

	for _, comm := range commpkgs {
		for _, file := range files {
			if file.Version == comm.Version && file.Suffix == comm.Suffix {
				if file.Module == comm.Module && file.Arch != comm.Arch {
					// if pkg is comm, we need to match arch
					continue
				}
				//tgz client and server is real arch
				if file.Module != comm.Module {
					if file.Arch != "noarch" && file.Arch != "all" {
						if file.Arch != comm.Arch {
							continue
						}
					}
				}
				key := fmt.Sprintf("%s.%s", comm.Arch, comm.Suffix)
				value, ok := CkPackages.Load(key)
				if !ok {
					pkgs := CkPackageFiles{file}
					CkPackages.Store(key, pkgs)
				} else {
					pkgs := value.(CkPackageFiles)
					found := false
					for _, pkg := range pkgs {
						if reflect.DeepEqual(pkg, file) {
							found = true
							break
						}
					}
					if !found {
						pkgs = append(pkgs, file)
					}
					CkPackages.Store(key, pkgs)
				}
			}
		}
	}

	return nil
}

func GetAllPackages() map[string]CkPackageFiles {
	pkgs := make(map[string]CkPackageFiles, 0)
	_ = LoadPackages()
	CkPackages.Range(func(k, v interface{}) bool {
		key := k.(string)
		files := v.(CkPackageFiles)
		var commpkgs CkPackageFiles
		ckClientMap := make(map[string]bool)
		ckServerMap := make(map[string]bool)
		for _, file := range files {
			if file.Module == PkgModuleCommon {
				commpkgs = append(commpkgs, file)
			} else if file.Module == PkgModuleClient {
				ckClientMap[file.Version] = true
			} else if file.Module == PkgModuleServer {
				ckServerMap[file.Version] = true
			}
		}
		sort.Sort(sort.Reverse(commpkgs))
		var list CkPackageFiles
		for _, comm := range commpkgs {
			_, clientOk := ckClientMap[comm.Version]
			_, serverOk := ckServerMap[comm.Version]
			if clientOk && serverOk {
				list = append(list, comm)
			}
		}
		pkgs[key] = list
		return true
	})
	return pkgs
}
