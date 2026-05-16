package main

/*
ckmanctl migrate -f /etc/ckman/conf/migrate.hjson
ckmanctl password
ckmanctl get znodes --path
ckmanctl delete znodes  cluster suball
ckmanctl delete znodes  cluster replica_queue
*/

import (
	"fmt"
	"strings"

	"github.com/alecthomas/kingpin/v2"

	"github.com/housepower/ckman/cmd/dumpjson"
	"github.com/housepower/ckman/cmd/metacache"
	"github.com/housepower/ckman/cmd/migrate"
	"github.com/housepower/ckman/cmd/password"
	"github.com/housepower/ckman/cmd/upgrade"
	"github.com/housepower/ckman/cmd/znodes"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/repository/sqlite"
)

var (
	migrateCmd = kingpin.Command("migrate", "migrate cluster config from old repersistence to new persistence")
	m_conf     = migrateCmd.Flag("conf", "migrate config file path").Default("/etc/ckman/conf/migrate.hjson").Short('c').String()

	dumpCmd  = kingpin.Command("dump-to-json", "export local SQLite db as legacy clusters.json")
	d_conf   = dumpCmd.Flag("conf", "ckman config file path").Short('c').Default("/etc/ckman/conf/ckman.hjson").String()
	d_output = dumpCmd.Flag("output", "output JSON file").Short('o').Default("conf/clusters.json").String()

	passCmd = kingpin.Command("password", "encrypt password")
	p_cwd   = passCmd.Flag("cwd", "current working directory").Short('p').Default("/etc/ckman").String()
	// ckmanctl get znodes /clickhouse/tables/chenyc1 -r -s 20
	getCmd       = kingpin.Command("get", "get options")
	g_znodes     = getCmd.Command("znodes", "get znodes")
	gz_host      = g_znodes.Flag("host", "host").Short('h').Default("127.0.0.1:2181").String()
	gz_recursive = g_znodes.Flag("recursive", "recursive").Short('r').Bool()
	gz_sort      = g_znodes.Flag("sort", "sort number").Short('s').Int()
	gz_path      = g_znodes.Arg("path", "path").Default("/clickhouse").String()

	deleteCmd   = kingpin.Command("cleanup", "cleanup options")
	d_znodes    = deleteCmd.Command("znodes", "delete znodes")
	dz_suball   = d_znodes.Command("suball", "delete all subnodes")
	dzs_cluster = dz_suball.Arg("cluster", "cluster").String()
	dzs_dryrun  = dz_suball.Flag("dryrun", "dryrun").Short('d').Bool()
	dzs_path    = dz_suball.Flag("path", "znode path").Short('p').String()
	dzs_conf    = dz_suball.Flag("conf", "config file path").Short('c').Default("/etc/ckman/conf/ckman.hjson").String()

	// ckmanctl delete znodes queue abc
	dz_queue           = d_znodes.Command("queue", "delete replica queue")
	dzq_cluster        = dz_queue.Arg("cluster", "cluster").String()
	dzq_dryrun         = dz_queue.Flag("dryrun", "dryrun").Short('d').Bool()
	dzq_sessionTimeout = dz_queue.Flag("session_timeout", "timeout").Short('t').Default("300").Int()
	dzq_conf           = dz_queue.Flag("conf", "config file path").Short('c').Default("/etc/ckman/conf/ckman.hjson").String()

	setCmd         = kingpin.Command("set", "set options")
	s_metacacheCmd = setCmd.Command("metacache", "set metacache options")
	sm_cluster     = s_metacacheCmd.Arg("cluster", "cluster").String()
	sm_conf        = s_metacacheCmd.Flag("conf", "config file path").Short('c').Default("/etc/ckman/conf/ckman.hjson").String()
	sm_dryrun      = s_metacacheCmd.Flag("dryrun", "dryrun").Short('d').Bool()

	upgradeCmd = kingpin.Command("upgrade", "upgrade options")
	u_backup   = upgradeCmd.Command("backup", "upgrade legacy Backup table to new BackupPolicy/BackupRun schema")
	ub_conf    = u_backup.Flag("conf", "ckman config file path").Short('c').Default("/etc/ckman/conf/ckman.hjson").String()
	ub_dryrun  = u_backup.Flag("dry-run", "print mapping plan without writing").Bool()
	ub_verbose = u_backup.Flag("verbose", "verbose log per row").Short('v').Bool()
	ub_force   = u_backup.Flag("force", "allow writing even if new tables non-empty").Bool()
	ub_cleanup = u_backup.Flag("cleanup", "delete migrated rows from old Backup table after success").Bool()
)

func main() {
	log.InitLoggerDefault("debug", []string{"/var/log/ckmanctl.log"})
	command := kingpin.Parse()
	firstCmd := strings.Split(command, " ")[0]
	switch firstCmd {
	case "migrate":
		migrate.MigrateHandle(*m_conf)
	case "dump-to-json":
		runDumpToJSON(*d_conf, *d_output)
	case "password":
		password.PasswordHandle(*p_cwd)
	case "get":
		znodes.ZCntHandle(znodes.ZCntOpts{
			Path:       *gz_path,
			Recursive:  *gz_recursive,
			SortNumber: *gz_sort,
			Zkhosts:    *gz_host,
		})
	case "cleanup":
		switch strings.Split(command, " ")[2] {
		case "suball":
			znodes.SuballHandle(znodes.ZSuballOpts{
				ClusterName: *dzs_cluster,
				ConfigFile:  *dzs_conf,
				Dryrun:      *dzs_dryrun,
				Node:        *dzs_path,
			})
		case "queue":
			znodes.ReplicaQueueHandle(znodes.ZReplicaQueueOpts{
				ClusterName:    *dzq_cluster,
				Dryrun:         *dzq_dryrun,
				SessionTimeout: *dzq_sessionTimeout,
				ConfigFile:     *dzq_conf,
			})
		}
	case "set":
		switch strings.Split(command, " ")[1] {
		case "metacache":
			metacache.MetacacheHandle(metacache.MetacacheOpts{
				ClusterName: *sm_cluster,
				ConfigFile:  *sm_conf,
				Dryrun:      *sm_dryrun,
			})
		}
	case "upgrade":
		switch strings.Split(command, " ")[1] {
		case "backup":
			upgrade.BackupUpgradeHandle(upgrade.BackupUpgradeOpts{
				ConfigFile: *ub_conf,
				DryRun:     *ub_dryrun,
				Verbose:    *ub_verbose,
				Force:      *ub_force,
				Cleanup:    *ub_cleanup,
			})
		}
	}
}

func runDumpToJSON(confPath, outPath string) {
	if err := config.ParseConfigFile(confPath, ""); err != nil {
		fmt.Printf("parse config %s failed: %v\n", confPath, err)
		return
	}
	locCfg := sqlite.LocalConfig{}
	if c, ok := config.GlobalConfig.PersistentConfig["local"]; ok {
		if fmtv, ok := c["format"].(string); ok {
			locCfg.Format = fmtv
		}
		if dir, ok := c["config_dir"].(string); ok {
			locCfg.ConfigDir = dir
		}
		if name, ok := c["config_file"].(string); ok {
			locCfg.ConfigFile = name
		}
	}
	locCfg.Normalize()

	if err := dumpjson.DumpFromDB(locCfg.DBPath(), outPath); err != nil {
		fmt.Printf("dump failed: %v\n", err)
		return
	}
	fmt.Printf("Dumped %s -> %s\n", locCfg.DBPath(), outPath)
}
