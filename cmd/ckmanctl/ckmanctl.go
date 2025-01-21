package main

/*
ckmanctl migrate -f /etc/ckman/conf/migrate.hjson
ckmanctl password
ckmanctl get znodes --path
ckmanctl delete znodes  cluster suball
ckmanctl delete znodes  cluster replica_queue
*/

import (
	"strings"

	"github.com/alecthomas/kingpin/v2"

	"github.com/housepower/ckman/cmd/metacache"
	"github.com/housepower/ckman/cmd/migrate"
	"github.com/housepower/ckman/cmd/password"
	"github.com/housepower/ckman/cmd/znodes"
	"github.com/housepower/ckman/log"
)

var (
	migrateCmd = kingpin.Command("migrate", "migrate cluster config from old repersistence to new persistence")
	m_conf     = migrateCmd.Flag("conf", "migrate config file path").Default("/etc/ckman/conf/migrate.hjson").Short('c').String()

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
)

func main() {
	log.InitLoggerDefault("debug", []string{"/var/log/ckmanctl.log"})
	command := kingpin.Parse()
	firstCmd := strings.Split(command, " ")[0]
	switch firstCmd {
	case "migrate":
		migrate.MigrateHandle(*m_conf)
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
		thirdCmd := strings.Split(command, " ")[2]
		if thirdCmd == "suball" {
			znodes.SuballHandle(znodes.ZSuballOpts{
				ClusterName: *dzs_cluster,
				ConfigFile:  *dzs_conf,
				Dryrun:      *dzs_dryrun,
				Node:        *dzs_path,
			})
		} else if thirdCmd == "queue" {
			znodes.ReplicaQueueHandle(znodes.ZReplicaQueueOpts{
				ClusterName:    *dzq_cluster,
				Dryrun:         *dzq_dryrun,
				SessionTimeout: *dzq_sessionTimeout,
				ConfigFile:     *dzq_conf,
			})
		}
	case "set":
		secondCmd := strings.Split(command, " ")[1]
		if secondCmd == "metacache" {
			metacache.MetacacheHandle(metacache.MetacacheOpts{
				ClusterName: *sm_cluster,
				ConfigFile:  *sm_conf,
				Dryrun:      *sm_dryrun,
			})
		}
	}
}
