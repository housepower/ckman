package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/housepower/ckman/business"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
)

// Rebalance the whole cluster. It queries every shard to get partitions size, calculates a plan, for each selected partition, detach from source node, rsync to dest node and attach it.
// https://clickhouse.tech/docs/en/sql-reference/statements/alter/#synchronicity-of-alter-queries

// [FETCH PARTITION](https://clickhouse.tech/docs/en/sql-reference/statements/alter/partition/#alter_fetch-partition) drawbacks:
// - If inserting to the partition during FETCH(node2)-ATTACH(node2)-DROP(node1), you get data loss.
// - Non-replicated table doesn't support this command.

type CmdOptions struct {
	ShowVer    bool
	ChHosts    string
	ChPort     int
	ChUser     string
	ChPassword string
	ChDataDir  string
	OsUser     string
	OsPassword string
	OsPort     int
}

var (
	cmdOps         CmdOptions
	GitCommitHash  string
	BuildTimeStamp string
	chHosts        []string
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer:   false,
		ChPort:    9000,
		OsPort:    22,
		ChDataDir: "/var/lib",
	}

	// 2. Replace options with the corresponding env variable if present.
	common.EnvBoolVar(&cmdOps.ShowVer, "v")
	common.EnvStringVar(&cmdOps.ChHosts, "ch-hosts")
	common.EnvIntVar(&cmdOps.ChPort, "ch-port")
	common.EnvStringVar(&cmdOps.ChUser, "ch-user")
	common.EnvStringVar(&cmdOps.ChPassword, "ch-password")
	common.EnvStringVar(&cmdOps.ChDataDir, "ch-data-dir")
	common.EnvStringVar(&cmdOps.OsUser, "os-user")
	common.EnvStringVar(&cmdOps.OsPassword, "os-password")
	common.EnvIntVar(&cmdOps.OsPort, "os-port")

	// 3. Replace options with the corresponding CLI parameter if present.
	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
	flag.StringVar(&cmdOps.ChHosts, "ch-hosts", cmdOps.ChHosts, "a list of comma-separated clickhouse host ip address, one host from each shard")
	flag.IntVar(&cmdOps.ChPort, "ch-port", cmdOps.ChPort, "clickhouse tcp listen port")
	flag.StringVar(&cmdOps.ChUser, "ch-user", cmdOps.ChUser, "clickhouse user")
	flag.StringVar(&cmdOps.ChPassword, "ch-password", cmdOps.ChPassword, "clickhouse password")
	flag.StringVar(&cmdOps.ChDataDir, "ch-data-dir", cmdOps.ChDataDir, "clickhouse data directory, required for rebalancing non-replicated tables")
	flag.StringVar(&cmdOps.OsUser, "os-user", cmdOps.OsUser, "os user, required for rebalancing non-replicated tables")
	flag.StringVar(&cmdOps.OsPassword, "os-password", cmdOps.OsPassword, "os password")
	flag.IntVar(&cmdOps.OsPort, "os-port", cmdOps.OsPort, "ssh port")
	flag.Parse()
}

func main() {
	var err error
	log.InitLoggerConsole()
	initCmdOptions()
	if cmdOps.ShowVer {
		fmt.Println("Build Timestamp:", BuildTimeStamp)
		fmt.Println("Git Commit Hash:", GitCommitHash)
		os.Exit(0)
	}
	if len(cmdOps.ChHosts) == 0 {
		log.Logger.Fatalf("need to specify clickhouse hosts, one from each shard")
	}
	chHosts = strings.Split(cmdOps.ChHosts, ",")
	rebalancer := &business.CKRebalance{
		Hosts:      chHosts,
		Port:       cmdOps.ChPort,
		User:       cmdOps.ChUser,
		Password:   cmdOps.ChPassword,
		DataDir:    cmdOps.ChDataDir,
		OsUser:     cmdOps.OsUser,
		OsPassword: cmdOps.OsPassword,
		OsPort:     cmdOps.OsPort,
		DBTables:   make(map[string][]string),
		RepTables:  make(map[string]map[string]string),
	}

	defer common.Pool.Close()

	if err = rebalancer.InitCKConns(); err != nil {
		log.Logger.Fatalf("got error %+v", err)
	}
	defer common.CloseConns(chHosts)
	if err = rebalancer.GetTables(); err != nil {
		log.Logger.Fatalf("got error %+v", err)
	}
	if err = rebalancer.GetRepTables(); err != nil {
		log.Logger.Fatalf("got error %+v", err)
	}

	if err = rebalancer.DoRebalance(); err != nil {
		log.Logger.Fatalf("got error %+v", err)
	}
	log.Logger.Infof("rebalance done")
}
