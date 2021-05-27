package main

import (
	"flag"
	"fmt"
	"github.com/housepower/ckman/business"
	"github.com/housepower/ckman/log"
	"os"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/housepower/ckman/common"
)

// purge data of given time range

type CmdOptions struct {
	ShowVer     bool
	ChHosts     string
	ChPort      int
	ChUser      string
	ChPassword  string
	ChDatabase  string
	ChTables    string
	DtBegin     string
	DtEnd       string
}

var (
	cmdOps         CmdOptions
	GitCommitHash  string
	BuildTimeStamp string
	chTables       []string
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer:    false,
		ChPort:     9000,
		ChDatabase: "default",
		DtBegin:    "1970-01-01",
		DtEnd:      "1970-01-01",
	}

	// 2. Replace options with the corresponding env variable if present.
	common.EnvBoolVar(&cmdOps.ShowVer, "v")
	common.EnvStringVar(&cmdOps.ChHosts, "ch-hosts")
	common.EnvIntVar(&cmdOps.ChPort, "ch-port")
	common.EnvStringVar(&cmdOps.ChUser, "ch-user")
	common.EnvStringVar(&cmdOps.ChPassword, "ch-password")
	common.EnvStringVar(&cmdOps.ChDatabase, "ch-database")
	common.EnvStringVar(&cmdOps.ChTables, "ch-tables")
	common.EnvStringVar(&cmdOps.DtBegin, "dt-begin")
	common.EnvStringVar(&cmdOps.DtEnd, "dt-end")

	// 3. Replace options with the corresponding CLI parameter if present.
	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
	flag.StringVar(&cmdOps.ChHosts, "ch-hosts", cmdOps.ChHosts, "a list of comma-separated clickhouse host ip address, one host from each shard")
	flag.IntVar(&cmdOps.ChPort, "ch-port", cmdOps.ChPort, "clickhouse tcp listen port")
	flag.StringVar(&cmdOps.ChUser, "ch-user", cmdOps.ChUser, "clickhouse user")
	flag.StringVar(&cmdOps.ChPassword, "ch-password", cmdOps.ChPassword, "clickhouse password")
	flag.StringVar(&cmdOps.ChDatabase, "ch-database", cmdOps.ChDatabase, "clickhouse database")
	flag.StringVar(&cmdOps.ChTables, "ch-tables", cmdOps.ChTables, "a list of comma-separated table")
	flag.StringVar(&cmdOps.DtBegin, "dt-begin", cmdOps.DtBegin, "date begin(inclusive) in ISO8601 format, for example 1970-01-01")
	flag.StringVar(&cmdOps.DtEnd, "dt-end", cmdOps.DtEnd, "date end(exclusive) in ISO8601 format")
	flag.Parse()
}

func purge(p *business.PurgerRange) (err error) {
	for _, table := range chTables {
		if err = p.PurgeTable(table); err != nil {
			return
		}
	}
	return
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
		log.Logger.Fatalf("need to specify clickhouse hosts")
		os.Exit(0)
	}
	if len(cmdOps.ChTables) == 0 {
		log.Logger.Fatalf("need to specify clickhouse tables")
	}

	chHosts := strings.Split(strings.TrimSpace(cmdOps.ChHosts), ",")
	p := business.NewPurgerRange(chHosts, cmdOps.ChPort, cmdOps.ChUser, cmdOps.ChPassword, cmdOps.ChDatabase, cmdOps.DtBegin, cmdOps.DtEnd)
	if err = p.InitConns(); err != nil {
		log.Logger.Fatalf("got error %+v", err)
	}
	defer common.CloseConns(chHosts)
	chTables = strings.Split(cmdOps.ChTables, ",")

	if err = purge(p); err != nil {
		log.Logger.Fatalf("got error %+v", err)
	}
	log.Logger.Infof("purge done")
}
