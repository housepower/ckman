package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/housepower/ckman/business"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"os"
)

// Create objects on a new ClickHouse instance.
// Refers to https://github.com/Altinity/clickhouse-operator/blob/master/pkg/model/schemer.go

type CmdOptions struct {
	ShowVer    bool
	SrcHost    string
	DstHost    string
	ChPort     int
	ChUser     string
	ChPassword string
}

var (
	cmdOps         CmdOptions
	GitCommitHash  string
	BuildTimeStamp string
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer: false,
		ChPort:  9000,
	}

	// 2. Replace options with the corresponding env variable if present.
	common.EnvBoolVar(&cmdOps.ShowVer, "v")
	common.EnvStringVar(&cmdOps.SrcHost, "src-host")
	common.EnvStringVar(&cmdOps.DstHost, "dst-host")
	common.EnvIntVar(&cmdOps.ChPort, "ch-port")
	common.EnvStringVar(&cmdOps.ChUser, "ch-user")
	common.EnvStringVar(&cmdOps.ChPassword, "ch-password")

	// 3. Replace options with the corresponding CLI parameter if present.
	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
	flag.StringVar(&cmdOps.SrcHost, "src-host", cmdOps.SrcHost, "clickhouse source host")
	flag.StringVar(&cmdOps.DstHost, "dst-host", cmdOps.DstHost, "clickhouse destination host")
	flag.IntVar(&cmdOps.ChPort, "ch-port", cmdOps.ChPort, "clickhouse tcp listen port")
	flag.StringVar(&cmdOps.ChUser, "ch-user", cmdOps.ChUser, "clickhouse user")
	flag.StringVar(&cmdOps.ChPassword, "ch-password", cmdOps.ChPassword, "clickhouse password")
	flag.Parse()
}

func main() {
	var names, statements []string
	var err error
	var db *sql.DB
	log.InitLoggerConsole()
	initCmdOptions()
	if cmdOps.ShowVer {
		fmt.Println("Build Timestamp:", BuildTimeStamp)
		fmt.Println("Git Commit Hash:", GitCommitHash)
		os.Exit(0)
	}
	if cmdOps.SrcHost == "" || cmdOps.DstHost == "" {
		log.Logger.Fatalf("need to specify clickhouse source host and dest host")
	} else if cmdOps.ChUser == "" || cmdOps.ChPassword == "" {
		log.Logger.Fatalf("need to specify clickhouse username and password")
	}

	db, err = common.ConnectClickHouse(cmdOps.DstHost, cmdOps.ChPort, model.ClickHouseDefaultDB, cmdOps.ChUser, cmdOps.ChPassword)
	if err != nil {
		return
	}
	defer common.CloseConns([]string{cmdOps.DstHost})

	if names, statements, err = business.GetCreateReplicaObjects(db, cmdOps.SrcHost); err != nil {
		log.Logger.Fatalf("got error %v", err)
	}
	log.Logger.Infof("names: %v", names)
	log.Logger.Infof("statements: %v", statements)
	num := len(names)
	for i := 0; i < num; i++ {
		log.Logger.Infof("executing %s", statements[i])
		if _, err = db.Exec(statements[i]); err != nil {
			return
		}
	}
}
