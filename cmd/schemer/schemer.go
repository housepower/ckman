package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/housepower/ckman/business"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
)

// Create objects on a new ClickHouse instance.
// Refers to https://github.com/Altinity/clickhouse-operator/blob/master/pkg/model/schemer.go

type CmdOptions struct {
	ShowVer       bool
	SrcHost       string
	SrcChPort     int
	SrcChUser     string
	SrcChPassword string
	DstHost       string
	DstChPort     int
	DstChUser     string
	DstChPassword string
}

var (
	cmdOps         CmdOptions
	GitCommitHash  string
	BuildTimeStamp string
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer:       false,
		SrcChPort:     9000,
		SrcChUser:     "default",
		SrcChPassword: "",
		DstChPort:     9000,
		DstChUser:     "default",
		DstChPassword: "",
	}

	// 2. Replace options with the corresponding env variable if present.
	common.EnvBoolVar(&cmdOps.ShowVer, "v")
	common.EnvStringVar(&cmdOps.SrcHost, "src-host")
	common.EnvIntVar(&cmdOps.SrcChPort, "src-ch-port")
	common.EnvStringVar(&cmdOps.SrcChUser, "src-ch-user")
	common.EnvStringVar(&cmdOps.SrcChPassword, "src-ch-password")
	common.EnvStringVar(&cmdOps.DstHost, "dst-host")
	common.EnvIntVar(&cmdOps.DstChPort, "dst-ch-port")
	common.EnvStringVar(&cmdOps.DstChUser, "dst-ch-user")
	common.EnvStringVar(&cmdOps.DstChPassword, "dst-ch-password")

	// 3. Replace options with the corresponding CLI parameter if present.
	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
	flag.StringVar(&cmdOps.SrcHost, "src-host", cmdOps.SrcHost, "clickhouse source host")
	flag.IntVar(&cmdOps.SrcChPort, "src-ch-port", cmdOps.SrcChPort, "clickhouse source tcp listen port")
	flag.StringVar(&cmdOps.SrcChUser, "src-ch-user", cmdOps.SrcChUser, "clickhouse source user")
	flag.StringVar(&cmdOps.SrcChPassword, "src-ch-password", cmdOps.SrcChPassword, "clickhouse source password")
	flag.StringVar(&cmdOps.DstHost, "dst-host", cmdOps.DstHost, "clickhouse destination host")
	flag.IntVar(&cmdOps.DstChPort, "dst-ch-port", cmdOps.DstChPort, "clickhouse destination tcp listen port")
	flag.StringVar(&cmdOps.DstChUser, "dst-ch-user", cmdOps.DstChUser, "clickhouse destination user")
	flag.StringVar(&cmdOps.DstChPassword, "dst-ch-password", cmdOps.DstChPassword, "clickhouse destination password")
	flag.Parse()
}

func main() {
	var names, statements []string
	var err error
	var dstDB *sql.DB
	log.InitLoggerConsole()
	initCmdOptions()
	if cmdOps.ShowVer {
		fmt.Println("Build Timestamp:", BuildTimeStamp)
		fmt.Println("Git Commit Hash:", GitCommitHash)
		os.Exit(0)
	}
	if cmdOps.SrcHost == "" || cmdOps.DstHost == "" {
		log.Logger.Fatalf("need to specify clickhouse source host and dest host")
	}

	dstDB, err = common.ConnectClickHouse(cmdOps.DstHost, cmdOps.DstChPort, "default", cmdOps.DstChUser, cmdOps.DstChPassword)
	if err != nil {
		log.Logger.Fatalf("got error %+v", err)
		return
	}
	defer common.CloseConns([]string{cmdOps.DstHost})

	if names, statements, err = business.GetCreateReplicaObjects(dstDB, cmdOps.SrcHost, cmdOps.SrcChUser, cmdOps.SrcChPassword); err != nil {
		log.Logger.Fatalf("got error %v", err)
	}
	log.Logger.Infof("%v names: %v", cmdOps.SrcHost, names)
	log.Logger.Debugf("%v statements: %v", cmdOps.SrcHost, statements)
	num := len(names)
	for i := 0; i < num; i++ {
		log.Logger.Infof("going to create %s on %s by executing: %s", names[i], cmdOps.DstHost, statements[i])
		if _, err = dstDB.Exec(statements[i]); err != nil {
			log.Logger.Fatalf("got error %v", err)
			return
		}
	}
}
