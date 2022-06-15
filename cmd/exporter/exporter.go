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

// export data of given time range to HDFS

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
	MaxFileSize int
	HdfsAddr    string
	HdfsUser    string
	HdfsDir     string
	Parallelism int
}

var (
	cmdOps         CmdOptions
	GitCommitHash  string
	BuildTimeStamp string
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer:     false,
		ChPort:      9000,
		ChDatabase:  "default",
		DtBegin:     "1970-01-01",
		MaxFileSize: 1e10, //10GB, Parquet files need be small, nearly equal size
		HdfsUser:    "root",
		HdfsDir:     "",
		Parallelism: 4,
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
	common.EnvIntVar(&cmdOps.MaxFileSize, "max-file-size")
	common.EnvStringVar(&cmdOps.HdfsAddr, "hdfs-addr")
	common.EnvStringVar(&cmdOps.HdfsUser, "hdfs-user")
	common.EnvStringVar(&cmdOps.HdfsDir, "hdfs-dir")

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
	flag.IntVar(&cmdOps.MaxFileSize, "max-file-size", cmdOps.MaxFileSize, "max parquet file size")

	flag.StringVar(&cmdOps.HdfsAddr, "hdfs-addr", cmdOps.HdfsAddr, "hdfs_name_node_active_ip:port")
	flag.StringVar(&cmdOps.HdfsUser, "hdfs-user", cmdOps.HdfsUser, "hdfs user")
	flag.StringVar(&cmdOps.HdfsDir, "hdfs-dir", cmdOps.HdfsDir, "hdfs dir, under which a subdirectory will be created according to the given timestamp range, defaults to /user/<hdfs-user>")
	flag.IntVar(&cmdOps.Parallelism, "parallelism", cmdOps.Parallelism, "how many time slots are allowed to export to HDFS at the same time")
	flag.Parse()

	// 4. Normalization
	if cmdOps.HdfsDir == "" || cmdOps.HdfsDir == "." {
		cmdOps.HdfsDir = fmt.Sprintf("/user/%s", cmdOps.HdfsUser)
	}
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
	chTables := strings.Split(strings.TrimSpace(cmdOps.ChTables), ",")
	archive := &business.ArchiveHDFS{
		Hosts:       chHosts,
		Port:        cmdOps.ChPort,
		User:        cmdOps.ChUser,
		Password:    cmdOps.ChPassword,
		Database:    cmdOps.ChDatabase,
		Tables:      chTables,
		Begin:       cmdOps.DtBegin,
		End:         cmdOps.DtEnd,
		MaxFileSize: cmdOps.MaxFileSize,
		HdfsAddr:    cmdOps.HdfsAddr,
		HdfsUser:    cmdOps.HdfsUser,
		HdfsDir:     cmdOps.HdfsDir,
		Parallelism: cmdOps.Parallelism,
	}
	archive.FillArchiveDefault()
	common.Pool.Resize(archive.Parallelism)
	defer common.Pool.Close()
	if err = archive.InitConns(); err != nil {
		log.Logger.Fatalf("got error %+v", err)
	}
	defer common.CloseConns(chHosts)

	if err = archive.GetSortingInfo(); err != nil {
		log.Logger.Fatalf("got error %+v", err)
	}

	if err = archive.ClearHDFS(); err != nil {
		log.Logger.Fatalf("got error %+v", err)
	}

	if err = archive.ExportToHDFS(); err != nil {
		log.Logger.Fatalf("got error %+v", err)
	}
	log.Logger.Info("export success!")
}
