package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/log"
	"gitlab.eoitek.net/EOI/ckman/server"
	"gitlab.eoitek.net/EOI/ckman/service/clickhouse"
	"gitlab.eoitek.net/EOI/ckman/service/prometheus"
	"gopkg.in/sevlyar/go-daemon.v0"
	"os"
	"syscall"
)

var (
	Version        = ""
	BuildTimeStamp = ""
	GitCommitHash  = ""
	Daemon         = false
	ConfigFilePath = ""
	LogFilePath    = ""
	PidFilePath    = ""
)

// @title Swagger Example API
// @version 1.0
// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name token
// @BasePath /
func main() {
	InitCmd()
	if err := config.ParseConfigFile(ConfigFilePath); err != nil {
		fmt.Printf("Parse config file %s fail: %v\n", ConfigFilePath, err)
		os.Exit(1)
	}
	log.InitLogger(LogFilePath, &config.GlobalConfig.Log)

	daemon.SetSigHandler(termHandler, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
	cntxt := &daemon.Context{
		PidFileName: PidFilePath,
		PidFilePerm: 0644,
		LogFilePerm: 0640,
		WorkDir:     "./",
		Umask:       027,
	}

	if Daemon {
		d, err := cntxt.Reborn()
		if err != nil {
			log.Logger.Fatal(err)
		}
		if d != nil {
			return
		}
		defer cntxt.Release()
	}

	log.Logger.Info("ckman starting...")
	log.Logger.Infof("version: %v", Version)
	log.Logger.Infof("build time: %v", BuildTimeStamp)
	log.Logger.Infof("git commit hash: %v", GitCommitHash)

	// create prometheus service
	prom := prometheus.NewPrometheusService(&config.GlobalConfig.Prometheus)

	// parse brokers file
	err := clickhouse.UnmarshalClusters()
	if err != nil {
		log.Logger.Fatalf("parse brokers file fail: %v", err)
	}

	// start http server
	svr := server.NewApiServer(&config.GlobalConfig, prom)
	if err := svr.Start(); err != nil {
		log.Logger.Fatalf("start http server fail: %v", err)
	}
	log.Logger.Infof("start http server %s:%d success", config.GlobalConfig.Server.Ip, config.GlobalConfig.Server.Port)

	//block here, waiting for terminal signal
	if err := daemon.ServeSignals(); err != nil {
		log.Logger.Errorf("waiting for signal fail: %v", err)
	}

	log.Logger.Warn("ckman exiting...")
	svr.Stop()
}

func termHandler(sig os.Signal) error {
	log.Logger.Infof("got terminal signal %s", sig)
	return daemon.ErrStop
}

var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version info",
	Long:  "Print version info",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("version: %v\n", Version)
		fmt.Printf("build time: %v\n", BuildTimeStamp)
		fmt.Printf("git commit hash: %v\n", GitCommitHash)
		os.Exit(0)
	},
}

func InitCmd() {
	var rootCmd = &cobra.Command{
		Use:   "ckman",
		Short: "ckman is used to manager and monitor clickhouse",
	}

	rootCmd.PersistentFlags().StringVarP(&ConfigFilePath, "conf", "c", "conf/ckman.yml", "Config file path")
	rootCmd.PersistentFlags().StringVarP(&LogFilePath, "log", "l", "logs/ckman.log", "Log file path")
	rootCmd.PersistentFlags().StringVarP(&PidFilePath, "pid", "p", "run/ckman.pid", "Pid file path")
	rootCmd.PersistentFlags().BoolVarP(&Daemon, "daemon", "d", false, "Run as daemon")
	rootCmd.AddCommand(VersionCmd)

	rootCmd.Execute()
}
