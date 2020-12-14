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
	"os/exec"
	"os/signal"
	"syscall"
)

const (
	MARK_NAME  = "_GO_CKMAN_RELOAD"
	MARK_VALUE = "1"
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

	cntxt := &daemon.Context{
		PidFileName: PidFilePath,
		PidFilePerm: 0644,
		LogFilePerm: 0640,
		WorkDir:     "./",
		Umask:       027,
	}

	if Daemon && os.Getenv(MARK_NAME) != MARK_VALUE {
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

	signalCh := make(chan os.Signal, 1)
	// create prometheus service
	prom := prometheus.NewPrometheusService(&config.GlobalConfig.Prometheus)

	// parse brokers file
	err := clickhouse.UnmarshalClusters()
	if err != nil {
		log.Logger.Fatalf("parse brokers file fail: %v", err)
	}

	// start http server
	svr := server.NewApiServer(&config.GlobalConfig, prom, signalCh)
	if err := svr.Start(); err != nil {
		log.Logger.Fatalf("start http server fail: %v", err)
	}
	log.Logger.Infof("start http server %s:%d success", config.GlobalConfig.Server.Ip, config.GlobalConfig.Server.Port)

	//block here, waiting for terminal signal
	handleSignal(signalCh, svr)
}

func handleSignal(ch chan os.Signal, svr *server.ApiServer) {
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	for {
		sig := <-ch
		log.Logger.Infof("receive signal: %v", sig)
		log.Logger.Warn("ckman exiting...")
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			termHandler(svr)
		case syscall.SIGHUP:
			termHandler(svr)
			reloadHandler()
		}
		break
	}
	signal.Stop(ch)
}

func termHandler(svr *server.ApiServer) error {
	if err := svr.Stop(); err != nil {
		return err
	}

	clickhouse.CkServices.Range(func(k, v interface{}) bool {
		service := v.(*clickhouse.ClusterService)
		service.Service.Stop()
		return true
	})

	return nil
}

func reloadHandler() error {
	env := os.Environ()
	mark := fmt.Sprintf("%s=%s", MARK_NAME, MARK_VALUE)
	env = append(env, mark)

	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = env
	return cmd.Start()
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
