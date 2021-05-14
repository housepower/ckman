package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/server"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/nacos"
	"github.com/housepower/ckman/service/prometheus"
	"github.com/housepower/ckman/service/zookeeper"
	"github.com/patrickmn/go-cache"
	"github.com/spf13/cobra"
	"gopkg.in/sevlyar/go-daemon.v0"
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
	if err := config.ParseConfigFile(ConfigFilePath, Version); err != nil {
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

	selfIP := GetOutboundIP().String()
	signalCh := make(chan os.Signal, 1)
	// parse brokers file
	err := clickhouse.ParseCkClusterConfigFile()
	if err != nil {
		log.Logger.Fatalf("parse brokers file fail: %v", err)
	}

	nacosClient, err := nacos.InitNacosClient(&config.GlobalConfig.Nacos, LogFilePath)
	if err != nil {
		log.Logger.Fatalf("Failed to init nacos client, %v", err)
	}
	err = nacosClient.Start(selfIP, config.GlobalConfig.Server.Port)
	if err != nil {
		log.Logger.Fatalf("Failed to start nacos client, %v", err)
	}

	zookeeper.ZkServiceCache = cache.New(time.Hour, time.Minute)
	// create prometheus service
	prom := prometheus.NewPrometheusService(&config.GlobalConfig.Prometheus)

	// start http server
	svr := server.NewApiServer(&config.GlobalConfig, prom, signalCh, nacosClient)
	if err := svr.Start(); err != nil {
		log.Logger.Fatalf("start http server fail: %v", err)
	}
	log.Logger.Infof("start http server %s:%d success", selfIP, config.GlobalConfig.Server.Port)

	//block here, waiting for terminal signal
	handleSignal(signalCh, svr)
}

func handleSignal(ch chan os.Signal, svr *server.ApiServer) {
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	sig := <-ch
	log.Logger.Infof("receive signal: %v", sig)
	log.Logger.Warn("ckman exiting...")
	switch sig {
	case syscall.SIGINT, syscall.SIGTERM:
		_ = termHandler(svr)
	case syscall.SIGHUP:
		_ = termHandler(svr)
		_ = reloadHandler()
	}
	signal.Stop(ch)
}

func termHandler(svr *server.ApiServer) error {
	if err := svr.Stop(); err != nil {
		return err
	}

	clickhouse.CkServices.Range(func(k, v interface{}) bool {
		service := v.(*clickhouse.ClusterService)
		_ = service.Service.Stop()
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

	rootCmd.PersistentFlags().StringVarP(&ConfigFilePath, "conf", "c", "conf/ckman.yaml", "Config file path")
	rootCmd.PersistentFlags().StringVarP(&LogFilePath, "log", "l", "logs/ckman.log", "Log file path")
	rootCmd.PersistentFlags().StringVarP(&PidFilePath, "pid", "p", "run/ckman.pid", "Pid file path")
	rootCmd.PersistentFlags().BoolVarP(&Daemon, "daemon", "d", false, "Run as daemon")
	rootCmd.AddCommand(VersionCmd)

	_ = rootCmd.Execute()
}

// GetOutboundIP get preferred outbound ip of this machine
//https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Logger.Fatal(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP
}
