package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/cron"
	"github.com/housepower/ckman/service/runner"

	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	_ "github.com/housepower/ckman/repository/dm8"
	_ "github.com/housepower/ckman/repository/local"
	_ "github.com/housepower/ckman/repository/mysql"
	_ "github.com/housepower/ckman/repository/postgres"
	"github.com/housepower/ckman/server"
	"github.com/housepower/ckman/service/nacos"
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
	Version         = ""
	BuildTimeStamp  = ""
	GitCommitHash   = ""
	Daemon          = false
	ConfigFilePath  = ""
	LogFilePath     = ""
	PidFilePath     = ""
	EncryptPassword = ""
)

//go:embed static/dist
var fs embed.FS

// @title CKMAN API
// @version 2.0
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
	var conf config.CKManConfig
	_ = common.DeepCopyByGob(&conf, &config.GlobalConfig)
	err := common.Gsypt.Unmarshal(&config.GlobalConfig)
	if err != nil {
		fmt.Printf("gsypt config file %s fail: %v\n", ConfigFilePath, err)
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
	//dump config to log must ensure the password not be decode
	DumpConfig(conf)
	if config.GlobalConfig.Server.Ip == "" {
		config.GlobalConfig.Server.Ip = common.GetOutboundIP().String()
	}
	signalCh := make(chan os.Signal, 1)

	defer common.Pool.Close()
	err = repository.InitPersistent()
	if err != nil {
		log.Logger.Fatalf("init persistent failed:%v", err)
	}

	nacosClient, err := nacos.InitNacosClient(&config.GlobalConfig.Nacos, LogFilePath)
	if err != nil {
		log.Logger.Fatalf("Failed to init nacos client, %v", err)
	}
	err = nacosClient.Start(config.GlobalConfig.Server.Ip, config.GlobalConfig.Server.Port)
	if err != nil {
		log.Logger.Fatalf("Failed to start nacos client, %v", err)
	}

	zookeeper.ZkServiceCache = cache.New(time.Hour, time.Minute)
	zookeeper.ZkServiceCache.OnEvicted(func(key string, value interface{}) {
		value.(*zookeeper.ZkService).Conn.Close()
	})

	runnerServ := runner.NewRunnerService(config.GlobalConfig.Server.Ip, config.GlobalConfig.Server)
	runnerServ.Start()
	defer runnerServ.Stop()

	// start http server
	svr := server.NewApiServer(&config.GlobalConfig, signalCh, fs)
	if err := svr.Start(); err != nil {
		log.Logger.Fatalf("start http server fail: %v", err)
	}
	defer svr.Stop()
	log.Logger.Infof("start http server %s:%d success", config.GlobalConfig.Server.Ip, config.GlobalConfig.Server.Port)

	cronSvr := cron.NewCronService(config.GlobalConfig.Cron)
	if err = cronSvr.Start(); err != nil {
		log.Logger.Fatalf("Failed to start cron service, %v", err)
	}
	defer cronSvr.Stop()
	//block here, waiting for terminal signal
	handleSignal(signalCh)
}

func handleSignal(ch chan os.Signal) {
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	sig := <-ch
	log.Logger.Infof("receive signal: %v", sig)
	log.Logger.Warn("ckman exiting...")
	switch sig {
	case syscall.SIGINT, syscall.SIGTERM:
		_ = termHandler()
	case syscall.SIGHUP:
		_ = termHandler()
		_ = reloadHandler()
	}
	signal.Stop(ch)
}

func termHandler() error {
	var hosts []string
	common.ConnectPool.Range(func(k, v interface{}) bool {
		hosts = append(hosts, k.(string))
		return true
	})

	common.CloseConns(hosts)

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
		Use: "ckman",
	}

	rootCmd.PersistentFlags().StringVarP(&ConfigFilePath, "conf", "c", "conf/ckman.hjson", "Task file path")
	rootCmd.PersistentFlags().StringVarP(&LogFilePath, "log", "l", "logs/ckman.log", "Log file path")
	rootCmd.PersistentFlags().StringVarP(&PidFilePath, "pid", "p", "run/ckman.pid", "Pid file path")
	rootCmd.PersistentFlags().StringVarP(&EncryptPassword, "encrypt", "e", "", "encrypt password")
	rootCmd.PersistentFlags().BoolVarP(&Daemon, "daemon", "d", false, "Run as daemon")
	rootCmd.AddCommand(VersionCmd)

	rootCmd.SetUsageFunc(func(cmd *cobra.Command) error {
		return nil
	})
	rootCmd.SetHelpCommand(&cobra.Command{
		Use:   "help",
		Short: "Help about any command",
		Long:  "Help about any command",
		Run: func(cmd *cobra.Command, args []string) {
			rootCmd.SetUsageFunc(nil)
			_ = rootCmd.Help()
			os.Exit(0)
		},
	})
	_ = rootCmd.Execute()
	if EncryptPassword != "" {
		fmt.Println(common.AesEncryptECB(EncryptPassword))
		os.Exit(0)
	}
	fmt.Println("ckman is used to manager and monitor clickhouse")
	fmt.Printf("ckman-%v is running...\n", Version)
	fmt.Printf("See more information in %s\n", LogFilePath)
}

func DumpConfig(conf config.CKManConfig) {
	data, err := json.MarshalIndent(conf, "", "  ")
	if err != nil {
		log.Logger.Errorf("marshal error: %v", err)
		return
	}
	log.Logger.Infof("%v", string(data))
}
