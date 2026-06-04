package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"syscall"
	"time"

	"github.com/housepower/ckman/cmd/dumpjson"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/repository/sqlite"
	"github.com/housepower/ckman/service/backup"
	"github.com/housepower/ckman/service/cron"
	"github.com/housepower/ckman/service/runner"

	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	_ "github.com/housepower/ckman/repository/dm8"
	_ "github.com/housepower/ckman/repository/sqlite"
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
	common.SetPkgPath(config.GlobalConfig.Server.PkgPath)
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

	// 在 InitLogger 之后注入 nacos_apply 使用的 logger（config 不能反向 import log）。
	config.SetNacosLogger(log.Logger)

	// 启动期尝试从 Nacos 拉取配置并合并到 GlobalConfig；失败/不可达则沿用本地，启动不阻塞。
	// 备份 Log 段以便在 merge 后立即按新值重建 logger（ApplyInitialNacos 故意不触发 applier）。
	logBefore := config.GlobalConfig.Log
	nacosClient.PullAndMerge(&config.GlobalConfig)
	if !reflect.DeepEqual(logBefore, config.GlobalConfig.Log) {
		log.InitLogger(LogFilePath, &config.GlobalConfig.Log)
		config.SetNacosLogger(log.Logger)
		log.Logger.Infof("log config updated by nacos at startup: %+v", config.GlobalConfig.Log)
	}

	// log applier 在 Log 段变化时重建 logger；其余 applier 待对应服务 Start 完成后再注册。
	config.RegisterApplier("log", logApplier)

	zookeeper.ZkServiceCache = cache.New(time.Hour, time.Minute)
	zookeeper.ZkServiceCache.OnEvicted(func(key string, value interface{}) {
		value.(*zookeeper.ZkService).Conn.Close()
	})

	self := net.JoinHostPort(config.GlobalConfig.Server.Ip, fmt.Sprint(config.GlobalConfig.Server.Port))
	chAdapter := backup.NewClickHouseAdapter()
	backupMaxConcurrent := config.GlobalConfig.Server.BackupMaxConcurrent
	backupStop, err := backup.Init(context.Background(), self, backupMaxConcurrent, chAdapter)
	if err != nil {
		log.Logger.Fatalf("init backup service failed: %v", err)
	}
	defer backupStop()
	log.Logger.Infof("start backup service success (self=%s, max_concurrent=%d)", self, backupMaxConcurrent)

	runnerServ := runner.NewRunnerService(config.GlobalConfig.Server.Ip, config.GlobalConfig.Server)
	// Expose to controller.StopTask so it can interrupt in-flight task
	// goroutines instead of merely flipping the DB status.
	runner.Default = runnerServ
	// Recover tasks left in Running status from a previous crashed process —
	// without this they would stay Running forever (polling only picks up
	// Waiting tasks). Must run before Start so the recovery happens before
	// new ticks fire.
	runnerServ.Boot()
	runnerServ.Start()
	defer runnerServ.Stop()

	// start http server
	svr := server.NewApiServer(&config.GlobalConfig, signalCh, fs, BuildTimeStamp)
	if err := svr.Start(); err != nil {
		log.Logger.Fatalf("start http server fail: %v", err)
	}
	defer svr.Stop()
	log.Logger.Infof("start http server %s:%d success", config.GlobalConfig.Server.Ip, config.GlobalConfig.Server.Port)

	cronSvr := cron.NewCronService(config.GlobalConfig)
	if err = cronSvr.Start(); err != nil {
		log.Logger.Fatalf("Failed to start cron service, %v", err)
	}
	defer func() { cronSvr.Stop() }()
	config.RegisterApplier("cron", cronApplier(&cronSvr))
	config.RegisterApplier("ck", ckPoolApplier)

	err = nacosClient.Start(config.GlobalConfig.Server.Ip, config.GlobalConfig.Server.Port)
	if err != nil {
		log.Logger.Fatalf("Failed to start nacos client, %v", err)
	}

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
		//_ = reloadHandler()
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

	if err := writeShutdownSnapshot(); err != nil {
		log.Logger.Warnf("shutdown snapshot failed: %v", err)
	}

	return nil
}

// writeShutdownSnapshot writes a JSON snapshot of the SQLite DB so that the
// operator has an emergency fallback for downgrading after a graceful stop.
// Best-effort: only attempts the dump if the local backend is in use; failures
// only log a Warn so they cannot block shutdown.
func writeShutdownSnapshot() error {
	if config.GlobalConfig.Server.PersistentPolicy != "local" {
		return nil
	}
	locCfg := sqlite.LocalConfig{}
	if c, ok := config.GlobalConfig.PersistentConfig["local"]; ok {
		if fmtv, ok := c["format"].(string); ok {
			locCfg.Format = fmtv
		}
		if dir, ok := c["config_dir"].(string); ok {
			locCfg.ConfigDir = dir
		}
		if name, ok := c["config_file"].(string); ok {
			locCfg.ConfigFile = name
		}
	}
	locCfg.Normalize()

	ts := time.Now().UTC().Format("20060102-150405")
	// DumpFromDB 内部会按需补 .json 扩展名（legacyjson.Normalize 行为）。
	// 最终落盘名形如 clusters.json.shutdown_snapshot.<ts>.json。
	snapshotBase := fmt.Sprintf("%s.json.shutdown_snapshot.%s", locCfg.ConfigFile, ts)
	outPath := filepath.Join(locCfg.ConfigDir, snapshotBase)
	return dumpjson.DumpFromDB(locCfg.DBPath(), outPath)
}

// func reloadHandler() error {
// 	env := os.Environ()
// 	mark := fmt.Sprintf("%s=%s", MARK_NAME, MARK_VALUE)
// 	env = append(env, mark)

// 	cmd := exec.Command(os.Args[0], os.Args[1:]...)
// 	cmd.Stdout = os.Stdout
// 	cmd.Stderr = os.Stderr
// 	cmd.Env = env
// 	return cmd.Start()
// }

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

// logApplier 在 Log 段变更时重新初始化 logger 并把新 logger 注入 config 包。
func logApplier(old, new *config.CKManConfig) {
	if reflect.DeepEqual(old.Log, new.Log) {
		return
	}
	log.InitLogger(LogFilePath, &new.Log)
	config.SetNacosLogger(log.Logger)
	log.Logger.Infof("log config reloaded: %+v", new.Log)
}

// cronApplier 在 Cron 段变更时停止旧调度器并启动新调度器。
// svrPtr 是 main.go 中 cronSvr 变量的地址；applier 内通过它替换实例。
func cronApplier(svrPtr **cron.CronService) config.Applier {
	return func(old, new *config.CKManConfig) {
		if reflect.DeepEqual(old.Cron, new.Cron) {
			return
		}
		(*svrPtr).Stop()
		ns := cron.NewCronService(*new)
		if err := ns.Start(); err != nil {
			log.Logger.Errorf("cron reload failed: %v", err)
			return
		}
		*svrPtr = ns
		log.Logger.Infof("cron reloaded")
	}
}

// ckPoolApplier 在 ClickHouse 段变更时关闭现有连接，下次取连接走新参数。
func ckPoolApplier(old, new *config.CKManConfig) {
	if reflect.DeepEqual(old.ClickHouse, new.ClickHouse) {
		return
	}
	var hosts []string
	common.ConnectPool.Range(func(k, _ interface{}) bool {
		hosts = append(hosts, k.(string))
		return true
	})
	common.CloseConns(hosts)
	log.Logger.Infof("clickhouse pool reloaded: %+v", new.ClickHouse)
}
