package main

import (
	"fmt"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/database/clickhouse"
	"gitlab.eoitek.net/EOI/ckman/log"
	"gitlab.eoitek.net/EOI/ckman/server"
	"gopkg.in/sevlyar/go-daemon.v0"
	"net/http"
	_ "net/http/pprof"
	"os"
	"syscall"
)

func main() {
	config.InitCmd()
	if err := config.ParseConfigFile(config.ConfigFilePath); err != nil {
		fmt.Printf("Parse config file %s fail: %v\n", config.ConfigFilePath, err)
		os.Exit(1)
	}
	log.InitLogger(config.LogFilePath, &config.GlobalConfig.Log)

	daemon.SetSigHandler(termHandler, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
	cntxt := &daemon.Context{
		PidFileName: config.PidFilePath,
		PidFilePerm: 0644,
		LogFilePerm: 0640,
		WorkDir:     "./",
		Umask:       027,
	}

	if config.Daemon {
		log.Logger.Info("daemonized ckman...")
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
	log.Logger.Infof("version: %v", config.Version)
	log.Logger.Infof("utc build time: %v", config.BuildTimeStamp)
	log.Logger.Infof("git commit hash: %v", config.GitCommitHash)

	// start pprof
	// http://127.0.0.1:6060/debug/pprof/
	if config.GlobalConfig.Pprof.Enabled {
		bind := fmt.Sprintf("%s:%d", config.GlobalConfig.Pprof.Ip, config.GlobalConfig.Pprof.Port)
		go func(addr string) {
			http.ListenAndServe(addr, nil)
		}(bind)
	}

	// create clickhouse client
	ck := clickhouse.NewCkClient(&config.GlobalConfig.ClickHouse)
	if err := ck.InitCkClient(); err != nil {
		log.Logger.Fatalf("create clickhouse client fail: %v", err)
	}
	log.Logger.Info("create clickhouse client success")

	// start http server
	svr := server.NewApiServer(&config.GlobalConfig, ck)
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
	ck.Stop()
}

func termHandler(sig os.Signal) error {
	log.Logger.Infof("got terminal signal %s", sig)
	return daemon.ErrStop
}
