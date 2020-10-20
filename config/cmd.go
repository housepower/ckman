package config

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
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

var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version info",
	Long:  "Print version info",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("version: %v\n", Version)
		fmt.Printf("utc build time: %v\n", BuildTimeStamp)
		fmt.Printf("git commit hash: %v\n", GitCommitHash)
		os.Exit(0)
	},
}

func InitCmd() {
	var rootCmd = &cobra.Command{
		Use: "ckman",
		Short: "ckman is used to manager and monitor clickhouse",
	}

	rootCmd.PersistentFlags().StringVarP(&ConfigFilePath, "conf", "c", "conf/ckman.yml", "Config file path")
	rootCmd.PersistentFlags().StringVarP(&LogFilePath, "log", "l", "log/ckman.log", "Log file path")
	rootCmd.PersistentFlags().StringVarP(&PidFilePath, "pid", "p", "run/ckman.pid", "Pid file path")
	rootCmd.PersistentFlags().BoolVarP(&Daemon, "daemon", "d", false, "Run as daemon")
	rootCmd.AddCommand(VersionCmd)

	rootCmd.Execute()
}