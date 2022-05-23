package model

import (
	"github.com/pkg/errors"
	"time"
)

var (
	CheckTimeOutErr = errors.New("check clickhouse timeout error")
)

const (
	TaskStatusWaiting int = 0
	TaskStatusRunning int = 1
	TaskStatusSuccess int = 2
	TaskStatusFailed  int = 3
	TaskStatusStopped int = 4

	TaskTypeCKDeploy     string = "clickhouse.deploy"
	TaskTypeCKUpgrade    string = "clickhouse.upgrade"
	TaskTypeCKAddNode    string = "clickhouse.addnode"
	TaskTypeCKDeleteNode string = "clickhouse.deletenode"
	TaskTypeCKDestory    string = "clickhouse.destory"
	TaskTypeCKSetting    string = "clickhouse.setting"

	TaskTypeZKDeploy  string = "zookeeper.deploy"
	TaskTypeZKUpgrade string = "zookeeper.upgrade"

	ALL_NODES_DEFAULT string = "all_hosts"
)

var TaskStatusMap = map[int]string{
	TaskStatusWaiting: "Waiting",
	TaskStatusRunning: "Running",
	TaskStatusSuccess: "Success",
	TaskStatusFailed:  "Failed",
	TaskStatusStopped: "Stopped",
}

type Internationalization struct {
	ZH string
	EN string
}

type NodeStatus struct {
	Host   string
	Status Internationalization
}

type Task struct {
	TaskId       string
	ClusterName  string
	ServerIp     string
	DeployConfig interface{}
	TaskType     string
	Status       int
	NodeStatus   []NodeStatus
	Message      string
	CreateTime   time.Time
	UpdateTime   time.Time
}

var (
	NodeStatusWating    = Internationalization{"未开始", "Wating"}
	NodeStatusInit      = Internationalization{"初始化", "Init"}
	NodeStatusPrepare   = Internationalization{"上传安装包", "Prepare"}
	NodeStatusInstall   = Internationalization{"安装", "Install"}
	NodeStatusUpgrade   = Internationalization{"升级", "Upgrade"}
	NodeStatusConfig    = Internationalization{"生成配置", "Config"}
	NodeStatusStart     = Internationalization{"启动服务", "Start"}
	NodeStatusStop      = Internationalization{"停止服务", "Stop"}
	NodeStatusUninstall = Internationalization{"卸载服务", "Uninstall"}
	NodeStatusRestart   = Internationalization{"重启服务", "Restart"}
	NodeStatusCheck     = Internationalization{"检查状态", "Check"}
	NodeStatusClearData = Internationalization{"清理数据", "ClearData"}
	NodeStatusConfigExt = Internationalization{"额外配置", "ConfigExt"}
	NodeStatusStore     = Internationalization{"保存配置", "Store"}
	NodeStatusDone      = Internationalization{"完成", "Done"}
	NodeStatusFailed    = Internationalization{"失败", "Failed"}
)

var (
	TaskOptionDeploy     = Internationalization{"部署集群", "Deploy"}
	TaskOptionUpgrade    = Internationalization{"升级集群", "Upgrade"}
	TaskOptionAddNode    = Internationalization{"增加节点", "AddNode"}
	TaskOptionDeleteNode = Internationalization{"删除节点", "DeleteNode"}
	TaskOptionDestory    = Internationalization{"销毁集群", "Destory"}
	TaskOptionSetting    = Internationalization{"设置集群", "Setting"}
)

var TaskOptionMap = map[string]Internationalization{
	TaskTypeCKDeploy:     TaskOptionDeploy,
	TaskTypeCKUpgrade:    TaskOptionUpgrade,
	TaskTypeCKAddNode:    TaskOptionAddNode,
	TaskTypeCKDeleteNode: TaskOptionDeleteNode,
	TaskTypeCKDestory:    TaskOptionDestory,
	TaskTypeCKSetting:    TaskOptionSetting,
	TaskTypeZKDeploy:     TaskOptionDeploy,
	TaskTypeZKUpgrade:    TaskOptionUpgrade,
}

type TaskStatusResp struct {
	TaskId      string
	ClusterName string
	Type        string
	Option      Internationalization
	NodeStatus  []NodeStatus
}

type TaskResp struct {
	TaskId      string
	ClusterName string
	Type        string
	Option      Internationalization
	Status      string
	Message     string
	CreateTime  time.Time
	UpdateTime  time.Time
	Duration    string
}
