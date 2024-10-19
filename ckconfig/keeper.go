package ckconfig

import (
	"path"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
	"github.com/imdario/mergo"
)

func keeper_root(ipv6Enable bool) map[string]interface{} {
	output := make(map[string]interface{})
	if ipv6Enable {
		output["listen_host"] = "::"
	} else {
		output["listen_host"] = "0.0.0.0"
	}
	output["max_connections"] = 4096
	return output
}

func keeper_server(conf *model.CKManClickHouseConfig, ipv6Enable bool, idx int) map[string]interface{} {
	output := keeper_root(ipv6Enable)
	output["logger"] = keeper_logger(conf)
	keeper_server := make(map[string]interface{})
	mergo.Merge(&keeper_server, conf.KeeperConf.Expert)
	keeper_server["tcp_port"] = conf.KeeperConf.TcpPort
	keeper_server["server_id"] = idx
	keeper_server["log_storage_path"] = conf.KeeperConf.LogPath + "clickhouse/coordination/logs"
	keeper_server["snapshot_storage_path"] = conf.KeeperConf.SnapshotPath + "clickhouse/coordination/snapshots"
	keeper_server["coordination_settings"] = coordination_settings(conf.KeeperConf.Coordination)
	keeper_server["raft_configuration"] = raft_configuration(conf.KeeperConf)
	output["keeper_server"] = keeper_server
	return output
}

// https://github.com/ClickHouse/ClickHouse/blob/master/src/Coordination/CoordinationSettings.h
func coordination_settings(coordination model.Coordination) map[string]interface{} {
	output := make(map[string]interface{})
	mergo.Merge(&output, coordination.Expert)
	output["operation_timeout_ms"] = coordination.OperationTimeoutMs
	output["session_timeout_ms"] = coordination.SessionTimeoutMs
	if coordination.ForceSync {
		output["force_sync"] = "true"
	} else {
		output["force_sync"] = "false"
	}
	if coordination.AutoForwarding {
		output["auto_forwarding"] = "true"
	} else {
		output["auto_forwarding"] = "false"
	}
	return output
}

func keeper_logger(conf *model.CKManClickHouseConfig) map[string]interface{} {
	output := make(map[string]interface{})
	output["level"] = "debug"
	output["size"] = "1000M"
	output["count"] = 10
	if !conf.NeedSudo {
		output["log"] = path.Join(conf.Cwd, "log", "clickhouse-keeper", "clickhouse-keeper.log")
		output["errorlog"] = path.Join(conf.Cwd, "log", "clickhouse-keeper", "clickhouse-keeper.err.log")
	}
	return output

}

func raft_configuration(conf *model.KeeperConf) []map[string]interface{} {
	var outputs []map[string]interface{}
	for idx, node := range conf.KeeperNodes {
		output := make(map[string]interface{})
		output["server"] = map[string]interface{}{
			"id":       idx + 1,
			"hostname": node,
			"port":     conf.RaftPort,
		}
		outputs = append(outputs, output)
	}
	return outputs
}

func GenerateKeeperXML(filename string, conf *model.CKManClickHouseConfig, ipv6Enable bool, idx int) (string, error) {
	xml := common.NewXmlFile(filename)
	rootTag := "clickhouse"
	xml.Begin(rootTag)
	xml.Merge(keeper_server(conf, ipv6Enable, idx))
	xml.End(rootTag)
	if err := xml.Dump(); err != nil {
		return filename, err
	}
	return filename, nil
}
