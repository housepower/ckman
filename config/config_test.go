package config

import (
	"reflect"
	"testing"
)

func TestGetClusterPeers(t *testing.T) {
	saved, savedNodes := GlobalConfig, ClusterNodes
	defer func() { GlobalConfig, ClusterNodes = saved, savedNodes }()

	GlobalConfig.Server.Ip = "10.0.0.1"
	GlobalConfig.Server.Port = 8808
	ClusterNodes = []ClusterNode{
		{Ip: "10.0.0.1", Port: 8808}, // self
		{Ip: "10.0.0.2", Port: 8808}, // 同端口不同 IP
		{Ip: "10.0.0.1", Port: 8809}, // 同 IP 不同端口
		{Ip: "10.0.0.3", Port: 9000},
	}

	got := GetClusterPeers()
	want := []ClusterNode{
		{Ip: "10.0.0.2", Port: 8808},
		{Ip: "10.0.0.1", Port: 8809},
		{Ip: "10.0.0.3", Port: 9000},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetClusterPeers() = %v, want %v", got, want)
	}
}
