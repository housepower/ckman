package common

import (
	"fmt"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type Replica struct {
	Ip       string
	HostName string
}

type Disk struct {
	Name      string
	Type      string
	DiskLocal *DiskLocal
	DiskS3    *DiskS3
}

type DiskLocal struct {
	Path               string
	KeepFreeSpaceBytes *int64
}

type DiskS3 struct {
	Endpoint                  string
	AccessKeyID               string
	SecretAccessKey           string
	Region                    *string
	UseEnvironmentCredentials *bool
	Expert                    map[string]string
}

type Volumn struct {
	Name string
	// Every disk shall be in storage.Disks
	Disks  []string
	Expert map[string]string
}

type Policy struct {
	Name       string
	Volumns    []Volumn
	MoveFactor *float32
}

type Storage struct {
	Disks    []Disk
	Policies []Policy
}

type CKManClickHouseConfig struct {
	Mode         string
	Cluster      string
	Version      string
	SshUser      string
	SshPassword  string
	IsReplica    bool
	ManualShards bool // one of Hosts, Shards is required
	Hosts        *[]string
	Shards       *[][]Replica
	Port         int
	ZkNodes      []string
	Storage      Storage
	ZooPath      map[string]string
}

func getParamsForAPICreateCluster() (params map[string]*Parameter) {
	params = make(map[string]*Parameter)
	PkgPath := "github.com/housepower/ckman/common"
	typCKManClickHouseConfig := PkgPath + ".CKManClickHouseConfig."
	params[typCKManClickHouseConfig+"Cluster"] = &Parameter{
		Label:       "物理集群名",
		Description: "不得与本ckman管理的其他集群名重复",
	}
	params[typCKManClickHouseConfig+"SshUser"] = &Parameter{
		Label:       "系统账户名",
		Description: "必须有root或者sudo权限",
	}
	params[typCKManClickHouseConfig+"SshPassword"] = &Parameter{
		Label:       "系统账户密码",
		Description: "不得为空",
	}
	params[typCKManClickHouseConfig+"IsReplica"] = &Parameter{
		Label:       "物理集群的每个shard是否为多副本",
		Description: "生产环境建议每个shard为两副本",
	}
	params[typCKManClickHouseConfig+"ManualShards"] = &Parameter{
		Label:              "手工指定各结点分配到shard",
		Description:        "由ckman完成或者手工指定各结点分配到shard",
		AvailableCondition: `IsReplica == true`,
	}
	params[typCKManClickHouseConfig+"Hosts"] = &Parameter{
		Label:             "集群结点IP地址列表",
		Description:       "由ckman完成各结点分配到shard。逗号分隔，每段为单个IP，或者IP范围，或者网段掩码",
		RequiredCondition: "ManualShards == false",
	}
	params[typCKManClickHouseConfig+"Shards"] = &Parameter{
		Label:             "集群结点IP地址列表",
		Description:       "手工指定各结点分配到shard",
		RequiredCondition: "ManualShards == true",
	}
	params[typCKManClickHouseConfig+"Port"] = &Parameter{
		Label:        "集群数据库监听TCP端口",
		DefaultValue: "9000",
	}
	params[typCKManClickHouseConfig+"ZkNodes"] = &Parameter{
		Label:       "ZooKeeper集群结点列表",
		Description: "逗号分隔，每段为单个IP，或者IP范围，或者网段掩码",
	}
	params[typCKManClickHouseConfig+"Storage"] = &Parameter{
		Label:       "集群存储配置",
		Description: "由disks, policies两部分构成。policies提到的disk名必须在disks中定义。ClickHouse内置了名为default的policy和disk。",
	}

	typReplica := PkgPath + ".Replica."
	params[typReplica+"Ip"] = &Parameter{
		Label:       "副本IP地址",
		Description: "副本IP地址",
	}
	params[typReplica+"Hostname"] = &Parameter{
		Label:              "副本hostname",
		Description:        "副本hostname",
		AvailableCondition: "false",
	}

	typStorage := PkgPath + ".Storage."
	params[typStorage+"Disks"] = &Parameter{
		Label:       "硬盘列表",
		Description: "定义的disks，后续在policies中用到",
	}
	params[typStorage+"Policies"] = &Parameter{
		Label:       "存储策略列表",
		Description: "存储策略列表",
	}

	typDisk := PkgPath + ".Disk."
	params[typDisk+"Type"] = &Parameter{
		Label:        "disk type",
		Description:  "硬盘类型",
		DefaultValue: "local",
		Candidates:   []string{"local", "s3", "hdfs"},
	}
	params[typDisk+"DiskLocal"] = &Parameter{
		Label:              "DiskLocal",
		Description:        "本地硬盘",
		AvailableCondition: `type == "local"`,
	}
	params[typDisk+"DiskS3"] = &Parameter{
		Label:              "DiskS3",
		Description:        "AWS S3",
		AvailableCondition: `type == "s3"`,
	}
	params[typDisk+"DiskHdfs"] = &Parameter{
		Label:              "DiskHdfs",
		Description:        "HDFS",
		AvailableCondition: `type == "hdfs"`,
	}

	typDiskLocal := PkgPath + ".DiskLocal."
	params[typDiskLocal+"Path"] = &Parameter{
		Label:       "挂载路径",
		Description: "挂载路径",
	}

	typDiskS3 := PkgPath + ".DiskS3."
	params[typDiskS3+"Endpoint"] = &Parameter{
		Label:       "S3端点URI",
		Description: "S3端点URI",
	}
	params[typDiskS3+"AccessKeyID"] = &Parameter{}
	params[typDiskS3+"SecretAccessKey"] = &Parameter{}
	params[typDiskS3+"Region"] = &Parameter{}
	params[typDiskS3+"UseEnvironmentCredentials"] = &Parameter{}
	params[typDiskS3+"Expert"] = &Parameter{
		Label:       "专家模式",
		Description: "专家模式的S3参数",
	}

	typDiskHdfs := PkgPath + ".DiskHdfs."
	params[typDiskHdfs+"Endpoint"] = &Parameter{}

	typPolicy := PkgPath + ".Policy."
	params[typPolicy+"Volumes"] = &Parameter{}
	params[typPolicy+"MoveFactor"] = &Parameter{
		Label:       "空闲占比阈值",
		Description: "当一个volume空闲空间占比小于此值时，移动部分parts到下一个volume",
	}

	typVolume := PkgPath + ".Volume."
	params[typVolume+".Disks"] = &Parameter{}
	params[typVolume+".MaxDataPartSizeBytes"] = &Parameter{}
	params[typVolume+".Expert"] = &Parameter{}
	return
}

type ConfigTestSuite struct {
	suite.Suite
	params map[string]*Parameter
}

func (su *ConfigTestSuite) SetupTest() {
	su.params = getParamsForAPICreateCluster()
}

func TestConfigTestSuite(t *testing.T) {
	suite.Run(t, new(ConfigTestSuite))
}

func (su *ConfigTestSuite) TestConfigSchema() {
	t := su.T()
	var c CKManClickHouseConfig
	data, err := MarshalConfigSchema(c, su.params)
	require.Nil(t, err)
	fmt.Printf("schema %+v\n", data)
}

func (su *ConfigTestSuite) TestConfigCodec() {
	t := su.T()
	var c CKManClickHouseConfig
	data, err := MarshalConfig(c, su.params)
	require.Nil(t, err)
	fmt.Printf("empty config %+v\n", data)
	fmt.Println()

	china := "china"
	hosts := []string{"192.168.1.1", "192.168.1.2", "192.168.1.3", "192.168.1.4"}
	shards := [][]Replica{
		{
			{"192.168.1.1", "node1"},
			{"192.168.1.2", "node2"},
		},
		{
			{"192.168.1.3", "node3"},
			{"192.168.1.4", "node4"},
		},
	}
	move_factor := float32(0.2)
	c = CKManClickHouseConfig{
		Mode:        "create",
		Cluster:     "abc",
		SshUser:     "root",
		SshPassword: "123456",
		Hosts:       &hosts,
		Port:        9000,
		IsReplica:   true,
		Shards:      &shards,
		Storage: Storage{
			Disks: []Disk{
				{
					Name:      "hdd1",
					Type:      "local",
					DiskLocal: &DiskLocal{Path: "/data01/clickhouse"},
				},
				{
					Name:      "hdd2",
					Type:      "local",
					DiskLocal: &DiskLocal{Path: "/data02/clickhouse"},
				},
				{
					Name: "external",
					Type: "s3",
					DiskS3: &DiskS3{
						Endpoint:        "http://192.168.102.114:3003/root/data/",
						AccessKeyID:     "minio",
						SecretAccessKey: "minio123",
						Region:          &china,
						Expert: map[string]string{
							"use_insecure_imds_request": "true",
							"connect_timeout_ms":        "1000",
						},
					},
				},
			},
			Policies: []Policy{
				{
					Name: "tiered2",
					Volumns: []Volumn{
						{
							Name:  "t1",
							Disks: []string{"default"},
						},
						{
							Name:  "t2",
							Disks: []string{"hdd1"},
						},
					},
					MoveFactor: &move_factor,
				},
				{
					Name: "tiered3",
					Volumns: []Volumn{
						{
							Name:  "t1",
							Disks: []string{"default"},
						},
						{
							Name:  "t2",
							Disks: []string{"hdd1", "hdd2"},
						},
						{
							Name:  "t3",
							Disks: []string{"external"},
						},
					},
				},
			},
		},
	}

	var bs []byte
	bs, err = json.Marshal(c)
	require.Nil(t, err)
	fmt.Printf("create cluster config(original) %+v\n", string(bs))
	fmt.Println()

	data, err = MarshalConfig(c, su.params)
	require.Nil(t, err)
	fmt.Printf("create cluster config(params, marshal) %+v\n", data)
	fmt.Println()

	var c2 CKManClickHouseConfig
	err = json.Unmarshal([]byte(data), &c2)
	require.Nil(t, err)
	fmt.Printf("create cluster config(params, unmarshal) %+v\n", spew.Sdump(c2))
	equals, first_diff := CompareConfig(c, c2, su.params)
	require.Equalf(t, true, equals, first_diff)

	c2.Storage.Disks = append(c2.Storage.Disks, Disk{
		Name:      "hdd3",
		Type:      "local",
		DiskLocal: &DiskLocal{Path: "/data03/clickhouse"},
	})
	equals, first_diff = CompareConfig(c, c2, su.params)
	require.Equalf(t, false, equals, first_diff)
}
