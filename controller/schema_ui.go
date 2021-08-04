package controller

import (
	"github.com/gin-gonic/gin"
	"github.com/go-errors/errors"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
	"path"
	"strings"
)

const (
	GET_SCHEMA_UI_CREATE = "create"
	GET_SCHEMA_UI_CONFIG = "config"
)

type SchemaUIController struct {
	schema map[string]map[string]*common.Parameter
}

var schemaHandleFunc = map[string]func() map[string]*common.Parameter{
	GET_SCHEMA_UI_CREATE: RegistCreateClusterSchema,
	GET_SCHEMA_UI_CONFIG: RegistUpdateConfigSchema,
}

func getVersionLists() []common.Candidate {
	var versionLists []common.Candidate
	files, err := GetAllFiles(path.Join(config.GetWorkDirectory(), DefaultPackageDirectory))
	if err != nil {
		return nil
	}
	versions := GetAllVersions(files)
	for _, version := range versions {
		can := common.Candidate{
			Value: version,
		}
		versionLists = append(versionLists, can)
	}
	return versionLists
}

func NewSchemaUIController() *SchemaUIController {
	return &SchemaUIController{
		schema: make(map[string]map[string]*common.Parameter),
	}
}

func RegistCreateClusterSchema() map[string]*common.Parameter {
	params := make(map[string]*common.Parameter)
	PkgPath := "github.com/housepower/ckman/model"
	typCKManClickHouseConfig := PkgPath + ".CKManClickHouseConfig."
	params[typCKManClickHouseConfig+"Cluster"] = &common.Parameter{
		LabelZH:       "物理集群名",
		LabelEN:       "Cluster Name",
		DescriptionZH: "不得与本ckman管理的其他集群名重复",
		DescriptionEN: "not allow to duplicate with exist name",
	}
	params[typCKManClickHouseConfig+"LogicClusters"] = &common.Parameter{
		LabelZH:       "逻辑集群名",
		LabelEN:       "Logic Name",
		DescriptionZH: "逻辑集群，存在于物理集群之上",
		DescriptionEN: "require physical cluster",
	}
	params[typCKManClickHouseConfig+"SshUser"] = &common.Parameter{
		LabelZH:       "系统账户名",
		LabelEN:       "SSH Username",
		DescriptionZH: "必须有root或者sudo权限",
		DescriptionEN: "must have permission with root or sudo",
	}
	params[typCKManClickHouseConfig+"AuthenticateType"] = &common.Parameter{
		LabelZH:       "认证方式",
		LabelEN:       "Authenticate Type",
		DescriptionZH: "SSH 访问节点的方式，可使用公钥或者密码",
		DescriptionEN: "Authenticate type of connect node",
		Candidates: []common.Candidate{
			{Value: "0", LabelEN: "Public Key", LabelZH: "公钥认证",},
			{Value: "1", LabelEN: "Password(save)", LabelZH: "密码认证(保存密码)",},
			{Value: "2", LabelEN: "Password(not save)", LabelZH: "密码认证(不保存密码)",},
		},
		Default: "0",
	}
	params[typCKManClickHouseConfig+"SshPassword"] = &common.Parameter{
		LabelZH:       "系统账户密码",
		LabelEN:       "SSH Password",
		DescriptionZH: "不得为空",
		DescriptionEN: "can't be empty",
		Visiable:      "AuthenticateType != '0'",
		InputType:     common.InputPassword,
	}
	params[typCKManClickHouseConfig+"SshPort"] = &common.Parameter{
		LabelZH:       "SSH 端口",
		LabelEN:       "SSH Port",
		DescriptionZH: "不得为空",
		Default:       "22",
	}
	params[typCKManClickHouseConfig+"User"] = &common.Parameter{
		LabelZH:       "ClickHouse 用户名",
		LabelEN:       "ClickHouse Username",
		DescriptionZH: "不能是default用户",
		DescriptionEN: "can not be default",
	}
	params[typCKManClickHouseConfig+"Password"] = &common.Parameter{
		LabelZH:       "ClickHouse 用户密码",
		LabelEN:       "ClickHouse Password",
		DescriptionZH: "不能为空",
		DescriptionEN: "can't be empty",
		InputType:     common.InputPassword,
	}
	params[typCKManClickHouseConfig+"IsReplica"] = &common.Parameter{
		LabelZH:       "是否为多副本",
		LabelEN:       "Replica",
		DescriptionZH: "物理集群的每个shard是否为多副本, 生产环境建议每个shard为两副本",
		DescriptionEN: "Whether each Shard of the cluster is multiple replication, we suggest each shard have two copies.",
	}
	params[typCKManClickHouseConfig+"ManualShards"] = &common.Parameter{
		LabelZH:       "是否手工指定shard",
		LabelEN:       "ManualShards",
		DescriptionZH: "由ckman完成或者手工指定各结点分配到shard",
		DescriptionEN: "Completed by ckman or by manually designation of each node to Shard",
		Visiable:      `IsReplica == true`,
	}
	params[typCKManClickHouseConfig+"Hosts"] = &common.Parameter{
		LabelZH:       "集群结点IP地址列表",
		LabelEN:       "ClickHouse Node List",
		DescriptionZH: "由ckman完成各结点分配到shard。每输入框为单个IP，或者IP范围，或者网段掩码",
		DescriptionEN: "ClickHouse Node ip, support CIDR or Range.designation by ckman automatically",
		Required:      "ManualShards == false",
	}
	params[typCKManClickHouseConfig+"Shards"] = &common.Parameter{
		LabelZH:       "集群结点IP地址列表",
		LabelEN:       "Shards",
		DescriptionZH: "手工指定各结点分配到shard",
		DescriptionEN: "manually designation node to shard",
		Required:      "ManualShards == true",
	}
	params[typCKManClickHouseConfig+"Port"] = &common.Parameter{
		LabelZH: "TCP端口",
		LabelEN: "TCPPort",
		Default: "9000",
	}
	params[typCKManClickHouseConfig+"ZkNodes"] = &common.Parameter{
		LabelZH:       "ZooKeeper集群结点列表",
		LabelEN:       "Zookeeper Node List",
		DescriptionZH: "每段为单个IP，或者IP范围，或者网段掩码",
		DescriptionEN: "Zookeeper Node ip, support CIDR or Range.",
	}
	params[typCKManClickHouseConfig+"ZkPort"] = &common.Parameter{
		LabelZH: "ZooKeeper集群监听端口",
		LabelEN: "Zookeeper Port",
		Default: "2181",
	}
	params[typCKManClickHouseConfig+"ZkStatusPort"] = &common.Parameter{
		LabelZH:       "Zookeeper监控端口",
		LabelEN:       "Zookeeper Status Port",
		DescriptionZH: "暴露给mntr等四字命令的端口，zookeeper 3.5.0 以上支持",
		DescriptionEN: "expose to commands/mntr, zookeeper support it after 3.5.0",
		Default:       "8080",
	}
	params[typCKManClickHouseConfig+"Version"] = &common.Parameter{
		LabelZH:       "ClickHouse版本",
		LabelEN:       "Package Version",
		DescriptionZH: "需要部署的ClickHouse集群的版本号，需提前上传安装包",
		DescriptionEN: "which version of clickhouse will deployed, need upload rpm package before",
	}
	params[typCKManClickHouseConfig+"Path"] = &common.Parameter{
		LabelZH:       "数据存储路径",
		LabelEN:       "Data Path",
		DescriptionZH: "ClickHouse存储数据的路径，路径需要存在且不要以'/'结尾",
		DescriptionEN: "path need exist, please don't end with '/'",
	}
	params[typCKManClickHouseConfig+"Storage"] = &common.Parameter{
		LabelZH:       "集群存储配置",
		LabelEN:       "Storage Policy",
		DescriptionZH: "由disks, policies两部分构成。policies提到的disk名必须在disks中定义。ClickHouse内置了名为default的policy和disk。",
		DescriptionEN: "Composed of Disks, Policies. The Disk name mentioned by Policies must be defined in Disks. Clickhouse has built-in Policy and Disk named Default. ",
	}

	typShard := PkgPath + ".CkShard."
	params[typShard+"Replicas"] = &common.Parameter{
		LabelZH:       "分片",
		LabelEN:       "Shard",
		DescriptionZH: "分片内结点IP列表",
		DescriptionEN: "ip list in shard",
	}

	typReplica := PkgPath + ".CkReplica."
	params[typReplica+"Ip"] = &common.Parameter{
		LabelZH: "副本IP地址",
		LabelEN: "Replica IP",
	}
	params[typReplica+"HostName"] = &common.Parameter{
		LabelZH:  "副本hostname",
		LabelEN:  "Replica hostname",
		Visiable: "false",
	}

	typStorage := PkgPath + ".Storage."
	params[typStorage+"Disks"] = &common.Parameter{
		LabelZH:       "硬盘列表",
		LabelEN:       "Disk List",
		DescriptionZH: "定义的disks，后续在policies中用到",
		DescriptionEN: "defined Disks, follow-up in policies",
	}
	params[typStorage+"Policies"] = &common.Parameter{
		LabelZH: "存储策略列表",
		LabelEN: "Policies List",
	}

	typDisk := PkgPath + ".Disk."
	params[typDisk+"Name"] = &common.Parameter{
		LabelZH: "磁盘名称",
		LabelEN: "Name",
	}
	params[typDisk+"Type"] = &common.Parameter{
		LabelZH: "硬盘类型",
		LabelEN: "Disk Type",
		Default: "local",
		Candidates: []common.Candidate{
			{Value: "local", LabelEN: "Local", LabelZH: "本地磁盘"},
			{Value: "s3", LabelEN: "AWS S3", LabelZH: "AWS S3"},
			{Value: "hdfs", LabelEN: "HDFS", LabelZH: "HDFS"},
		},
	}
	params[typDisk+"DiskLocal"] = &common.Parameter{
		LabelZH:  "本地硬盘",
		LabelEN:  "Local",
		Visiable: "Type == 'local'",
	}
	params[typDisk+"DiskS3"] = &common.Parameter{
		LabelZH:  "AWS S3",
		LabelEN:  "AWS S3",
		Visiable: "Type == 's3'",
	}
	params[typDisk+"DiskHdfs"] = &common.Parameter{
		LabelZH:  "HDFS",
		LabelEN:  "HDFS",
		Visiable: "Type == 'hdfs'",
	}

	typDiskLocal := PkgPath + ".DiskLocal."
	params[typDiskLocal+"Path"] = &common.Parameter{
		LabelZH: "挂载路径",
		LabelEN: "Amount Path",
	}
	params[typDiskLocal+"KeepFreeSpaceBytes"] = &common.Parameter{
		LabelZH: "保留空闲空间大小",
		LabelEN: "KeepFreeSpaceBytes",
	}

	typDiskS3 := PkgPath + ".DiskS3."
	params[typDiskS3+"Endpoint"] = &common.Parameter{
		LabelZH: "S3端点URI",
		LabelEN: "Endpoint",
	}
	params[typDiskS3+"AccessKeyID"] = &common.Parameter{
		LabelZH: "AccessKeyID",
		LabelEN: "AccessKeyID",
	}
	params[typDiskS3+"SecretAccessKey"] = &common.Parameter{
		LabelZH: "SecretAccessKey",
		LabelEN: "SecretAccessKey",
	}
	params[typDiskS3+"Region"] = &common.Parameter{
		LabelZH: "Region",
		LabelEN: "Region",
	}
	params[typDiskS3+"UseEnvironmentCredentials"] = &common.Parameter{
		LabelZH: "UseEnvironmentCredentials",
		LabelEN: "UseEnvironmentCredentials",
	}
	params[typDiskS3+"Expert"] = &common.Parameter{
		LabelZH:       "专家模式",
		LabelEN:       "Expert Mode",
		DescriptionZH: "专家模式的S3参数",
		DescriptionEN: "configure S3 params by yourself",
	}

	typDiskHdfs := PkgPath + ".DiskHdfs."
	params[typDiskHdfs+"Endpoint"] = &common.Parameter{
		LabelZH: "HDFS端点URI",
		LabelEN: "Endpoint",
	}

	typPolicy := PkgPath + ".Policy."
	params[typPolicy+"Name"] = &common.Parameter{
		LabelZH: "策略名称",
		LabelEN: "Name",
	}
	params[typPolicy+"Volumns"] = &common.Parameter{
		LabelZH: "卷",
		LabelEN: "Volumns",
	}
	params[typPolicy+"MoveFactor"] = &common.Parameter{
		LabelZH:       "空闲占比阈值",
		DescriptionZH: "当一个volume空闲空间占比小于此值时，移动部分parts到下一个volume",
		Range:         &common.Range{Min: 0.0, Max: 1.0, Step: 0.1},
	}

	typVolume := PkgPath + ".Volumn."
	params[typVolume+"Name"] = &common.Parameter{
		LabelZH: "卷名称",
		LabelEN: "Name",
	}
	params[typVolume+".Disks"] = &common.Parameter{
		LabelZH: "磁盘",
		LabelEN: "Disks",
	}
	params[typVolume+".MaxDataPartSizeBytes"] = &common.Parameter{
		LabelZH: "MaxDataPartSizeBytes",
		LabelEN: "MaxDataPartSizeBytes",
	}
	params[typVolume+".Expert"] = &common.Parameter{
		LabelZH: "专家模式",
		LabelEN: "Expert",
	}

	return params
}

func RegistUpdateConfigSchema() map[string]*common.Parameter {
	return nil
}

func (ui *SchemaUIController) RegistSchemaInstance() {
	for k, v := range schemaHandleFunc {
		ui.schema[k] = v()
	}
}

// @Summary Get ui schema
// @Description Get ui schema
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json ""
// @Router /api/v1/ui/schema [get]
func (ui *SchemaUIController) GetUISchema(c *gin.Context) {
	Type := c.Query("type")
	if Type == "" {
		model.WrapMsg(c, model.INVALID_PARAMS, nil)
		return
	}
	params, ok := ui.schema[strings.ToLower(Type)]
	if !ok {
		model.WrapMsg(c, model.GET_SCHEMA_UI_FAILED, errors.Errorf("type %s is not regist", Type))
		return
	}

	// get version list every time
	PkgPath := "github.com/housepower/ckman/model"
	typCKManClickHouseConfig := PkgPath + ".CKManClickHouseConfig."
	if _, ok := params[typCKManClickHouseConfig+"Version"]; ok {
		params[typCKManClickHouseConfig+"Version"] = &common.Parameter{
			Candidates: getVersionLists(),
		}
	}

	var conf model.CKManClickHouseConfig
	schema, err := common.MarshalConfigSchema(conf, params)
	if err != nil {
		model.WrapMsg(c, model.GET_SCHEMA_UI_FAILED, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, schema)
}
