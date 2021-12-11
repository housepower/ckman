package controller

import (
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
	"io"
	"net/http"
	"path"
	"strings"
)

const (
	GET_SCHEMA_UI_DEPLOY = "deploy"
	GET_SCHEMA_UI_CONFIG = "config"
)

var SchemaUIMapping map[string]common.ConfigParams

type SchemaUIController struct{}

var schemaHandleFunc = map[string]func() common.ConfigParams{
	GET_SCHEMA_UI_DEPLOY: RegistCreateClusterSchema,
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
	return &SchemaUIController{}
}

func RegistCreateClusterSchema() common.ConfigParams {
	var params common.ConfigParams = make(map[string]*common.Parameter)
	var conf model.CKManClickHouseConfig
	params.MustRegister(conf, "Cluster", &common.Parameter{
		LabelZH:       "物理集群名",
		LabelEN:       "Cluster Name",
		DescriptionZH: "不得与本ckman管理的其他集群名重复",
		DescriptionEN: "not allow to duplicate with exist name",
	})
	params.MustRegister(conf, "LogicCluster", &common.Parameter{
		LabelZH:       "逻辑集群名",
		LabelEN:       "Logic Name",
		DescriptionZH: "逻辑集群，存在于物理集群之上",
		DescriptionEN: "require physical cluster",
	})
	params.MustRegister(conf, "SshUser", &common.Parameter{
		LabelZH:       "系统账户名",
		LabelEN:       "SSH Username",
		DescriptionZH: "必须有root或者sudo权限",
		DescriptionEN: "must have permission with root or sudo",
	})
	params.MustRegister(conf, "AuthenticateType", &common.Parameter{
		LabelZH:       "认证方式",
		LabelEN:       "Authenticate Type",
		DescriptionZH: "SSH 访问节点的方式，可使用公钥或者密码，使用公钥时需将公钥文件放到conf目录下",
		DescriptionEN: "Authenticate type of connect node, you need copy id_rsa to conf/ if use public key",
		Candidates: []common.Candidate{
			{Value: "0", LabelEN: "Password(save)", LabelZH: "密码认证(保存密码)",},
			{Value: "1", LabelEN: "Password(not save)", LabelZH: "密码认证(不保存密码)",},
			{Value: "2", LabelEN: "Public Key", LabelZH: "公钥认证",},
		},
		Default: "2",
	})
	params.MustRegister(conf, "SshPassword", &common.Parameter{
		LabelZH:       "系统账户密码",
		LabelEN:       "SSH Password",
		DescriptionZH: "不得为空",
		DescriptionEN: "can't be empty",
		Visiable:      "AuthenticateType != '2'",
		InputType:     common.InputPassword,
	})
	params.MustRegister(conf, "SshPort", &common.Parameter{
		LabelZH:       "SSH 端口",
		LabelEN:       "SSH Port",
		DescriptionZH: "不得为空",
		Default:       "22",
	})
	params.MustRegister(conf, "Password", &common.Parameter{
		LabelZH:   "默认用户密码",
		LabelEN:   "Default Password",
		InputType: common.InputPassword,
	})
	params.MustRegister(conf, "IsReplica", &common.Parameter{
		LabelZH:       "是否为多副本",
		LabelEN:       "Replica",
		DescriptionZH: "物理集群的每个shard是否为多副本, 生产环境建议每个shard为两副本",
		DescriptionEN: "Whether each Shard of the cluster is multiple replication, we suggest each shard have two copies.",
	})
	params.MustRegister(conf, "Hosts", &common.Parameter{
		LabelZH:       "集群结点IP地址列表",
		LabelEN:       "ClickHouse Node List",
		DescriptionZH: "由ckman完成各结点分配到shard。每输入框为单个IP，或者IP范围，或者网段掩码",
		DescriptionEN: "ClickHouse Node ip, support CIDR or Range.designation by ckman automatically",
	})
	params.MustRegister(conf, "Port", &common.Parameter{
		LabelZH: "TCP端口",
		LabelEN: "TCPPort",
		Default: "9000",
	})
	params.MustRegister(conf, "ZkNodes", &common.Parameter{
		LabelZH:       "ZooKeeper集群结点列表",
		LabelEN:       "Zookeeper Node List",
		DescriptionZH: "每段为单个IP，或者IP范围，或者网段掩码",
		DescriptionEN: "Zookeeper Node ip, support CIDR or Range.",
	})
	params.MustRegister(conf, "ZkPort", &common.Parameter{
		LabelZH: "ZooKeeper集群监听端口",
		LabelEN: "Zookeeper Port",
		Default: "2181",
	})
	params.MustRegister(conf, "ZkStatusPort", &common.Parameter{
		LabelZH:       "Zookeeper监控端口",
		LabelEN:       "Zookeeper Status Port",
		DescriptionZH: "暴露给mntr等四字命令的端口，zookeeper 3.5.0 以上支持",
		DescriptionEN: "expose to commands/mntr, zookeeper support it after 3.5.0",
		Default:       "8080",
	})
	params.MustRegister(conf, "PromHost", &common.Parameter{
		LabelZH:  "Promethues 地址",
		LabelEN:  "Prometheus Host",
		Default:  "127.0.0.1",
		Required: "false",
	})
	params.MustRegister(conf, "PromPort", &common.Parameter{
		LabelZH:  "Promethues 端口",
		LabelEN:  "Prometheus Port",
		Default:  "9090",
		Required: "false",
	})
	params.MustRegister(conf, "Path", &common.Parameter{
		LabelZH:       "数据存储路径",
		LabelEN:       "Data Path",
		DescriptionZH: "ClickHouse存储数据的路径，路径需要存在且必须以'/'结尾",
		DescriptionEN: "path need exist, must end with '/'",
		Regexp:        "^/.+/$",
	})
	params.MustRegister(conf, "Storage", &common.Parameter{
		LabelZH:       "集群存储配置",
		LabelEN:       "Storage Policy",
		DescriptionZH: "由disks, policies两部分构成。policies提到的disk名必须在disks中定义。ClickHouse内置了名为default的policy和disk。",
		DescriptionEN: "Composed of Disks, Policies. The Disk name mentioned by Policies must be defined in Disks. Clickhouse has built-in Policy and Disk named Default. ",
	})
	params.MustRegister(conf, "MergeTreeConf", &common.Parameter{
		LabelZH:  "MergeTree配置",
		LabelEN:  "MergeTree Config",
		Required: "false",
	})
	params.MustRegister(conf, "UsersConf", &common.Parameter{
		LabelZH:  "用户管理配置",
		LabelEN:  "User Config",
		Required: "false",
	})

	var storage model.Storage
	params.MustRegister(storage, "Disks", &common.Parameter{
		LabelZH:       "硬盘列表",
		LabelEN:       "Disk List",
		DescriptionZH: "定义的disks，后续在policies中用到",
		DescriptionEN: "defined Disks, follow-up in policies",
		Required:      "false",
	})
	params.MustRegister(storage, "Policies", &common.Parameter{
		LabelZH:  "存储策略列表",
		LabelEN:  "Policies List",
		Required: "false",
	})

	var disk model.Disk
	params.MustRegister(disk, "Name", &common.Parameter{
		LabelZH: "磁盘名称",
		LabelEN: "Name",
	})
	params.MustRegister(disk, "Type", &common.Parameter{
		LabelZH: "硬盘类型",
		LabelEN: "Disk Type",
		Default: "local",
		Candidates: []common.Candidate{
			{Value: "local", LabelEN: "Local", LabelZH: "本地磁盘"},
			{Value: "s3", LabelEN: "AWS S3", LabelZH: "AWS S3"},
			{Value: "hdfs", LabelEN: "HDFS", LabelZH: "HDFS"},
		},
	})
	params.MustRegister(disk, "DiskLocal", &common.Parameter{
		LabelZH:  "本地硬盘",
		LabelEN:  "Local",
		Visiable: "Type == 'local'",
	})
	params.MustRegister(disk, "DiskS3", &common.Parameter{
		LabelZH:  "AWS S3",
		LabelEN:  "AWS S3",
		Visiable: "Type == 's3'",
	})
	params.MustRegister(disk, "DiskHdfs", &common.Parameter{
		LabelZH:  "HDFS",
		LabelEN:  "HDFS",
		Visiable: "Type == 'hdfs'",
	})

	var disklocal model.DiskLocal
	params.MustRegister(disklocal, "Path", &common.Parameter{
		LabelZH:       "挂载路径",
		LabelEN:       "Amount Path",
		DescriptionZH: "必须存在，clickhouse用户可访问， 且必须以'/'开头和结尾",
		DescriptionEN: "need exist, can be accessed by clickhouse, and must begin and end with '/'",
		Regexp:        "^/.+/$",
	})
	params.MustRegister(disklocal, "KeepFreeSpaceBytes", &common.Parameter{
		LabelZH: "保留空闲空间大小",
		LabelEN: "KeepFreeSpaceBytes",
	})

	var disks3 model.DiskS3
	params.MustRegister(disks3, "Endpoint", &common.Parameter{
		LabelZH: "S3端点URI",
		LabelEN: "Endpoint",
		Regexp:  "^(http|https)://.+/$",
	})

	params.MustRegister(disks3, "AccessKeyID", &common.Parameter{
		LabelZH: "AccessKeyID",
		LabelEN: "AccessKeyID",
	})
	params.MustRegister(disks3, "SecretAccessKey", &common.Parameter{
		LabelZH: "SecretAccessKey",
		LabelEN: "SecretAccessKey",
	})
	params.MustRegister(disks3, "Region", &common.Parameter{
		LabelZH: "Region",
		LabelEN: "Region",
	})
	params.MustRegister(disks3, "UseEnvironmentCredentials", &common.Parameter{
		LabelZH: "UseEnvironmentCredentials",
		LabelEN: "UseEnvironmentCredentials",
	})
	params.MustRegister(disks3, "Expert", &common.Parameter{
		LabelZH:       "专家模式",
		LabelEN:       "Expert Mode",
		DescriptionZH: "专家模式的S3参数, 请参考: https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-s3",
		DescriptionEN: "configure S3 params by yourself, please visit: https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-s3",
	})

	var diskhdfs model.DiskHdfs
	params.MustRegister(diskhdfs, "Endpoint", &common.Parameter{
		LabelZH: "HDFS端点URI",
		LabelEN: "Endpoint",
		Regexp:  "^hdfs://.+/$",
	})

	var policy model.Policy
	params.MustRegister(policy, "Name", &common.Parameter{
		LabelZH: "策略名称",
		LabelEN: "Name",
	})
	params.MustRegister(policy, "Volumns", &common.Parameter{
		LabelZH: "卷",
		LabelEN: "Volumns",
	})
	params.MustRegister(policy, "MoveFactor", &common.Parameter{
		LabelZH:       "空闲占比阈值",
		DescriptionZH: "当一个volume空闲空间占比小于此值时，移动部分parts到下一个volume",
		Range:         &common.Range{Min: 0.0, Max: 1.0, Step: 0.1},
	})

	var vol model.Volumn
	params.MustRegister(vol, "Name", &common.Parameter{
		LabelZH: "卷名称",
		LabelEN: "Name",
	})
	params.MustRegister(vol, "Disks", &common.Parameter{
		LabelZH: "磁盘",
		LabelEN: "Disks",
	})
	params.MustRegister(vol, "MaxDataPartSizeBytes", &common.Parameter{
		LabelZH: "MaxDataPartSizeBytes",
		LabelEN: "MaxDataPartSizeBytes",
	})

	var mergetree model.MergeTreeConf
	params.MustRegister(mergetree, "Expert", &common.Parameter{
		LabelZH:       "专家模式",
		LabelEN:       "Expert",
		DescriptionZH: "自定义配置merge_tree的配置项，生成在config.d/merge_tree.xml中, 请参考: https://clickhouse.tech/docs/en/operations/settings/merge-tree-settings/",
		DescriptionEN: "define the configuration items for configuring merge_tree, generated in config.d/merge_tree.xml, please visit: https://clickhouse.tech/docs/en/operations/settings/merge-tree-settings/",
		Required:      "false",
	})

	var userconf model.UsersConf
	params.MustRegister(userconf, "Users", &common.Parameter{
		LabelZH:       "用户",
		LabelEN:       "Users",
		DescriptionZH: "普通用户的管理",
		DescriptionEN: "normal user config management",
		Required:      "false",
	})

	var user model.User
	params.MustRegister(user, "Name", &common.Parameter{
		LabelZH:       "用户名",
		LabelEN:       "Name",
		DescriptionZH: "用户名称，不可以是已经存在的或default",
		DescriptionEN: "username, can't be duplicate or default",
	})
	params.MustRegister(user, "Password", &common.Parameter{
		LabelZH:       "密码",
		LabelEN:       "Password",
		DescriptionZH: "用户密码，不可为空",
		DescriptionEN: "can't be empty",
		InputType:     common.InputPassword,
	})

	return params
}

func RegistUpdateConfigSchema() common.ConfigParams {
	var params common.ConfigParams = make(map[string]*common.Parameter)
	var conf model.CKManClickHouseConfig
	params.MustRegister(conf, "SshUser", &common.Parameter{
		LabelZH:       "系统账户名",
		LabelEN:       "SSH Username",
		DescriptionZH: "必须有root或者sudo权限",
		DescriptionEN: "must have permission with root or sudo",
	})
	params.MustRegister(conf, "AuthenticateType", &common.Parameter{
		LabelZH:       "认证方式",
		LabelEN:       "Authenticate Type",
		DescriptionZH: "SSH 访问节点的方式，可使用公钥或者密码，使用公钥时需将公钥文件放到conf目录下",
		DescriptionEN: "Authenticate type of connect node, you need copy id_rsa to conf/ if use public key",
		Candidates: []common.Candidate{
			{Value: "0", LabelEN: "Password(save)", LabelZH: "密码认证(保存密码)",},
			{Value: "1", LabelEN: "Password(not save)", LabelZH: "密码认证(不保存密码)",},
			{Value: "2", LabelEN: "Public Key", LabelZH: "公钥认证",},
		},
	})
	params.MustRegister(conf, "SshPassword", &common.Parameter{
		LabelZH:       "系统账户密码",
		LabelEN:       "SSH Password",
		DescriptionZH: "不得为空",
		DescriptionEN: "can't be empty",
		Visiable:      "AuthenticateType != '2'",
		InputType:     common.InputPassword,
	})
	params.MustRegister(conf, "SshPort", &common.Parameter{
		LabelZH:       "SSH 端口",
		LabelEN:       "SSH Port",
		DescriptionZH: "不得为空",
	})
	params.MustRegister(conf, "ZkPort", &common.Parameter{
		LabelZH: "ZooKeeper集群监听端口",
		LabelEN: "Zookeeper Port",
		Default: "2181",
	})
	params.MustRegister(conf, "ZkStatusPort", &common.Parameter{
		LabelZH:       "Zookeeper监控端口",
		LabelEN:       "Zookeeper Status Port",
		DescriptionZH: "暴露给mntr等四字命令的端口，zookeeper 3.5.0 以上支持",
		DescriptionEN: "expose to commands/mntr, zookeeper support it after 3.5.0",
		Default:       "8080",
	})
	params.MustRegister(conf, "PromHost", &common.Parameter{
		LabelZH:  "Promethues 地址",
		LabelEN:  "Prometheus Host",
		Default:  "127.0.0.1",
		Required: "false",
	})
	params.MustRegister(conf, "PromPort", &common.Parameter{
		LabelZH:  "Promethues 端口",
		LabelEN:  "Prometheus Port",
		Default:  "9090",
		Required: "false",
	})
	params.MustRegister(conf, "Password", &common.Parameter{
		LabelZH:   "默认用户密码",
		LabelEN:   "Default Password",
		InputType: common.InputPassword,
	})
	params.MustRegister(conf, "Port", &common.Parameter{
		LabelZH: "TCP端口",
		LabelEN: "TCPPort",
	})
	params.MustRegister(conf, "Storage", &common.Parameter{
		LabelZH:       "集群存储配置",
		LabelEN:       "Storage Policy",
		DescriptionZH: "由disks, policies两部分构成。policies提到的disk名必须在disks中定义。ClickHouse内置了名为default的policy和disk。",
		DescriptionEN: "Composed of Disks, Policies. The Disk name mentioned by Policies must be defined in Disks. Clickhouse has built-in Policy and Disk named Default. ",
	})
	params.MustRegister(conf, "MergeTreeConf", &common.Parameter{
		LabelZH:  "MergeTree配置",
		LabelEN:  "MergeTree Config",
		Required: "false",
	})
	params.MustRegister(conf, "UsersConf", &common.Parameter{
		LabelZH:  "用户管理配置",
		LabelEN:  "User Config",
		Required: "false",
	})

	var storage model.Storage
	params.MustRegister(storage, "Disks", &common.Parameter{
		LabelZH:       "硬盘列表",
		LabelEN:       "Disk List",
		DescriptionZH: "定义的disks，如果磁盘中有数据，则不允许删除该磁盘",
		DescriptionEN: "defined Disks, it's not allow to detele disk which have data yet",
		Required:      "false",
	})
	params.MustRegister(storage, "Policies", &common.Parameter{
		LabelZH:  "存储策略列表",
		LabelEN:  "Policies List",
		Required: "false",
	})

	var disk model.Disk
	params.MustRegister(disk, "Name", &common.Parameter{
		LabelZH: "磁盘名称",
		LabelEN: "Name",
	})
	params.MustRegister(disk, "Type", &common.Parameter{
		LabelZH: "硬盘类型",
		LabelEN: "Disk Type",
		Default: "local",
		Candidates: []common.Candidate{
			{Value: "local", LabelEN: "Local", LabelZH: "本地磁盘"},
			{Value: "s3", LabelEN: "AWS S3", LabelZH: "AWS S3"},
			{Value: "hdfs", LabelEN: "HDFS", LabelZH: "HDFS"},
		},
	})
	params.MustRegister(disk, "DiskLocal", &common.Parameter{
		LabelZH:  "本地硬盘",
		LabelEN:  "Local",
		Visiable: "Type == 'local'",
	})
	params.MustRegister(disk, "DiskS3", &common.Parameter{
		LabelZH:  "AWS S3",
		LabelEN:  "AWS S3",
		Visiable: "Type == 's3'",
	})
	params.MustRegister(disk, "DiskHdfs", &common.Parameter{
		LabelZH:  "HDFS",
		LabelEN:  "HDFS",
		Visiable: "Type == 'hdfs'",
	})

	var disklocal model.DiskLocal
	params.MustRegister(disklocal, "Path", &common.Parameter{
		LabelZH:       "挂载路径",
		LabelEN:       "Amount Path",
		DescriptionZH: "必须存在，clickhouse用户可访问， 且必须以'/'开头和结尾",
		DescriptionEN: "need exist, can be accessed by clickhouse, and must begin and end with '/'",
		Regexp:        "^/.+/$",
	})
	params.MustRegister(disklocal, "KeepFreeSpaceBytes", &common.Parameter{
		LabelZH: "保留空闲空间大小",
		LabelEN: "KeepFreeSpaceBytes",
	})

	var disks3 model.DiskS3
	params.MustRegister(disks3, "Endpoint", &common.Parameter{
		LabelZH: "S3端点URI",
		LabelEN: "Endpoint",
		Regexp:  "^(http|https)://.+/$",
	})

	params.MustRegister(disks3, "AccessKeyID", &common.Parameter{
		LabelZH: "AccessKeyID",
		LabelEN: "AccessKeyID",
	})
	params.MustRegister(disks3, "SecretAccessKey", &common.Parameter{
		LabelZH: "SecretAccessKey",
		LabelEN: "SecretAccessKey",
	})
	params.MustRegister(disks3, "Region", &common.Parameter{
		LabelZH: "Region",
		LabelEN: "Region",
	})
	params.MustRegister(disks3, "UseEnvironmentCredentials", &common.Parameter{
		LabelZH: "UseEnvironmentCredentials",
		LabelEN: "UseEnvironmentCredentials",
	})
	params.MustRegister(disks3, "Expert", &common.Parameter{
		LabelZH:       "专家模式",
		LabelEN:       "Expert Mode",
		DescriptionZH: "专家模式的S3参数, 请参考: https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-s3",
		DescriptionEN: "configure S3 params by yourself, please visit: https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-s3",
	})

	var diskhdfs model.DiskHdfs
	params.MustRegister(diskhdfs, "Endpoint", &common.Parameter{
		LabelZH: "HDFS端点URI",
		LabelEN: "Endpoint",
		Regexp:  "^hdfs://.+/$",
	})

	var policy model.Policy
	params.MustRegister(policy, "Name", &common.Parameter{
		LabelZH: "策略名称",
		LabelEN: "Name",
	})
	params.MustRegister(policy, "Volumns", &common.Parameter{
		LabelZH: "卷",
		LabelEN: "Volumns",
	})
	params.MustRegister(policy, "MoveFactor", &common.Parameter{
		LabelZH:       "空闲占比阈值",
		DescriptionZH: "当一个volume空闲空间占比小于此值时，移动部分parts到下一个volume",
		Range:         &common.Range{Min: 0.0, Max: 1.0, Step: 0.1},
	})

	var vol model.Volumn
	params.MustRegister(vol, "Name", &common.Parameter{
		LabelZH: "卷名称",
		LabelEN: "Name",
	})
	params.MustRegister(vol, "Disks", &common.Parameter{
		LabelZH: "磁盘",
		LabelEN: "Disks",
	})
	params.MustRegister(vol, "MaxDataPartSizeBytes", &common.Parameter{
		LabelZH: "MaxDataPartSizeBytes",
		LabelEN: "MaxDataPartSizeBytes",
	})

	var mergetree model.MergeTreeConf
	params.MustRegister(mergetree, "Expert", &common.Parameter{
		LabelZH:       "专家模式",
		LabelEN:       "Expert",
		DescriptionZH: "自定义配置merge_tree的配置项，生成在config.d/merge_tree.xml中， 请参考: https://clickhouse.tech/docs/en/operations/settings/merge-tree-settings/",
		DescriptionEN: "define the configuration items for configuring merge_tree, generated in config.d/merge_tree.xml, please visit: https://clickhouse.tech/docs/en/operations/settings/merge-tree-settings/",
		Required:      "false",
	})

	var userconf model.UsersConf
	params.MustRegister(userconf, "Users", &common.Parameter{
		LabelZH:       "用户",
		LabelEN:       "Users",
		DescriptionZH: "普通用户的管理",
		DescriptionEN: "normal user config management",
		Required:      "false",
	})

	var user model.User
	params.MustRegister(user, "Name", &common.Parameter{
		LabelZH:       "用户名",
		LabelEN:       "Name",
		DescriptionZH: "用户名称，不可以是已经存在的或default",
		DescriptionEN: "username, can't be duplicate or default",
	})
	params.MustRegister(user, "Password", &common.Parameter{
		LabelZH:       "密码",
		LabelEN:       "Password",
		DescriptionZH: "用户密码，不可为空",
		DescriptionEN: "can't be empty",
		InputType:     common.InputPassword,
	})

	return params
}

func (ui *SchemaUIController) RegistSchemaInstance() {
	SchemaUIMapping = make(map[string]common.ConfigParams)
	for k, v := range schemaHandleFunc {
		SchemaUIMapping[k] = v()
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
	var conf model.CKManClickHouseConfig
	typo := strings.ToLower(Type)
	params := GetSchemaParams(typo, conf)
	if params == nil {
		model.WrapMsg(c, model.GET_SCHEMA_UI_FAILED, errors.Errorf("type %s is not regist", typo))
		return
	}
	schema, err := params.MarshalSchema(conf)
	if err != nil {
		model.WrapMsg(c, model.GET_SCHEMA_UI_FAILED, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, schema)
}

func GetSchemaParams(typo string, conf model.CKManClickHouseConfig) common.ConfigParams {
	params, ok := SchemaUIMapping[typo]
	if !ok {
		return nil
	}

	if typo == GET_SCHEMA_UI_DEPLOY {
		// get version list every time
		params.MustRegister(conf, "Version", &common.Parameter{
			LabelZH:       "ClickHouse版本",
			LabelEN:       "Package Version",
			DescriptionZH: "需要部署的ClickHouse集群的版本号，需提前上传安装包",
			DescriptionEN: "which version of clickhouse will deployed, need upload rpm package before",
			Candidates:    getVersionLists(),
		})
	}
	return params
}

func DecodeRequestBody(request *http.Request, conf *model.CKManClickHouseConfig, typo string) error {
	params := GetSchemaParams(typo, *conf)
	if params == nil {
		return errors.Errorf("type %s is not registered", typo)
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return err
	}
	err = params.UnmarshalConfig(string(body), conf)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(*conf, "", "  ")
	if err != nil {
		return err
	}
	log.Logger.Debugf("[request] | %s | %s | %s \n%v ", request.Host, request.Method, request.URL, string(data))
	return nil
}
