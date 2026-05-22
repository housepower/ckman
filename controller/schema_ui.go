package controller

import (
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

const (
	GET_SCHEMA_UI_DEPLOY = "deploy"
	GET_SCHEMA_UI_CONFIG = "config"
)

var SchemaUIMapping map[string]common.ConfigParams

type SchemaUIController struct {
	Controller
}

var schemaHandleFunc = map[string]func() common.ConfigParams{
	GET_SCHEMA_UI_DEPLOY: RegistCreateClusterSchema,
	GET_SCHEMA_UI_CONFIG: RegistUpdateConfigSchema,
}

func getPkgType() []common.Candidate {
	pkgs := common.GetAllPackages()
	var lists []common.Candidate
	for pkgType := range pkgs {
		can := common.Candidate{
			Value: pkgType,
		}
		lists = append(lists, can)
	}
	return lists
}

func getPkgLists() []common.Candidate {
	packages := common.GetAllPackages()
	var pkgLists []common.Candidate
	for _, pkgs := range packages {
		for _, pkg := range pkgs {
			can := common.Candidate{
				Value: pkg.PkgName,
			}
			pkgLists = append(pkgLists, can)
		}
	}
	return pkgLists
}

func NewSchemaUIController(wrapfunc Wrapfunc) *SchemaUIController {
	return &SchemaUIController{
		Controller: Controller{
			wrapfunc: wrapfunc,
		},
	}
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
	params.MustRegister(conf, "Comment", &common.Parameter{
		LabelZH:  "备注",
		LabelEN:  "Comment",
		Required: "false",
	})
	params.MustRegister(conf, "LogicCluster", &common.Parameter{
		LabelZH:       "逻辑集群名",
		LabelEN:       "Logic Name",
		DescriptionZH: "逻辑集群，存在于物理集群之上",
		DescriptionEN: "require physical cluster",
	})
	params.MustRegister(conf, "Cwd", &common.Parameter{
		LabelZH:       "工作路径",
		LabelEN:       "WorkingDirectory",
		DescriptionZH: "tgz部署的工作目录，存放二进制/配置/日志/pid。需以'/'结尾。卸载时仅清空其下的 bin/etc/log/run 四个子目录；数据存储路径(Path)与Keeper数据路径不可落在这四个子目录中，但允许放在工作目录下其他位置（如 ${Cwd}data/）",
		DescriptionEN: "Working directory for tgz deployment (binaries/configs/logs/pid). Must end with '/'. Uninstall only removes the bin/etc/log/run subdirectories; Data Path and Keeper Path must not live under any of those four, but are allowed elsewhere under Cwd (e.g. ${Cwd}data/).",
		Visiable:      "PkgType.indexOf('tgz') !== -1",
		Regexp:        "^/.+/$",
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
			{Value: "0", LabelEN: "Password(save)", LabelZH: "密码认证(保存密码)"},
			{Value: "1", LabelEN: "Password(not save)", LabelZH: "密码认证(不保存密码)"},
			{Value: "2", LabelEN: "Public Key", LabelZH: "公钥认证"},
		},
		Default: "0",
	})
	params.MustRegister(conf, "SshPassword", &common.Parameter{
		LabelZH:       "系统账户密码",
		LabelEN:       "SSH Password",
		DescriptionZH: "不得为空",
		DescriptionEN: "can't be empty",
		Visiable:      "AuthenticateType != '2'",
		InputType:     common.InputPassword,
		Required:      "false",
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
	params.MustRegister(conf, "EncryptType", &common.Parameter{
		LabelZH:       "密码加密算法",
		LabelEN:       "EncryptType",
		DescriptionZH: "密码保存时使用什么加密方式，默认明文",
		DescriptionEN: "What encryption method is used when the password is saved, the default is plaintext",
		Candidates: []common.Candidate{
			{Value: "0", LabelEN: "PLAINTEXT", LabelZH: "PLAINTEXT"},
			{Value: "1", LabelEN: "SHA256_HEX", LabelZH: "SHA256_HEX"},
			{Value: "2", LabelEN: "DOUBLE_SHA1_HEX", LabelZH: "DOUBLE_SHA1_HEX"},
		},
		Default: "0",
	})
	params.MustRegister(conf, "Shards", &common.Parameter{
		LabelZH:       "集群节点配置",
		LabelEN:       "ClickHouse Cluster Node",
		DescriptionEN: "shard of clickhouse cluster",
		DescriptionZH: "集群分片信息",
	})

	var shard model.CkShard
	params.MustRegister(shard, "Replicas", &common.Parameter{
		LabelZH:       "分片",
		LabelEN:       "Shard",
		DescriptionEN: "replicas of each shard",
		DescriptionZH: "集群每个分片的副本信息",
	})
	var replica model.CkReplica
	params.MustRegister(replica, "Ip", &common.Parameter{
		LabelZH: "副本节点IP",
		LabelEN: "Replica Node IP",
	})
	params.MustRegister(conf, "Protocol", &common.Parameter{
		LabelZH: "连接协议",
		LabelEN: "Protocol",
		Default: "native",
		Candidates: []common.Candidate{
			{Value: "native", LabelEN: "native", LabelZH: "native"},
			{Value: "http", LabelEN: "http", LabelZH: "http"},
		},
	})
	params.MustRegister(conf, "Port", &common.Parameter{
		LabelZH: "TCP端口",
		LabelEN: "TCPPort",
		Default: "9000",
	})
	params.MustRegister(conf, "HttpPort", &common.Parameter{
		LabelZH: "HTTP端口",
		LabelEN: "HTTP Port",
		Default: "8123",
	})
	params.MustRegister(conf, "Secure", &common.Parameter{
		LabelZH:  "TLS安全认证",
		LabelEN:  "TLS Secure",
		Default:  "false",
		Required: "false",
		Editable: "false",
	})

	params.MustRegister(conf, "Keeper", &common.Parameter{
		Default:       "zookeeper",
		DescriptionZH: "如果使用clickhouse-keeper， 则默认由ckman托管；如果使用已有zookeeper或已经创建好的keeper集群，都视同zookeeper",
		Candidates: []common.Candidate{
			{Value: model.Zookeeper, LabelEN: "Zookeeper", LabelZH: "Zookeeper"},
			{Value: model.ClickhouseKeeper, LabelEN: "ClickHouse-Keeper", LabelZH: "ClickHouse-Keeper"},
		},
	})

	params.MustRegister(conf, "KeeperConf", &common.Parameter{
		LabelZH:       "Keeper配置",
		LabelEN:       "KeeperConf",
		DescriptionZH: "clickhouse-keeper的配置项",
		Visiable:      "Keeper == 'clickhouse-keeper'",
	})

	var keeper model.KeeperConf
	params.MustRegister(keeper, "Runtime", &common.Parameter{
		LabelZH:       "运行方式",
		LabelEN:       "Runtime",
		Default:       model.KeeperRuntimeStandalone,
		DescriptionZH: "如果单独部署，则和clickhouse-server 分开进程；如果内置，则和clickhouse-server放在一块",
		Candidates: []common.Candidate{
			{Value: model.KeeperRuntimeStandalone, LabelEN: "Standalone", LabelZH: "单独部署"},
			{Value: model.KeeperRuntimeInternal, LabelEN: "Internal", LabelZH: "内置"},
		},
	})
	params.MustRegister(keeper, "KeeperNodes", &common.Parameter{
		LabelZH:  "Keeper节点",
		LabelEN:  "KeeperNodes",
		Visiable: "Runtime == 'standalone'",
	})

	params.MustRegister(keeper, "TcpPort", &common.Parameter{
		LabelZH: "Keeper端口",
		LabelEN: "TcpPort",
		Default: "9181",
	})
	params.MustRegister(keeper, "RaftPort", &common.Parameter{
		LabelZH: "Raft通信端口",
		LabelEN: "RaftPort",
		Default: "9234",
	})
	params.MustRegister(keeper, "Path", &common.Parameter{
		LabelZH:       "Keeper数据路径",
		LabelEN:       "KeeperPath",
		DescriptionZH: "Keeper存储数据的路径，需存在且以'/'结尾。tgz部署时可以放在工作路径(Cwd)同级或其下子目录，但不得落在 ${Cwd}{bin,etc,log,run}/ 中——这四个子目录会在卸载时被清空",
		DescriptionEN: "Data path for Keeper, must exist and end with '/'. For tgz deployment, may live alongside or beneath Cwd; must NOT fall under ${Cwd}{bin,etc,log,run}/ which are wiped on uninstall.",
		Default:       "/var/lib/",
		Regexp:        "^/.+/$",
	})
	params.MustRegister(keeper, "Expert", &common.Parameter{
		LabelZH:  "专家配置",
		LabelEN:  "Expert",
		Required: "false",
	})
	params.MustRegister(keeper, "Coordination", &common.Parameter{
		LabelZH:  "协作配置",
		LabelEN:  "Coordination",
		Required: "false",
	})

	var coordination model.Coordination
	params.MustRegister(coordination, "OperationTimeoutMs", &common.Parameter{
		LabelZH:  "OperationTimeoutMs",
		LabelEN:  "OperationTimeoutMs",
		Default:  "10000",
		Required: "false",
	})
	params.MustRegister(coordination, "SessionTimeoutMs", &common.Parameter{
		LabelZH:  "SessionTimeoutMs",
		LabelEN:  "SessionTimeoutMs",
		Default:  "30000",
		Required: "false",
	})
	params.MustRegister(coordination, "ForceSync", &common.Parameter{
		LabelZH:  "ForceSync",
		LabelEN:  "ForceSync",
		Required: "false",
	})
	// params.MustRegister(coordination, "AutoForwarding", &common.Parameter{
	// 	LabelZH:  "AutoForwarding",
	// 	LabelEN:  "AutoForwarding",
	// 	Required: "false",
	// 	Default:  "true",
	// })
	params.MustRegister(coordination, "Expert", &common.Parameter{
		LabelZH:  "专家配置",
		LabelEN:  "Expert",
		Required: "false",
	})

	params.MustRegister(conf, "ZkNodes", &common.Parameter{
		LabelZH:       "ZooKeeper集群结点列表",
		LabelEN:       "Zookeeper Node List",
		DescriptionZH: "每段为单个IP，或者IP范围，或者网段掩码",
		DescriptionEN: "Zookeeper Node ip, support CIDR or Range.",
		Visiable:      "Keeper == 'zookeeper'",
	})
	params.MustRegister(conf, "ZkPort", &common.Parameter{
		LabelZH:  "ZooKeeper集群监听端口",
		LabelEN:  "Zookeeper Port",
		Default:  "2181",
		Visiable: "Keeper == 'zookeeper'",
	})
	// params.MustRegister(conf, "PromHost", &common.Parameter{
	// 	LabelZH:  "Promethues 地址",
	// 	LabelEN:  "Prometheus Host",
	// 	Default:  "127.0.0.1",
	// 	Required: "false",
	// })
	// params.MustRegister(conf, "PromPort", &common.Parameter{
	// 	LabelZH:  "Promethues 端口",
	// 	LabelEN:  "Prometheus Port",
	// 	Default:  "9090",
	// 	Required: "false",
	// })
	// params.MustRegister(conf, "PromMetricPort", &common.Parameter{
	// 	LabelZH:  "Promethues指标端口",
	// 	LabelEN:  "Prometheus Metrics Port",
	// 	Required: "false",
	// })

	// var promPort model.PromMetricPort
	// params.MustRegister(promPort, "ClickHouse", &common.Parameter{
	// 	LabelZH:       "ClickHouse指标端口",
	// 	LabelEN:       "ClickHouse Metrics Port",
	// 	Default:       "9363",
	// 	Required:      "false",
	// 	DescriptionEN: "Prometheus port export by ClickHouse",
	// 	DescriptionZH: "clickhouse暴露的prometheus端口",
	// })
	// params.MustRegister(promPort, "ZooKeeper", &common.Parameter{
	// 	LabelZH:       "ZooKeeper指标端口",
	// 	LabelEN:       "Zookeeper Metrics Port",
	// 	Default:       "7000",
	// 	Required:      "false",
	// 	DescriptionEN: "Prometheus port export by Zookeeper",
	// 	DescriptionZH: "zookeeper暴露的prometheus端口",
	// })
	// params.MustRegister(promPort, "NodeExport", &common.Parameter{
	// 	LabelZH:       "Node Exporter端口",
	// 	LabelEN:       "Node Exporter Port",
	// 	Default:       "9100",
	// 	Required:      "false",
	// 	DescriptionEN: "Prometheus port export by NodeExporter",
	// 	DescriptionZH: "NodeExporter暴露的prometheus端口",
	// })

	params.MustRegister(conf, "Path", &common.Parameter{
		LabelZH:       "数据存储路径",
		LabelEN:       "Data Path",
		DescriptionZH: "ClickHouse存储数据的路径，需存在且以'/'结尾。tgz部署时可以放在工作路径(Cwd)同级或其下子目录（如 ${Cwd}data/），但不得落在 ${Cwd}{bin,etc,log,run}/ 中——这四个子目录会在卸载时被清空",
		Default:       "/var/lib/",
		DescriptionEN: "Data path for ClickHouse, must exist and end with '/'. For tgz deployment, may live alongside or beneath Cwd (e.g. ${Cwd}data/); must NOT fall under ${Cwd}{bin,etc,log,run}/ which are wiped on uninstall.",
		Regexp:        "^/.+/$",
	})
	params.MustRegister(conf, "Storage", &common.Parameter{
		LabelZH:       "集群存储配置",
		LabelEN:       "Storage Policy",
		DescriptionZH: "由disks, policies两部分构成。policies提到的disk名必须在disks中定义。ClickHouse内置了名为default的policy和disk。",
		DescriptionEN: "Composed of Disks, Policies. The Disk name mentioned by Policies must be defined in Disks. Clickhouse has built-in Policy and Disk named Default. ",
	})
	params.MustRegister(conf, "Expert", &common.Parameter{
		LabelZH: "自定义配置项",
		LabelEN: "Custom Config",
		DescriptionZH: `自定义配置文件，语法接近xpath(https://www.w3schools.com/xml/xpath_syntax.asp);
举例：title[@lang='en', @size=4]/header:header123， 最终生成的配置为:
<title lang="en" size="4">
    <header>header123</header>
</title>
非专业人士请勿填写此项`,
		DescriptionEN: `Custom configuration items, similar to xpath syntax(https://www.w3schools.com/xml/xpath_syntax.asp);
For example: title[@lang='en', @size=4]/header:header123, the final generated configuration is:
<title lang="en" size="4">
    <header>header123</header>
</title>
Non-professionals please do not fill in this`,
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
			{Value: "cache", LabelEN: "Cache", LabelZH: "缓存"},
			//{Value: "hdfs", LabelEN: "HDFS", LabelZH: "HDFS"},
		},
	})
	params.MustRegister(disk, "AllowedBackup", &common.Parameter{
		LabelZH:       "允许备份",
		LabelEN:       "AllowedBackup",
		Required:      "false",
		DescriptionZH: "是否允许备份数据到该磁盘",
		DescriptionEN: "Whether to allow backup data to the disk",
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
	params.MustRegister(disk, "DiskCache", &common.Parameter{
		LabelZH:  "磁盘缓存",
		LabelEN:  "Disk Cache",
		Visiable: "Type == 'cache'",
	})

	var diskcache model.DiskCache
	params.MustRegister(diskcache, "Path", &common.Parameter{
		LabelZH:       "缓存路径",
		LabelEN:       "Cache Path",
		DescriptionZH: "缓存数据存储的路径，必须以'/'开头和结尾",
		DescriptionEN: "Path for cached data, must begin and end with '/'",
		Regexp:        "^/.+/$",
		Required:      "true",
	})
	params.MustRegister(diskcache, "MaxSize", &common.Parameter{
		LabelZH:       "最大缓存大小（字节）",
		LabelEN:       "Max Size",
		DescriptionZH: "缓存的最大大小（字节）, 默认大小为10G",
		DescriptionEN: "Maximum size of cache in bytes， default is 10G",
		Required:      "true",
		Default:       "10737418240",
	})
	params.MustRegister(diskcache, "Disk", &common.Parameter{
		LabelZH:       "底层磁盘",
		LabelEN:       "Backend Disk",
		DescriptionZH: "用于存储缓存数据的底层磁盘名称",
		DescriptionEN: "Name of the backend disk for storing cached data",
		Required:      "true",
	})
	params.MustRegister(diskcache, "CacheOnWriteOperations", &common.Parameter{
		LabelZH:       "写操作时缓存",
		LabelEN:       "Cache On Write Operations",
		DescriptionZH: "在写入操作时是否启用缓存， 如果不是直写底层磁盘，不建议打开",
		DescriptionEN: "Enable caching on write operations",
		Default:       "false",
		Required:      "false",
	})
	params.MustRegister(diskcache, "CachePolicy", &common.Parameter{
		LabelZH:       "缓存策略",
		LabelEN:       "Cache Policy",
		DescriptionZH: "缓存淘汰策略: SLRU 或 LRU",
		DescriptionEN: "Cache eviction policy: SLRU or LRU",
		Candidates: []common.Candidate{
			{Value: "SLRU", LabelEN: "SLRU", LabelZH: "SLRU"},
			{Value: "LRU", LabelEN: "LRU", LabelZH: "LRU"},
		},
		Default:  "SLRU",
		Required: "true",
	})
	params.MustRegister(diskcache, "SLRUSizeRatio", &common.Parameter{
		LabelZH:       "SLRU大小比例",
		LabelEN:       "SLRU Size Ratio",
		DescriptionZH: "SLRU 缓存中试用部分的比例",
		DescriptionEN: "Ratio of probationary part in SLRU cache",
		Range:         &common.Range{Min: 0.0, Max: 1.0, Step: 0.1},
		Required:      "false",
		Visiable:      "CachePolicy == 'SLRU'",
		Default:       "0.6",
	})
	params.MustRegister(diskcache, "Expert", &common.Parameter{
		LabelZH:       "专家配置",
		LabelEN:       "Expert",
		DescriptionZH: "缓存高级配置，参考: https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCacheSettings.cpp",
		DescriptionEN: "Advanced cache configuration, refer to: https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCacheSettings.cpp",
		Required:      "false",
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
		LabelZH: "保留空闲空间大小（字节）",
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
		Required:      "false",
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

	var userconf model.UsersConf
	params.MustRegister(userconf, "Users", &common.Parameter{
		LabelZH:       "用户",
		LabelEN:       "Users",
		DescriptionZH: "普通用户的管理",
		DescriptionEN: "normal user config management",
		Required:      "false",
	})
	params.MustRegister(userconf, "Profiles", &common.Parameter{
		LabelZH:  "配置管理",
		LabelEN:  "Profiles",
		Required: "false",
	})
	params.MustRegister(userconf, "Quotas", &common.Parameter{
		LabelZH:  "配额管理",
		LabelEN:  "Quotas",
		Required: "false",
	})
	params.MustRegister(userconf, "Roles", &common.Parameter{
		LabelZH:  "角色管理",
		LabelEN:  "Roles",
		Required: "false",
	})
	params.MustRegister(userconf, "Expert", &common.Parameter{
		LabelZH: "用户高级配置",
		LabelEN: "User Custom Config",
		DescriptionZH: `自定义配置文件，语法接近xpath(https://www.w3schools.com/xml/xpath_syntax.asp);
举例：title[@lang='en', @size=4]/header:header123， 最终生成的配置为:
<title lang="en" size="4">
    <header>header123</header>
</title>
非专业人士请勿填写此项`,
		DescriptionEN: `Custom configuration items, similar to xpath syntax(https://www.w3schools.com/xml/xpath_syntax.asp);
For example: title[@lang='en', @size=4]/header:header123, the final generated configuration is:
<title lang="en" size="4">
    <header>header123</header>
</title>
Non-professionals please do not fill in this`,
		Required: "false",
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
	params.MustRegister(user, "EncryptType", &common.Parameter{
		LabelZH:       "密码加密算法",
		LabelEN:       "EncryptType",
		DescriptionZH: "密码保存时使用什么加密方式，默认明文",
		DescriptionEN: "What encryption method is used when the password is saved, the default is plaintext",
		Candidates: []common.Candidate{
			{Value: "0", LabelEN: "PLAINTEXT", LabelZH: "PLAINTEXT"},
			{Value: "1", LabelEN: "SHA256_HEX", LabelZH: "SHA256_HEX"},
			{Value: "2", LabelEN: "DOUBLE_SHA1_HEX", LabelZH: "DOUBLE_SHA1_HEX"},
		},
		Default: "0",
	})
	params.MustRegister(user, "Profile", &common.Parameter{
		LabelZH:       "限额",
		LabelEN:       "Profile",
		DescriptionZH: "设置用户相关参数，如：最大内存使用、只读等",
		DescriptionEN: "Set user-related parameters, such as: maximum memory usage, read-only, etc",
		Required:      "false",
	})
	params.MustRegister(user, "Quota", &common.Parameter{
		LabelZH:       "配额",
		LabelEN:       "Quota",
		DescriptionZH: "配额允许您在一段时间内跟踪或限制资源使用情况",
		DescriptionEN: "Quotas allow you to track or limit resource usage over a period of time. ",
		Required:      "false",
	})
	params.MustRegister(user, "Roles", &common.Parameter{
		LabelZH:       "角色",
		LabelEN:       "Roles",
		DescriptionZH: "角色可以通过SQL的方式定义某类用户对数据库的访问权限",
		DescriptionEN: "Roles can define the access rights of certain types of users to the database by means of SQL.",
		Required:      "false",
	})
	params.MustRegister(user, "Networks", &common.Parameter{
		LabelZH:       "允许登录地址",
		LabelEN:       "NetWorks",
		DescriptionZH: "用户可以连接到 ClickHouse 服务器的网络列表。",
		DescriptionEN: "List of networks from which the user can connect to the ClickHouse server.",
		Required:      "false",
	})
	params.MustRegister(user, "DbRowPolices", &common.Parameter{
		LabelZH:       "访问权限",
		LabelEN:       "DbRowPolices",
		DescriptionZH: "设置数据库及表的访问权限",
		DescriptionEN: "Set database and table access permissions",
		Required:      "false",
	})

	var dbRow model.DbRowPolicy
	params.MustRegister(dbRow, "Database", &common.Parameter{
		LabelZH:       "数据库",
		LabelEN:       "Database",
		DescriptionZH: "用户只能访问的数据库",
		DescriptionEN: "Databases that users can only access",
	})
	params.MustRegister(dbRow, "TblRowPolicies", &common.Parameter{
		LabelZH:       "行访问权限",
		LabelEN:       "TblRowPolicies",
		DescriptionZH: "用户只能访问数据库的哪些行",
		DescriptionEN: "Which rows of the database the user can only access",
		Required:      "false",
	})

	var networks model.Networks
	params.MustRegister(networks, "IPs", &common.Parameter{
		LabelZH:       "IP列表",
		LabelEN:       "IPs",
		DescriptionZH: "用户能访问的数据库表",
		DescriptionEN: "IP address or network mask",
		Required:      "false",
	})
	params.MustRegister(networks, "Hosts", &common.Parameter{
		LabelZH:       "主机列表",
		LabelEN:       "Hosts",
		DescriptionZH: "用户能访问的数据库表",
		DescriptionEN: "o check access, a DNS query is performed, and all returned IP addresses are compared to the peer address.",
		Required:      "false",
	})
	params.MustRegister(networks, "HostRegexps", &common.Parameter{
		LabelZH:       "主机名正则匹配",
		LabelEN:       "HostRegexps",
		DescriptionZH: "主机名正则表达式匹配",
		DescriptionEN: "Regular expression for hostnames",
		Required:      "false",
	})

	var tblRow model.TblRowPolicy
	params.MustRegister(tblRow, "Table", &common.Parameter{
		LabelZH:       "表名",
		LabelEN:       "Table",
		DescriptionZH: "用户能访问的数据库表",
		DescriptionEN: "Which table the user can only access",
	})
	params.MustRegister(tblRow, "Filter", &common.Parameter{
		LabelZH:       "过滤器",
		LabelEN:       "Filter",
		DescriptionZH: "过滤器可以是任何产生 UInt8 类型值的表达式。它通常包含比较和逻辑运算符。不为此用户返回从 database_name.table1 中筛选结果为 0 的行。过滤与 PREWHERE 操作不兼容，并禁用 WHERE→PREWHERE 优化。",
		DescriptionEN: "The filter can be any expression resulting in a UInt8-type value. It usually contains comparisons and logical operators. Rows from database_name.table1 where filter results to 0 are not returned for this user. The filtering is incompatible with PREWHERE operations and disables WHERE→PREWHERE optimization.",
	})

	var profile model.Profile
	params.MustRegister(profile, "Name", &common.Parameter{
		LabelZH: "配置名称",
		LabelEN: "Name",
	})
	params.MustRegister(profile, "ReadOnly", &common.Parameter{
		LabelZH:       "只读约束",
		LabelEN:       "ReadOnly",
		DescriptionZH: "限制除 DDL 查询之外的所有类型的查询的权限。",
		DescriptionEN: "Restricts permissions for all types of queries except DDL queries.",
		Candidates: []common.Candidate{
			{Value: "0", LabelEN: "Default", LabelZH: "不限制"},
			{Value: "1", LabelEN: "Read", LabelZH: "只读权限"},
			{Value: "2", LabelEN: "Read and Set", LabelZH: "读权限和设置权限"},
		},
		Default:  "0",
		Required: "false",
	})
	params.MustRegister(profile, "AllowDDL", &common.Parameter{
		LabelZH:       "DDL权限",
		LabelEN:       "AllowDDL",
		DescriptionZH: "限制除 DDL 查询之外的所有类型的查询的权限",
		DescriptionEN: "Restricts permissions for DDL queries",
		Candidates: []common.Candidate{
			{Value: "0", LabelEN: "Not Allowed", LabelZH: "不允许 "},
			{Value: "1", LabelEN: "Allowed", LabelZH: "允许"},
		},
		Default:  "1",
		Required: "false",
	})
	params.MustRegister(profile, "MaxThreads", &common.Parameter{
		LabelZH:       "最大线程数",
		LabelEN:       "MaxThreads",
		DescriptionZH: "查询处理线程的最大数量，不包括从远程服务器检索数据的线程",
		DescriptionEN: "The maximum number of query processing threads, excluding threads for retrieving data from remote servers (see the ‘max_distributed_connections’ parameter).",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxExecutionTime", &common.Parameter{
		LabelZH:       "SQL超时时间（秒）",
		LabelEN:       "MaxExecutionTime",
		DescriptionZH: "如果查询运行时间超过指定的秒数，则行为将由 'timeout_overflow_mode' 确定，默认情况下为 - 引发异常。请注意，在数据处理过程中，将检查超时，查询只能在指定位置停止。它目前无法在聚合状态合并或查询分析期间停止，实际运行时间将高于此设置的值。",
		DescriptionEN: "If query run time exceeded the specified number of seconds, the behavior will be determined by the 'timeout_overflow_mode' which by default is - throw an exception. Note that the timeout is checked and query can stop only in designated places during data processing. It currently cannot stop during merging of aggregation states or during query analysis, and the actual run time will be higher than the value of this setting.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxMemoryPercent", &common.Parameter{
		LabelZH:       "最大使用内存（百分比）",
		LabelEN:       "MaxMemoryPercent",
		DescriptionZH: "用于在单个服务器上运行查询的最大RAM量（百分比）",
		DescriptionEN: "The maximum amount of RAM to use for running a query on a single server.",
		Range:         &common.Range{Min: 0.0, Max: 1.0},
		Default:       "0.5",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxMemoryPercentForAllQueries", &common.Parameter{
		LabelZH:       "用户查询可用最大内存（百分比）",
		LabelEN:       "MaxMemoryPercentForAllQueries",
		DescriptionZH: "用于在单个服务器上运行查询的最大RAM量（百分比）",
		DescriptionEN: "The maximum amount of RAM to use for running a query on a single server.",
		Range:         &common.Range{Min: 0.0, Max: 1.0},
		Default:       "0.75",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxBytesToRead", &common.Parameter{
		LabelZH:       "最大读取字节数（字节）",
		LabelEN:       "MaxBytesToRead",
		DescriptionZH: "在执行查询时，从表中可读取的未压缩数据的最大字节数。该限制会在每个处理的数据块上进行检查，仅适用于最深层的表表达式；在从远程服务器读取时，该限制仅在远程服务器上进行检查。",
		DescriptionEN: "The maximum number of bytes of uncompressed data that can be read from a table when executing a query. This limit is checked on each processed data block and only applies to the deepest table expression; when reading from remote servers, this limit is only checked on the remote servers.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxConcurrentQueriesForUser", &common.Parameter{
		LabelZH:       "用户最大并发查询数",
		LabelEN:       "MaxConcurrentQueriesForUser",
		DescriptionZH: "用户并发查询的最大数量。0 表示不限制。",
		DescriptionEN: "The maximum number of concurrent queries for a user. 0 means no limit.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxPartitionsToRead", &common.Parameter{
		LabelZH:       "最大读取分区数",
		LabelEN:       "MaxPartitionsToRead",
		DescriptionZH: "限制在单个查询中可访问的最大分区数量。-1 表示没有限制。",
		DescriptionEN: "Limits the maximum number of partitions that can be accessed in a single query. -1 means no limit.",
		Default:       "-1",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxResultBytes", &common.Parameter{
		LabelZH:       "最大结果字节数（字节）",
		LabelEN:       "MaxResultBytes",
		DescriptionZH: "限制结果的大小（以字节数计，未压缩）。如果达到该阈值，查询在处理完一个数据块后会停止，但不会截断结果的最后一个数据块，因此最终结果的大小可能会大于该阈值。",
		DescriptionEN: "Limits the size of the result in bytes (uncompressed). If the threshold is reached, the query stops after processing a data block, but does not truncate the last data block of the result, so the final result size may be larger than the threshold.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxResultRows", &common.Parameter{
		LabelZH:       "最大结果行数（行）",
		LabelEN:       "MaxResultRows",
		DescriptionZH: "限制结果中的行数。对子查询，以及在远程服务器上执行分布式查询的部分时也会进行检查。当值为 0 时，不施加任何限制。",
		DescriptionEN: "Limits the number of rows in the result. Also checked for subqueries and when executing distributed query parts on remote servers. When the value is 0, no limit is imposed.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxRowsToRead", &common.Parameter{
		LabelZH:       "最大读取行数",
		LabelEN:       "MaxRowsToRead",
		DescriptionZH: "单查询从表中可读取的最大行数。0 表示不限制。",
		DescriptionEN: "Maximum number of rows that can be read from a table in a single query. 0 means no limit.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxBytesBeforeExternalGroupBy", &common.Parameter{
		LabelZH:       "外部聚合阈值（字节）",
		LabelEN:       "MaxBytesBeforeExternalGroupBy",
		DescriptionZH: "GROUP BY 强制 spill 到磁盘的内存阈值（字节）。0 表示禁用外部聚合。建议设为 max_memory_usage 的一半。",
		DescriptionEN: "Memory threshold (bytes) above which GROUP BY spills to disk. 0 disables external aggregation. Recommended to set to half of max_memory_usage.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxBytesBeforeExternalSort", &common.Parameter{
		LabelZH:       "外部排序阈值（字节）",
		LabelEN:       "MaxBytesBeforeExternalSort",
		DescriptionZH: "ORDER BY 强制 spill 到磁盘的内存阈值（字节）。0 表示禁用外部排序。",
		DescriptionEN: "Memory threshold (bytes) above which ORDER BY spills to disk. 0 disables external sorting.",
		Required:      "false",
	})
	params.MustRegister(profile, "S3MaxGetRPS", &common.Parameter{
		LabelZH:       "S3 GET 限速（请求/秒）",
		LabelEN:       "S3MaxGetRPS",
		DescriptionZH: "单查询访问 S3 的最大 GET 请求速率（次/秒）。0 表示不限制。",
		DescriptionEN: "Maximum S3 GET request rate per query (requests/sec). 0 means no limit.",
		Required:      "false",
	})
	params.MustRegister(profile, "ForceIndexByDate", &common.Parameter{
		LabelZH:       "强制日期索引",
		LabelEN:       "ForceIndexByDate",
		DescriptionZH: "强制查询使用基于日期的索引，未命中时拒绝执行。0/1。",
		DescriptionEN: "Force queries to use date-based index; reject queries without one. 0/1.",
		Range:         &common.Range{Min: 0, Max: 1, Step: 1},
		Required:      "false",
	})
	params.MustRegister(profile, "ForcePrimaryKey", &common.Parameter{
		LabelZH:       "强制主键",
		LabelEN:       "ForcePrimaryKey",
		DescriptionZH: "强制查询使用主键，未命中时拒绝执行。0/1。",
		DescriptionEN: "Force queries to use primary key; reject queries without one. 0/1.",
		Range:         &common.Range{Min: 0, Max: 1, Step: 1},
		Required:      "false",
	})
	params.MustRegister(profile, "Expert", &common.Parameter{
		LabelZH:       "专家配置",
		LabelEN:       "Expert",
		DescriptionZH: "限额高级配置，参考：https://clickhouse.com/docs/en/operations/settings/settings-profiles/",
		DescriptionEN: "Advanced configuration of quota, refer to https://clickhouse.com/docs/en/operations/settings/settings-profiles/",
		Required:      "false",
	})

	var quota model.Quota
	params.MustRegister(quota, "Name", &common.Parameter{
		LabelZH: "配额名称",
		LabelEN: "Name",
	})
	params.MustRegister(quota, "Intervals", &common.Parameter{
		LabelZH:       "周期",
		LabelEN:       "Interval",
		DescriptionZH: "配额生效的周期时段",
		DescriptionEN: "Restrictions for a time period. You can set many intervals with different restrictions.",
	})

	var role model.Role
	params.MustRegister(role, "Name", &common.Parameter{
		LabelZH: "角色名称",
		LabelEN: "Name",
	})
	params.MustRegister(role, "Grants", &common.Parameter{
		LabelZH: "授权",
		LabelEN: "Grants",
	})
	var grants model.Grants
	params.MustRegister(grants, "Query", &common.Parameter{
		LabelZH:       "授权 SQL",
		LabelEN:       "Grant SQL",
		DescriptionEN: "write SQL like `GRANT CREATE ON *.* WITH GRANT OPTION`",
		DescriptionZH: "通过SQL编写GRANT语句定义， 如： `GRANT CREATE ON *.* WITH GRANT OPTION`",
	})

	var interval model.Interval
	params.MustRegister(interval, "Duration", &common.Parameter{
		LabelZH:       "周期时间（秒）",
		LabelEN:       "Duration",
		DescriptionZH: "周期的有效时长，默认为1小时",
		DescriptionEN: "Length of the interval.",
		Default:       "3600",
	})
	params.MustRegister(interval, "Queries", &common.Parameter{
		LabelZH:       "请求总数限制",
		LabelEN:       "Queries",
		DescriptionZH: "0为不限制",
		DescriptionEN: "Length of the interval.",
		Default:       "0",
		Required:      "false",
	})
	params.MustRegister(interval, "QuerySelects", &common.Parameter{
		LabelZH:  "查询限制",
		LabelEN:  "QuerySelects",
		Default:  "0",
		Required: "false",
	})
	params.MustRegister(interval, "QueryInserts", &common.Parameter{
		LabelZH:  "插入限制",
		LabelEN:  "QueryInserts",
		Default:  "0",
		Required: "false",
	})
	params.MustRegister(interval, "Errors", &common.Parameter{
		LabelZH:  "错误限制",
		LabelEN:  "Errors",
		Default:  "0",
		Required: "false",
	})
	params.MustRegister(interval, "ResultRows", &common.Parameter{
		LabelZH:  "返回行限制",
		LabelEN:  "ResultRows",
		Default:  "0",
		Required: "false",
	})
	params.MustRegister(interval, "ReadRows", &common.Parameter{
		LabelZH:  "读取行限制",
		LabelEN:  "ReadRows",
		Default:  "0",
		Required: "false",
	})
	params.MustRegister(interval, "ExecutionTime", &common.Parameter{
		LabelZH:  "执行时间限制",
		LabelEN:  "ExecutionTime",
		Default:  "0",
		Required: "false",
	})
	return params
}

func RegistUpdateConfigSchema() common.ConfigParams {
	var params common.ConfigParams = make(map[string]*common.Parameter)
	var conf model.CKManClickHouseConfig
	params.MustRegister(conf, "Comment", &common.Parameter{
		LabelZH:  "备注",
		LabelEN:  "Comment",
		Required: "false",
	})
	params.MustRegister(conf, "Version", &common.Parameter{
		LabelZH:       "ClickHouse版本",
		LabelEN:       "Version",
		DescriptionZH: "需要部署的ClickHouse集群的安装包版本",
		DescriptionEN: "which version of clickhouse ",
		Editable:      "false",
	})

	params.MustRegister(conf, "PkgType", &common.Parameter{
		LabelZH:       "安装包类型",
		LabelEN:       "Package Type",
		DescriptionZH: "安装包的类型，表示当前安装包是什么系统架构，什么压缩格式",
		DescriptionEN: "The type of the installation package, indicating what system architecture and compression format",
		Editable:      "false",
	})
	params.MustRegister(conf, "Cwd", &common.Parameter{
		LabelZH:       "工作路径",
		LabelEN:       "WorkingDirectory",
		DescriptionZH: "tgz部署的工作目录（不可修改）。卸载时仅清空其下的 bin/etc/log/run 四个子目录",
		DescriptionEN: "Working directory for tgz deployment (read-only). Uninstall removes only the bin/etc/log/run subdirectories.",
		Visiable:      "PkgType.indexOf('tgz') !== -1",
		Editable:      "false",
	})
	params.MustRegister(conf, "Cluster", &common.Parameter{
		LabelZH:       "物理集群名",
		LabelEN:       "Cluster Name",
		DescriptionZH: "不得与本ckman管理的其他集群名重复",
		DescriptionEN: "not allow to duplicate with exist name",
		Editable:      "false",
	})
	params.MustRegister(conf, "LogicCluster", &common.Parameter{
		LabelZH:       "逻辑集群名",
		LabelEN:       "Logic Name",
		DescriptionZH: "逻辑集群，存在于物理集群之上， 仅支持将未设置逻辑集群的物理集群加入已有逻辑集群",
		DescriptionEN: "require physical cluster, only supports adding a physical cluster without a logical cluster to an existed",
	})

	params.MustRegister(conf, "Path", &common.Parameter{
		LabelZH:       "数据存储路径",
		LabelEN:       "Data Path",
		DescriptionZH: "ClickHouse存储数据的路径（不可修改）。tgz部署下不会落在 ${Cwd}{bin,etc,log,run}/ 中，因此卸载不会误删数据",
		DescriptionEN: "Data path for ClickHouse (read-only). Guaranteed to live outside ${Cwd}{bin,etc,log,run}/ for tgz, so uninstall will not delete data.",
		Editable:      "false",
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
			{Value: "0", LabelEN: "Password(save)", LabelZH: "密码认证(保存密码)"},
			{Value: "1", LabelEN: "Password(not save)", LabelZH: "密码认证(不保存密码)"},
			{Value: "2", LabelEN: "Public Key", LabelZH: "公钥认证"},
		},
	})
	params.MustRegister(conf, "SshPassword", &common.Parameter{
		LabelZH:       "系统账户密码",
		LabelEN:       "SSH Password",
		DescriptionZH: "不得为空",
		DescriptionEN: "can't be empty",
		Visiable:      "AuthenticateType != '2'",
		InputType:     common.InputPassword,
		Required:      "false",
	})
	params.MustRegister(conf, "SshPort", &common.Parameter{
		LabelZH:       "SSH 端口",
		LabelEN:       "SSH Port",
		DescriptionZH: "不得为空",
	})
	params.MustRegister(conf, "Shards", &common.Parameter{
		LabelZH:       "集群节点配置",
		LabelEN:       "ClickHouse Cluster Node",
		DescriptionEN: "shard of clickhouse cluster",
		DescriptionZH: "集群分片信息",
		Editable:      "false",
	})

	var shard model.CkShard
	params.MustRegister(shard, "Replicas", &common.Parameter{
		LabelZH:       "分片",
		LabelEN:       "Shard",
		DescriptionEN: "replicas of each shard",
		DescriptionZH: "集群每个分片的副本信息",
		Editable:      "false",
	})
	var replica model.CkReplica
	params.MustRegister(replica, "Ip", &common.Parameter{
		LabelZH:  "副本节点IP",
		LabelEN:  "Replica Node IP",
		Editable: "false",
	})
	params.MustRegister(conf, "Keeper", &common.Parameter{
		DescriptionZH: "如果使用clickhouse-keeper， 则默认由ckman托管；如果使用已有zookeeper或已经创建好的keeper集群，都视同zookeeper",
		Candidates: []common.Candidate{
			{Value: model.Zookeeper, LabelEN: "Zookeeper", LabelZH: "Zookeeper"},
			{Value: model.ClickhouseKeeper, LabelEN: "ClickHouse-Keeper", LabelZH: "ClickHouse-Keeper"},
		},
		Editable: "false",
	})

	params.MustRegister(conf, "KeeperConf", &common.Parameter{
		LabelZH:       "Keeper配置",
		LabelEN:       "KeeperConf",
		DescriptionZH: "clickhouse-keeper的配置项",
		Visiable:      "Keeper == 'clickhouse-keeper'",
	})

	var keeper model.KeeperConf
	params.MustRegister(keeper, "Runtime", &common.Parameter{
		LabelZH:       "运行方式",
		LabelEN:       "Runtime",
		DescriptionZH: "如果单独部署，则和clickhouse-server 分开进程；如果内置，则和clickhouse-server放在一块",
		Candidates: []common.Candidate{
			{Value: model.KeeperRuntimeStandalone, LabelEN: "Standalone", LabelZH: "单独部署"},
			{Value: model.KeeperRuntimeInternal, LabelEN: "Internal", LabelZH: "内置"},
		},
		Editable: "false",
	})
	params.MustRegister(keeper, "KeeperNodes", &common.Parameter{
		LabelZH:  "Keeper节点",
		LabelEN:  "KeeperNodes",
		Editable: "false",
	})

	params.MustRegister(keeper, "TcpPort", &common.Parameter{
		LabelZH: "Keeper端口",
		LabelEN: "TcpPort",
	})
	params.MustRegister(keeper, "RaftPort", &common.Parameter{
		LabelZH: "Raft通信端口",
		LabelEN: "RaftPort",
	})
	params.MustRegister(keeper, "Path", &common.Parameter{
		LabelZH:       "Keeper数据路径",
		LabelEN:       "KeeperPath",
		DescriptionZH: "Keeper存储数据的路径（不可修改）。tgz部署下不会落在 ${Cwd}{bin,etc,log,run}/ 中，因此卸载不会误删数据",
		DescriptionEN: "Data path for Keeper (read-only). Guaranteed to live outside ${Cwd}{bin,etc,log,run}/ for tgz, so uninstall will not delete data.",
		Editable:      "false",
	})
	params.MustRegister(keeper, "Expert", &common.Parameter{
		LabelZH:  "专家配置",
		LabelEN:  "Expert",
		Required: "false",
	})
	params.MustRegister(keeper, "Coordination", &common.Parameter{
		LabelZH:  "协作配置",
		LabelEN:  "Coordination",
		Required: "false",
	})

	var coordination model.Coordination
	params.MustRegister(coordination, "OperationTimeoutMs", &common.Parameter{
		LabelZH:  "OperationTimeoutMs",
		LabelEN:  "OperationTimeoutMs",
		Default:  "10000",
		Required: "false",
	})
	params.MustRegister(coordination, "SessionTimeoutMs", &common.Parameter{
		LabelZH:  "SessionTimeoutMs",
		LabelEN:  "SessionTimeoutMs",
		Default:  "30000",
		Required: "false",
	})
	params.MustRegister(coordination, "ForceSync", &common.Parameter{
		LabelZH:  "ForceSync",
		LabelEN:  "ForceSync",
		Required: "false",
	})
	// params.MustRegister(coordination, "AutoForwarding", &common.Parameter{
	// 	LabelZH:  "AutoForwarding",
	// 	LabelEN:  "AutoForwarding",
	// 	Required: "false",
	// })
	params.MustRegister(coordination, "Expert", &common.Parameter{
		LabelZH:  "专家配置",
		LabelEN:  "Expert",
		Required: "false",
	})

	params.MustRegister(conf, "ZkNodes", &common.Parameter{
		LabelZH:       "ZooKeeper集群结点列表",
		LabelEN:       "Zookeeper Node List",
		DescriptionZH: "每段为单个IP，或者IP范围，或者网段掩码",
		DescriptionEN: "Zookeeper Node ip, support CIDR or Range.",
		Visiable:      "Keeper == 'zookeeper'",
	})
	params.MustRegister(conf, "ZkPort", &common.Parameter{
		LabelZH:  "ZooKeeper集群监听端口",
		LabelEN:  "Zookeeper Port",
		Visiable: "Keeper == 'zookeeper'",
	})
	// params.MustRegister(conf, "PromHost", &common.Parameter{
	// 	LabelZH:  "Promethues 地址",
	// 	LabelEN:  "Prometheus Host",
	// 	Default:  "127.0.0.1",
	// 	Required: "false",
	// })
	// params.MustRegister(conf, "PromPort", &common.Parameter{
	// 	LabelZH:  "Promethues 端口",
	// 	LabelEN:  "Prometheus Port",
	// 	Default:  "9090",
	// 	Required: "false",
	// })
	// params.MustRegister(conf, "PromMetricPort", &common.Parameter{
	// 	LabelZH:  "Promethues指标端口",
	// 	LabelEN:  "Prometheus Metrics Port",
	// 	Required: "false",
	// })

	// var promPort model.PromMetricPort
	// params.MustRegister(promPort, "ClickHouse", &common.Parameter{
	// 	LabelZH:       "ClickHouse指标端口",
	// 	LabelEN:       "ClickHouse Metrics Port",
	// 	Default:       "9363",
	// 	Required:      "false",
	// 	DescriptionEN: "Prometheus port export by ClickHouse",
	// 	DescriptionZH: "clickhouse暴露的prometheus端口",
	// })
	// params.MustRegister(promPort, "ZooKeeper", &common.Parameter{
	// 	LabelZH:       "ZooKeeper指标端口",
	// 	LabelEN:       "Zookeeper Metrics Port",
	// 	Default:       "7000",
	// 	Required:      "false",
	// 	DescriptionEN: "Prometheus port export by Zookeeper",
	// 	DescriptionZH: "zookeeper暴露的prometheus端口",
	// })
	// params.MustRegister(promPort, "NodeExport", &common.Parameter{
	// 	LabelZH:       "Node Exporter端口",
	// 	LabelEN:       "Node Exporter Port",
	// 	Default:       "9100",
	// 	Required:      "false",
	// 	DescriptionEN: "Prometheus port export by NodeExporter",
	// 	DescriptionZH: "NodeExporter暴露的prometheus端口",
	// })

	params.MustRegister(conf, "Password", &common.Parameter{
		LabelZH:   "默认用户密码",
		LabelEN:   "Default Password",
		InputType: common.InputPassword,
	})
	params.MustRegister(conf, "EncryptType", &common.Parameter{
		LabelZH:       "密码加密算法",
		LabelEN:       "EncryptType",
		DescriptionZH: "密码保存时使用什么加密方式，默认明文",
		DescriptionEN: "What encryption method is used when the password is saved, the default is plaintext",
		Candidates: []common.Candidate{
			{Value: "0", LabelEN: "PLAINTEXT", LabelZH: "PLAINTEXT"},
			{Value: "1", LabelEN: "SHA256_HEX", LabelZH: "SHA256_HEX"},
			{Value: "2", LabelEN: "DOUBLE_SHA1_HEX", LabelZH: "DOUBLE_SHA1_HEX"},
		},
		Default: "0",
	})
	params.MustRegister(conf, "Protocol", &common.Parameter{
		LabelZH: "连接协议",
		LabelEN: "Protocol",
		Default: "native",
		Candidates: []common.Candidate{
			{Value: "native", LabelEN: "native", LabelZH: "native"},
			{Value: "http", LabelEN: "http", LabelZH: "http"},
		},
		Editable: "false",
	})
	params.MustRegister(conf, "Port", &common.Parameter{
		LabelZH: "TCP端口",
		LabelEN: "TCPPort",
	})
	params.MustRegister(conf, "HttpPort", &common.Parameter{
		LabelZH:  "HTTP端口",
		LabelEN:  "HTTP Port",
		Editable: "false",
	})
	params.MustRegister(conf, "Secure", &common.Parameter{
		LabelZH:  "TLS安全认证",
		LabelEN:  "TLS Secure",
		Editable: "false",
	})
	params.MustRegister(conf, "Storage", &common.Parameter{
		LabelZH:       "集群存储配置",
		LabelEN:       "Storage Policy",
		DescriptionZH: "由disks, policies两部分构成。policies提到的disk名必须在disks中定义。ClickHouse内置了名为default的policy和disk。",
		DescriptionEN: "Composed of Disks, Policies. The Disk name mentioned by Policies must be defined in Disks. Clickhouse has built-in Policy and Disk named Default. ",
	})
	params.MustRegister(conf, "Expert", &common.Parameter{
		LabelZH: "自定义配置项",
		LabelEN: "Custom Config",
		DescriptionZH: `自定义配置文件，语法接近xpath(https://www.w3schools.com/xml/xpath_syntax.asp);
举例：title[@lang='en', @size=4]/header:header123， 最终生成的配置为:
<title lang="en" size="4">
    <header>header123</header>
</title>
非专业人士请勿填写此项`,
		DescriptionEN: `Custom configuration items, similar to xpath syntax(https://www.w3schools.com/xml/xpath_syntax.asp);
For example: title[@lang='en', @size=4]/header:header123, the final generated configuration is:
<title lang="en" size="4">
    <header>header123</header>
</title>
Non-professionals please do not fill in this`,
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
			{Value: "cache", LabelEN: "Cache", LabelZH: "缓存"},
			//{Value: "hdfs", LabelEN: "HDFS", LabelZH: "HDFS"},
		},
	})
	params.MustRegister(disk, "AllowedBackup", &common.Parameter{
		LabelZH:       "允许备份",
		LabelEN:       "AllowedBackup",
		Required:      "false",
		DescriptionZH: "是否允许备份数据到该磁盘",
		DescriptionEN: "Whether to allow backup data to the disk",
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
	params.MustRegister(disk, "DiskCache", &common.Parameter{
		LabelZH:  "磁盘缓存",
		LabelEN:  "Disk Cache",
		Visiable: "Type == 'cache'",
	})

	var diskcache model.DiskCache
	params.MustRegister(diskcache, "Path", &common.Parameter{
		LabelZH:       "缓存路径",
		LabelEN:       "Cache Path",
		DescriptionZH: "缓存数据存储的路径，必须以'/'开头和结尾",
		DescriptionEN: "Path for cached data, must begin and end with '/'",
		Regexp:        "^/.+/$",
		Editable:      "false",
		Required:      "true",
	})
	params.MustRegister(diskcache, "MaxSize", &common.Parameter{
		LabelZH:       "最大缓存大小",
		LabelEN:       "Max Size",
		DescriptionZH: "缓存的最大大小（字节）",
		DescriptionEN: "Maximum size of cache in bytes",
		Required:      "true",
		Default:       "10737418240",
	})
	params.MustRegister(diskcache, "Disk", &common.Parameter{
		LabelZH:       "底层磁盘",
		LabelEN:       "Backend Disk",
		DescriptionZH: "用于存储缓存数据的底层磁盘名称",
		DescriptionEN: "Name of the backend disk for storing cached data",
		Editable:      "true",
		Required:      "true",
	})
	params.MustRegister(diskcache, "CacheOnWriteOperations", &common.Parameter{
		LabelZH:       "写操作时缓存",
		LabelEN:       "Cache On Write Operations",
		DescriptionZH: "在写入操作时是否启用缓存",
		DescriptionEN: "Enable caching on write operations",
		Default:       "false",
		Required:      "false",
	})
	params.MustRegister(diskcache, "CachePolicy", &common.Parameter{
		LabelZH:       "缓存策略",
		LabelEN:       "Cache Policy",
		DescriptionZH: "缓存淘汰策略:SLRU 或 LRU",
		DescriptionEN: "Cache eviction policy: SLRU or LRU",
		Candidates: []common.Candidate{
			{Value: "SLRU", LabelEN: "SLRU", LabelZH: "SLRU"},
			{Value: "LRU", LabelEN: "LRU", LabelZH: "LRU"},
		},
		Default:  "SLRU",
		Required: "true",
	})
	params.MustRegister(diskcache, "SLRUSizeRatio", &common.Parameter{
		LabelZH:       "SLRU大小比例",
		LabelEN:       "SLRU Size Ratio",
		DescriptionZH: "SLRU 缓存中试用部分的比例",
		DescriptionEN: "Ratio of probationary part in SLRU cache",
		Range:         &common.Range{Min: 0.0, Max: 1.0, Step: 0.1},
		Required:      "false",
	})
	params.MustRegister(diskcache, "Expert", &common.Parameter{
		LabelZH:       "专家配置",
		LabelEN:       "Expert",
		DescriptionZH: "缓存高级配置，参考: https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCacheSettings.cpp",
		DescriptionEN: "Advanced cache configuration, refer to: https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCacheSettings.cpp",
		Required:      "false",
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
		Required:      "false",
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

	var userconf model.UsersConf
	params.MustRegister(userconf, "Users", &common.Parameter{
		LabelZH:       "用户",
		LabelEN:       "Users",
		DescriptionZH: "普通用户的管理",
		DescriptionEN: "normal user config management",
		Required:      "false",
	})
	params.MustRegister(userconf, "Profiles", &common.Parameter{
		LabelZH:  "配置管理",
		LabelEN:  "Profiles",
		Required: "false",
	})
	params.MustRegister(userconf, "Quotas", &common.Parameter{
		LabelZH:  "配额管理",
		LabelEN:  "Quotas",
		Required: "false",
	})
	params.MustRegister(userconf, "Roles", &common.Parameter{
		LabelZH:  "角色管理",
		LabelEN:  "Roles",
		Required: "false",
	})
	params.MustRegister(userconf, "Expert", &common.Parameter{
		LabelZH: "用户高级配置",
		LabelEN: "User Custom Config",
		DescriptionZH: `自定义配置文件，语法接近xpath(https://www.w3schools.com/xml/xpath_syntax.asp);
举例：title[@lang='en', @size=4]/header:header123， 最终生成的配置为:
<title lang="en" size="4">
    <header>header123</header>
</title>
非专业人士请勿填写此项`,
		DescriptionEN: `Custom configuration items, similar to xpath syntax(https://www.w3schools.com/xml/xpath_syntax.asp);
For example: title[@lang='en', @size=4]/header:header123, the final generated configuration is:
<title lang="en" size="4">
    <header>header123</header>
</title>
Non-professionals please do not fill in this`,
		Required: "false",
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
	params.MustRegister(user, "EncryptType", &common.Parameter{
		LabelZH:       "密码加密算法",
		LabelEN:       "EncryptType",
		DescriptionZH: "密码保存时使用什么加密方式，默认明文",
		DescriptionEN: "What encryption method is used when the password is saved, the default is plaintext",
		Candidates: []common.Candidate{
			{Value: "0", LabelEN: "PLAINTEXT", LabelZH: "PLAINTEXT"},
			{Value: "1", LabelEN: "SHA256_HEX", LabelZH: "SHA256_HEX"},
			{Value: "2", LabelEN: "DOUBLE_SHA1_HEX", LabelZH: "DOUBLE_SHA1_HEX"},
		},
		Default: "0",
	})
	params.MustRegister(user, "Profile", &common.Parameter{
		LabelZH:       "限额",
		LabelEN:       "Profile",
		DescriptionZH: "设置用户相关参数，如：最大内存使用、只读等",
		DescriptionEN: "Set user-related parameters, such as: maximum memory usage, read-only, etc",
		Required:      "false",
	})
	params.MustRegister(user, "Quota", &common.Parameter{
		LabelZH:       "配额",
		LabelEN:       "Quota",
		DescriptionZH: "配额允许您在一段时间内跟踪或限制资源使用情况",
		DescriptionEN: "Quotas allow you to track or limit resource usage over a period of time. ",
		Required:      "false",
	})
	params.MustRegister(user, "Roles", &common.Parameter{
		LabelZH:       "角色",
		LabelEN:       "Roles",
		DescriptionZH: "角色可以通过SQL的方式定义某类用户对数据库的访问权限",
		DescriptionEN: "Roles can define the access rights of certain types of users to the database by means of SQL.",
		Required:      "false",
	})
	params.MustRegister(user, "Networks", &common.Parameter{
		LabelZH:       "允许登录地址",
		LabelEN:       "NetWorks",
		DescriptionZH: "用户可以连接到 ClickHouse 服务器的网络列表。",
		DescriptionEN: "List of networks from which the user can connect to the ClickHouse server.",
		Required:      "false",
	})
	params.MustRegister(user, "DbRowPolices", &common.Parameter{
		LabelZH:       "访问权限",
		LabelEN:       "DbRowPolices",
		DescriptionZH: "设置数据库及表的访问权限",
		DescriptionEN: "Set database and table access permissions",
		Required:      "false",
	})

	var dbRow model.DbRowPolicy
	params.MustRegister(dbRow, "Database", &common.Parameter{
		LabelZH:       "数据库",
		LabelEN:       "Database",
		DescriptionZH: "用户只能访问的数据库",
		DescriptionEN: "Databases that users can only access",
	})
	params.MustRegister(dbRow, "TblRowPolicies", &common.Parameter{
		LabelZH:       "行访问权限",
		LabelEN:       "TblRowPolicies",
		DescriptionZH: "用户只能访问数据库的哪些行",
		DescriptionEN: "Which rows of the database the user can only access",
		Required:      "false",
	})

	var networks model.Networks
	params.MustRegister(networks, "IPs", &common.Parameter{
		LabelZH:       "IP列表",
		LabelEN:       "IPs",
		DescriptionZH: "用户能访问的数据库表",
		DescriptionEN: "IP address or network mask",
		Required:      "false",
	})
	params.MustRegister(networks, "Hosts", &common.Parameter{
		LabelZH:       "主机列表",
		LabelEN:       "Hosts",
		DescriptionZH: "用户能访问的数据库表",
		DescriptionEN: "o check access, a DNS query is performed, and all returned IP addresses are compared to the peer address.",
		Required:      "false",
	})
	params.MustRegister(networks, "HostRegexps", &common.Parameter{
		LabelZH:       "主机名正则匹配",
		LabelEN:       "HostRegexps",
		DescriptionZH: "主机名正则表达式匹配",
		DescriptionEN: "Regular expression for hostnames",
		Required:      "false",
	})

	var tblRow model.TblRowPolicy
	params.MustRegister(tblRow, "Table", &common.Parameter{
		LabelZH:       "表名",
		LabelEN:       "Table",
		DescriptionZH: "用户能访问的数据库表",
		DescriptionEN: "Which table the user can only access",
	})
	params.MustRegister(tblRow, "Filter", &common.Parameter{
		LabelZH:       "过滤器",
		LabelEN:       "Filter",
		DescriptionZH: "过滤器可以是任何产生 UInt8 类型值的表达式。它通常包含比较和逻辑运算符。不为此用户返回从 database_name.table1 中筛选结果为 0 的行。过滤与 PREWHERE 操作不兼容，并禁用 WHERE→PREWHERE 优化。",
		DescriptionEN: "The filter can be any expression resulting in a UInt8-type value. It usually contains comparisons and logical operators. Rows from database_name.table1 where filter results to 0 are not returned for this user. The filtering is incompatible with PREWHERE operations and disables WHERE→PREWHERE optimization.",
	})

	var profile model.Profile
	params.MustRegister(profile, "Name", &common.Parameter{
		LabelZH: "配置名称",
		LabelEN: "Name",
	})
	params.MustRegister(profile, "ReadOnly", &common.Parameter{
		LabelZH:       "只读约束",
		LabelEN:       "ReadOnly",
		DescriptionZH: "限制除 DDL 查询之外的所有类型的查询的权限。",
		DescriptionEN: "Restricts permissions for all types of queries except DDL queries.",
		Candidates: []common.Candidate{
			{Value: "0", LabelEN: "Default", LabelZH: "不限制"},
			{Value: "1", LabelEN: "Read", LabelZH: "只读权限"},
			{Value: "2", LabelEN: "Read and Set", LabelZH: "读权限和设置权限"},
		},
		Default:  "0",
		Required: "false",
	})
	params.MustRegister(profile, "AllowDDL", &common.Parameter{
		LabelZH:       "DDL权限",
		LabelEN:       "AllowDDL",
		DescriptionZH: "限制除 DDL 查询之外的所有类型的查询的权限",
		DescriptionEN: "Restricts permissions for DDL queries",
		Candidates: []common.Candidate{
			{Value: "0", LabelEN: "Not Allowed", LabelZH: "不允许 "},
			{Value: "1", LabelEN: "Allowed", LabelZH: "允许"},
		},
		Default:  "1",
		Required: "false",
	})
	params.MustRegister(profile, "MaxThreads", &common.Parameter{
		LabelZH:       "最大线程数",
		LabelEN:       "MaxThreads",
		DescriptionZH: "查询处理线程的最大数量，不包括从远程服务器检索数据的线程",
		DescriptionEN: "The maximum number of query processing threads, excluding threads for retrieving data from remote servers (see the ‘max_distributed_connections’ parameter).",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxMemoryPercent", &common.Parameter{
		LabelZH:       "最大使用内存（百分比）",
		LabelEN:       "MaxMemoryPercent",
		DescriptionZH: "用于在单个服务器上运行查询的最大RAM量（百分比）",
		DescriptionEN: "The maximum amount of RAM to use for running a query on a single server.",
		Range:         &common.Range{Min: 0.0, Max: 1.0},
		Default:       "0.5",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxMemoryPercentForAllQueries", &common.Parameter{
		LabelZH:       "用户查询可用最大内存（百分比）",
		LabelEN:       "MaxMemoryPercentForAllQueries",
		DescriptionZH: "用于在单个服务器上运行查询的最大RAM量（百分比）",
		DescriptionEN: "The maximum amount of RAM to use for running a query on a single server.",
		Range:         &common.Range{Min: 0.0, Max: 1.0},
		Default:       "0.75",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxExecutionTime", &common.Parameter{
		LabelZH:       "SQL超时时间（秒）",
		LabelEN:       "MaxExecutionTime",
		DescriptionZH: "如果查询运行时间超过指定的秒数，则行为将由 'timeout_overflow_mode' 确定，默认情况下为 - 引发异常。请注意，在数据处理过程中，将检查超时，查询只能在指定位置停止。它目前无法在聚合状态合并或查询分析期间停止，实际运行时间将高于此设置的值。",
		DescriptionEN: "If query run time exceeded the specified number of seconds, the behavior will be determined by the 'timeout_overflow_mode' which by default is - throw an exception. Note that the timeout is checked and query can stop only in designated places during data processing. It currently cannot stop during merging of aggregation states or during query analysis, and the actual run time will be higher than the value of this setting.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxBytesToRead", &common.Parameter{
		LabelZH:       "最大读取字节数（字节）",
		LabelEN:       "MaxBytesToRead",
		DescriptionZH: "在执行查询时，从表中可读取的未压缩数据的最大字节数。该限制会在每个处理的数据块上进行检查，仅适用于最深层的表表达式；在从远程服务器读取时，该限制仅在远程服务器上进行检查。",
		DescriptionEN: "The maximum number of bytes of uncompressed data that can be read from a table when executing a query. This limit is checked on each processed data block and only applies to the deepest table expression; when reading from remote servers, this limit is only checked on the remote servers.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxConcurrentQueriesForUser", &common.Parameter{
		LabelZH:       "用户最大并发查询数",
		LabelEN:       "MaxConcurrentQueriesForUser",
		DescriptionZH: "用户并发查询的最大数量。0 表示不限制。",
		DescriptionEN: "The maximum number of concurrent queries for a user. 0 means no limit.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxPartitionsToRead", &common.Parameter{
		LabelZH:       "最大读取分区数",
		LabelEN:       "MaxPartitionsToRead",
		DescriptionZH: "限制在单个查询中可访问的最大分区数量。-1 表示没有限制。",
		DescriptionEN: "Limits the maximum number of partitions that can be accessed in a single query. -1 means no limit.",
		Default:       "-1",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxResultBytes", &common.Parameter{
		LabelZH:       "最大结果字节数（字节）",
		LabelEN:       "MaxResultBytes",
		DescriptionZH: "限制结果的大小（以字节数计，未压缩）。如果达到该阈值，查询在处理完一个数据块后会停止，但不会截断结果的最后一个数据块，因此最终结果的大小可能会大于该阈值。",
		DescriptionEN: "Limits the size of the result in bytes (uncompressed). If the threshold is reached, the query stops after processing a data block, but does not truncate the last data block of the result, so the final result size may be larger than the threshold.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxResultRows", &common.Parameter{
		LabelZH:       "最大结果行数（行）",
		LabelEN:       "MaxResultRows",
		DescriptionZH: "限制结果中的行数。对子查询，以及在远程服务器上执行分布式查询的部分时也会进行检查。当值为 0 时，不施加任何限制。",
		DescriptionEN: "Limits the number of rows in the result. Also checked for subqueries and when executing distributed query parts on remote servers. When the value is 0, no limit is imposed.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxRowsToRead", &common.Parameter{
		LabelZH:       "最大读取行数",
		LabelEN:       "MaxRowsToRead",
		DescriptionZH: "单查询从表中可读取的最大行数。0 表示不限制。",
		DescriptionEN: "Maximum number of rows that can be read from a table in a single query. 0 means no limit.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxBytesBeforeExternalGroupBy", &common.Parameter{
		LabelZH:       "外部聚合阈值（字节）",
		LabelEN:       "MaxBytesBeforeExternalGroupBy",
		DescriptionZH: "GROUP BY 强制 spill 到磁盘的内存阈值（字节）。0 表示禁用外部聚合。建议设为 max_memory_usage 的一半。",
		DescriptionEN: "Memory threshold (bytes) above which GROUP BY spills to disk. 0 disables external aggregation. Recommended to set to half of max_memory_usage.",
		Required:      "false",
	})
	params.MustRegister(profile, "MaxBytesBeforeExternalSort", &common.Parameter{
		LabelZH:       "外部排序阈值（字节）",
		LabelEN:       "MaxBytesBeforeExternalSort",
		DescriptionZH: "ORDER BY 强制 spill 到磁盘的内存阈值（字节）。0 表示禁用外部排序。",
		DescriptionEN: "Memory threshold (bytes) above which ORDER BY spills to disk. 0 disables external sorting.",
		Required:      "false",
	})
	params.MustRegister(profile, "S3MaxGetRPS", &common.Parameter{
		LabelZH:       "S3 GET 限速（请求/秒）",
		LabelEN:       "S3MaxGetRPS",
		DescriptionZH: "单查询访问 S3 的最大 GET 请求速率（次/秒）。0 表示不限制。",
		DescriptionEN: "Maximum S3 GET request rate per query (requests/sec). 0 means no limit.",
		Required:      "false",
	})
	params.MustRegister(profile, "ForceIndexByDate", &common.Parameter{
		LabelZH:       "强制日期索引",
		LabelEN:       "ForceIndexByDate",
		DescriptionZH: "强制查询使用基于日期的索引，未命中时拒绝执行。0/1。",
		DescriptionEN: "Force queries to use date-based index; reject queries without one. 0/1.",
		Range:         &common.Range{Min: 0, Max: 1, Step: 1},
		Required:      "false",
	})
	params.MustRegister(profile, "ForcePrimaryKey", &common.Parameter{
		LabelZH:       "强制主键",
		LabelEN:       "ForcePrimaryKey",
		DescriptionZH: "强制查询使用主键，未命中时拒绝执行。0/1。",
		DescriptionEN: "Force queries to use primary key; reject queries without one. 0/1.",
		Range:         &common.Range{Min: 0, Max: 1, Step: 1},
		Required:      "false",
	})
	params.MustRegister(profile, "Expert", &common.Parameter{
		LabelZH:       "专家配置",
		LabelEN:       "Expert",
		DescriptionZH: "限额高级配置，参考：https://clickhouse.com/docs/en/operations/settings/settings-profiles/",
		DescriptionEN: "Advanced configuration of quota, refer to https://clickhouse.com/docs/en/operations/settings/settings-profiles/",
		Required:      "false",
	})

	var quota model.Quota
	params.MustRegister(quota, "Name", &common.Parameter{
		LabelZH: "配额名称",
		LabelEN: "Name",
	})
	params.MustRegister(quota, "Intervals", &common.Parameter{
		LabelZH:       "周期（秒）",
		LabelEN:       "Interval",
		DescriptionZH: "配额生效的周期时段",
		DescriptionEN: "Restrictions for a time period. You can set many intervals with different restrictions.",
	})

	var role model.Role
	params.MustRegister(role, "Name", &common.Parameter{
		LabelZH: "角色名称",
		LabelEN: "Name",
	})
	params.MustRegister(role, "Grants", &common.Parameter{
		LabelZH: "授权",
		LabelEN: "Grants",
	})
	var grants model.Grants
	params.MustRegister(grants, "Query", &common.Parameter{
		LabelZH:       "授权 SQL",
		LabelEN:       "Grant SQL",
		DescriptionEN: "write SQL like `GRANT CREATE ON *.* WITH GRANT OPTION`",
		DescriptionZH: "通过SQL编写GRANT语句定义， 如： `GRANT CREATE ON *.* WITH GRANT OPTION`",
	})

	var interval model.Interval
	params.MustRegister(interval, "Duration", &common.Parameter{
		LabelZH:       "周期时间",
		LabelEN:       "Duration",
		DescriptionZH: "周期的有效时长，默认为1小时",
		DescriptionEN: "Length of the interval.",
		Default:       "3600",
	})
	params.MustRegister(interval, "Queries", &common.Parameter{
		LabelZH:       "请求总数限制",
		LabelEN:       "Queries",
		DescriptionZH: "0为不限制",
		DescriptionEN: "Length of the interval.",
		Default:       "0",
		Required:      "false",
	})
	params.MustRegister(interval, "QuerySelects", &common.Parameter{
		LabelZH:  "查询限制",
		LabelEN:  "QuerySelects",
		Default:  "0",
		Required: "false",
	})
	params.MustRegister(interval, "QueryInserts", &common.Parameter{
		LabelZH:  "插入限制",
		LabelEN:  "QueryInserts",
		Default:  "0",
		Required: "false",
	})
	params.MustRegister(interval, "Errors", &common.Parameter{
		LabelZH:  "错误限制",
		LabelEN:  "Errors",
		Default:  "0",
		Required: "false",
	})
	params.MustRegister(interval, "ResultRows", &common.Parameter{
		LabelZH:  "返回行限制",
		LabelEN:  "ResultRows",
		Default:  "0",
		Required: "false",
	})
	params.MustRegister(interval, "ReadRows", &common.Parameter{
		LabelZH:  "读取行限制",
		LabelEN:  "ReadRows",
		Default:  "0",
		Required: "false",
	})
	params.MustRegister(interval, "ExecutionTime", &common.Parameter{
		LabelZH:  "执行时间限制",
		LabelEN:  "ExecutionTime",
		Default:  "0",
		Required: "false",
	})

	return params
}

func (ui *SchemaUIController) RegistSchemaInstance() {
	SchemaUIMapping = make(map[string]common.ConfigParams)
	for k, v := range schemaHandleFunc {
		SchemaUIMapping[k] = v()
	}
}

// 这个接口不暴露出去
func (controller *SchemaUIController) GetUISchema(c *gin.Context) {
	Type := c.Query("type")
	if Type == "" {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, nil)
		return
	}

	var schema string
	var err error
	switch Type {
	case GET_SCHEMA_UI_CONFIG, GET_SCHEMA_UI_DEPLOY:
		var conf model.CKManClickHouseConfig
		typo := strings.ToLower(Type)
		params := GetSchemaParams(typo, conf)
		if params == nil {
			controller.wrapfunc(c, model.E_INVALID_VARIABLE, errors.Errorf("type %s is not regist", typo))
			return
		}
		schema, err = params.MarshalSchema(conf)
		if err != nil {
			controller.wrapfunc(c, model.E_MARSHAL_FAILED, err)
			return
		}
	}
	controller.wrapfunc(c, model.E_SUCCESS, schema)
}

func GetSchemaParams(typo string, conf model.CKManClickHouseConfig) common.ConfigParams {
	params, ok := SchemaUIMapping[typo]
	if !ok {
		return nil
	}

	if typo == GET_SCHEMA_UI_DEPLOY {
		// get version list every time
		params.MustRegister(conf, "PkgName", &common.Parameter{
			LabelZH:       "ClickHouse版本",
			LabelEN:       "Package Name",
			DescriptionZH: "需要部署的ClickHouse集群的安装包版本，只显示common安装包，但需提前上传common、server、client安装包",
			DescriptionEN: "which package of clickhouse will deployed, need upload rpm package before",
			Candidates:    getPkgLists(),
			Filter:        "\"PkgName\".indexOf(PkgType) !== -1",
		})

		params.MustRegister(conf, "PkgType", &common.Parameter{
			LabelZH:       "安装包类型",
			LabelEN:       "Package Type",
			DescriptionZH: "安装包的类型，表示当前安装包是什么系统架构，什么压缩格式",
			DescriptionEN: "The type of the installation package, indicating what system architecture and compression format",
			Candidates:    getPkgType(),
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
		return errors.Wrap(err, "")
	}
	err = params.UnmarshalConfig(string(body), conf)
	if err != nil {
		return errors.Wrap(err, "")
	}
	data, err := json.MarshalIndent(*conf, "", "  ")
	if err != nil {
		return errors.Wrap(err, "")
	}
	log.Logger.Debugf("[request] | %s | %s | %s \n%v ", request.Host, request.Method, request.URL, string(data))
	return nil
}
