package deploy

import (
	"github.com/go-errors/errors"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
)

func GenerateStorageXML(filename string, storage model.Storage)(string, error){
	if len(storage.Disks) == 0 && len(storage.Policies) == 0 {
		return "", nil
	}
	xml := common.NewXmlFile(filename)
	xml.XMLBegin("yandex", 0)
	xml.XMLBegin("storage_configuration",1 )
	if len(storage.Disks) > 0 {
		xml.XMLBegin("disks", 2)
			for _, disk := range storage.Disks {
				xml.XMLBegin(disk.Name, 3)
				xml.XMLWrite("type", disk.Type, 4)
				switch disk.Type {
				case "hdfs":
					xml.XMLWrite("endpoint", disk.DiskHdfs.Endpoint, 4)
				case "local":
					xml.XMLWrite("path", disk.DiskLocal.Path, 4)
					xml.XMLWrite("keep_free_space_bytes", disk.DiskLocal.KeepFreeSpaceBytes, 4)
				case "s3":
					xml.XMLWrite("endpoint", disk.DiskS3.Endpoint, 4)
					xml.XMLWrite("access_key_id", disk.DiskS3.AccessKeyID, 4)
					xml.XMLWrite("secret_access_key", disk.DiskS3.SecretAccessKey, 4)
					xml.XMLWrite("region", disk.DiskS3.Region, 4)
					xml.XMLWrite("use_environment_credentials", disk.DiskS3.UseEnvironmentCredentials, 4)
					for k, v := range disk.DiskS3.Expert {
						xml.XMLWrite(k, v, 4)
					}
				default:
					return "", errors.Errorf("unsupport disk type %s", disk.Type)
				}
				xml.XMLEnd(disk.Name, 3)
			}
		xml.XMLEnd("disks", 2)
	}
	if len(storage.Policies) > 0 {
		xml.XMLBegin("policies", 2)
		for _, policy := range storage.Policies {
			xml.XMLBegin(policy.Name, 3)
			xml.XMLBegin("volumes", 4)
			for _, vol := range policy.Volumns {
				xml.XMLBegin(vol.Name, 5)
				for _, disk := range vol.Disks {
					xml.XMLWrite("disk", disk, 6)
				}
				xml.XMLWrite("max_data_part_size_bytes", vol.MaxDataPartSizeBytes, 6)
				xml.XMLWrite("prefer_not_to_merge", vol.PreferNotToMerge, 6)
				xml.XMLEnd(vol.Name, 5)
			}
			xml.XMLWrite("move_factor", policy.MoveFactor, 5)
			xml.XMLEnd("volumes", 4)
			xml.XMLEnd(policy.Name, 3)
		}
		xml.XMLEnd("policies", 2)
	}

	xml.XMLEnd("storage_configuration", 1)
	xml.XMLEnd("yandex", 0)
	err := xml.XMLDump()
	if err != nil {
		return "", err
	}
	return filename, nil
}

func GenerateMacrosXML(filename string, conf *model.CkDeployConfig, host string)(string, error){
	shardIndex := 0
	hostName := ""
	for i, shard := range conf.Shards {
		for _, replica := range shard.Replicas {
			if host == replica.Ip {
				shardIndex = i + 1
				hostName = replica.HostName
				break
			}
		}
	}

	xml := common.NewXmlFile(filename)
	xml.XMLBegin("yandex", 0)
	xml.XMLBegin("macros", 1)
	xml.XMLWrite("cluster", conf.ClusterName, 2)
	xml.XMLWrite("shard", shardIndex, 2)
	xml.XMLWrite("replica", hostName, 2)
	xml.XMLEnd("macros", 1)
	xml.XMLEnd("yandex", 0)
	err := xml.XMLDump()
	if err != nil {
		return "", err
	}
	return filename, nil
}

func GenerateMetrikaXML(filename string, conf *model.CkDeployConfig)(string, error){
	xml := common.NewXmlFile(filename)
	xml.XMLBegin("yandex", 0)
	xml.XMLAppend(GenZookeeperMetrika(conf))
	xml.XMLBegin("clickhouse_remote_servers", 1)
	xml.XMLAppend(GenLocalMetrika(conf))
	xml.XMLEnd("clickhouse_remote_servers", 1)
	xml.XMLEnd("yandex", 0)
	err := xml.XMLDump()
	if err != nil {
		return "", err
	}
	return filename, nil
}

func GenerateMetrikaXMLwithLogic(filename string, conf *model.CkDeployConfig, logicMrtrika string)(string, error){
	xml := common.NewXmlFile(filename)
	xml.XMLBegin("yandex", 0)
	xml.XMLAppend(GenZookeeperMetrika(conf))
	xml.XMLBegin("clickhouse_remote_servers", 1)
	xml.XMLAppend(GenLocalMetrika(conf))
	xml.XMLAppend(logicMrtrika)
	xml.XMLEnd("clickhouse_remote_servers", 1)
	xml.XMLEnd("yandex", 0)
	err := xml.XMLDump()
	if err != nil {
		return "", err
	}
	return filename, nil
}

func GenZookeeperMetrika( conf *model.CkDeployConfig) string {
	xml := common.NewXmlFile("")
	xml.XMLBegin("zookeeper-servers", 1)
	for index, zk := range conf.ZkNodes {
		xml.XMLBeginwithAttr("node",  []common.XMLAttr{{Key:"index", Value:index+1}}, 2)
		xml.XMLWrite("host", zk, 3)
		xml.XMLWrite("port", conf.ZkPort, 3)
		xml.XMLEnd("node", 2)
	}
	xml.XMLEnd("zookeeper-servers", 1)
	return xml.GetContext()
}

func GenLocalMetrika(conf *model.CkDeployConfig)string {
	xml := common.NewXmlFile("")
	xml.XMLBegin(conf.ClusterName, 2)
	for _, shard := range conf.Shards {
		xml.XMLBegin("shard", 3)
		xml.XMLWrite("internal_replication", conf.IsReplica, 4)
		for _, replica := range shard.Replicas {
			xml.XMLBegin("replica", 4)
			xml.XMLWrite("host", replica.HostName, 5)
			xml.XMLWrite("port", conf.CkTcpPort, 5)
			xml.XMLEnd("replica", 4)
		}
		xml.XMLEnd("shard", 3)
	}
	xml.XMLEnd(conf.ClusterName, 2)
	return xml.GetContext()
}

func GenLogicMetrika(d *CKDeploy)(string, []*CKDeploy) {
	var deploys []*CKDeploy
	xml := common.NewXmlFile("")
	xml.XMLBegin(*d.Conf.LogicCluster, 2)
	logics, ok := clickhouse.CkClusters.GetLogicClusterByName(*d.Conf.LogicCluster)
	if ok {
		for _, logic := range logics {
			if logic == d.Conf.ClusterName {
				// if the operation is addNode or deleteNode, we do not use global config
				continue
			}
			c, _ := clickhouse.CkClusters.GetClusterByName(logic)
			deploy := ConvertCKDeploy(&c)
			deploys = append(deploys, deploy)
		}
	}
	deploys = append(deploys, d)
	for _, deploy := range deploys {
		for _, shard := range deploy.Conf.Shards {
			xml.XMLBegin("shard", 3)
			xml.XMLWrite("internal_replication", d.Conf.IsReplica, 4)
			for _, replica := range shard.Replicas {
				xml.XMLBegin("replica", 4)
				xml.XMLWrite("host", replica.Ip, 5)
				xml.XMLWrite("port", d.Conf.CkTcpPort, 5)
				xml.XMLEnd("replica", 4)
			}
			xml.XMLEnd("shard", 3)
		}
	}
	xml.XMLEnd(*d.Conf.LogicCluster, 2)
	return xml.GetContext(), deploys
}