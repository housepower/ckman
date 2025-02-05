package ckconfig

import (
	"fmt"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
	"github.com/imdario/mergo"
)

type HostInfo struct {
	MemoryTotal int
	NumCPU      int
	//Ipv6Enable  bool
}

func users(conf *model.CKManClickHouseConfig) map[string]interface{} {
	output := make(map[string]interface{})
	userConf := make(map[string]interface{})

	//default
	defaultUser := make(map[string]interface{})
	if conf.EncryptType == common.PLAINTEXT {
		defaultUser["password"] = conf.Password
	} else {
		defaultUser["password[@remove=\"remove\"]"] = ""
		defaultUser[common.CkPasswdLabel(conf.EncryptType)] = common.CkPassword(conf.Password, conf.EncryptType)
	}
	defaultUser["profile"] = model.ClickHouseUserProfileDefault
	defaultUser["quota"] = model.ClickHouseUserQuotaDefault
	defaultUser["access_management"] = 1
	defaultNetworks := make(map[string]interface{})
	defaultNetworks["ip"] = model.ClickHouseUserNetIpDefault
	defaultUser["networks"] = defaultNetworks
	userConf["default"] = defaultUser

	//normal users
	if len(conf.UsersConf.Users) > 0 {
		for _, normalUser := range conf.UsersConf.Users {
			normal := make(map[string]interface{})
			normal[common.CkPasswdLabel(normalUser.EncryptType)] = common.CkPassword(normalUser.Password, normalUser.EncryptType)
			normal["profile"] = common.GetStringwithDefault(normalUser.Profile, model.ClickHouseUserProfileDefault)
			normal["quota"] = common.GetStringwithDefault(normalUser.Quota, model.ClickHouseUserQuotaDefault)
			if normalUser.Roles != "" {
				normal["grants"] = map[string]interface{}{
					"query": fmt.Sprintf("GRANT %s", normalUser.Roles),
				}
			}
			networks := make(map[string]interface{})
			if len(normalUser.Networks.IPs) > 0 {
				var ips []string
				for _, ip := range normalUser.Networks.IPs {
					ips = append(ips, common.GetStringwithDefault(ip, model.ClickHouseUserNetIpDefault))
				}
				networks["ip"] = ips
			} else {
				networks["ip"] = model.ClickHouseUserNetIpDefault
			}
			if len(normalUser.Networks.Hosts) > 0 {
				networks["host"] = normalUser.Networks.Hosts
			}
			if len(normalUser.Networks.HostRegexps) > 0 {
				networks["host_regexp"] = normalUser.Networks.HostRegexps
			}
			normal["networks"] = networks

			database := make(map[string]interface{})
			if len(normalUser.DbRowPolices) > 0 {
				for _, rowsdb := range normalUser.DbRowPolices {
					rowpolicies := make(map[string]interface{})
					for _, policy := range rowsdb.TblRowPolicies {
						rowpolicies[policy.Table] = map[string]interface{}{
							"filter": policy.Filter,
						}
					}
					database[rowsdb.Database] = rowpolicies
				}
				normal["databases"] = database
			}
			userConf[normalUser.Name] = normal
		}
	}
	output["users"] = userConf
	return output
}

func profiles(userProfiles []model.Profile, info HostInfo, version string) map[string]interface{} {
	output := make(map[string]interface{})
	profileMap := make(map[string]interface{})
	//default
	defaultProfile := make(map[string]interface{})
	defaultProfile["max_memory_usage"] = int64((info.MemoryTotal / 2) * 1e3)
	defaultProfile["max_memory_usage_for_all_queries"] = int64(((info.MemoryTotal * 3) / 4) * 1e3)
	defaultProfile["max_bytes_before_external_group_by"] = int64((info.MemoryTotal / 4) * 1e3)
	defaultProfile["max_query_size"] = 1073741824
	defaultProfile["distributed_aggregation_memory_efficient"] = 1
	defaultProfile["joined_subquery_requires_alias"] = 0
	defaultProfile["distributed_ddl_task_timeout"] = 60
	defaultProfile["allow_drop_detached"] = 1
	defaultProfile["use_uncompressed_cache"] = 0
	defaultProfile["max_execution_time"] = 3600 // 1 hour
	defaultProfile["max_partitions_per_insert_block"] = 500
	if common.CompareClickHouseVersion("22.5.1.2079", version) > 0 {
		defaultProfile["background_fetches_pool_size"] = common.MaxInt(info.NumCPU/4, 16)
	}
	profileMap["default"] = defaultProfile

	//normal
	if len(userProfiles) > 0 {
		for _, prof := range userProfiles {
			normalProfile := make(map[string]interface{})
			normalProfile["readonly"] = prof.ReadOnly
			normalProfile["allow_ddl"] = prof.AllowDDL
			normalProfile["max_threads"] = prof.MaxThreads
			if prof.MaxMemoryUsage > int64((info.MemoryTotal/2)*1e3) {
				prof.MaxMemoryUsage = int64((info.MemoryTotal / 2) * 1e3)
			}
			normalProfile["max_memory_usage"] = prof.MaxMemoryUsage
			if prof.MaxMemoryUsageForAllQueries > int64(((info.MemoryTotal*3)/4)*1e3) {
				prof.MaxMemoryUsageForAllQueries = int64(((info.MemoryTotal * 3) / 4) * 1e3)
			}
			normalProfile["max_memory_usage_for_all_queries"] = prof.MaxMemoryUsageForAllQueries
			normalProfile["max_execution_time"] = prof.MaxExecutionTime
			mergo.Merge(&normalProfile, expert(prof.Expert))
			profileMap[prof.Name] = normalProfile
		}
	}
	output["profiles"] = profileMap
	return output
}

func quotas(userQuotas []model.Quota) map[string]interface{} {
	output := make(map[string]interface{})
	if len(userQuotas) > 0 {
		quotasMap := make(map[string]interface{})
		for _, quota := range userQuotas {
			quotaInterval := make(map[string]interface{})
			var intervals []map[string]interface{}
			for _, interval := range quota.Intervals {
				intervalMap := make(map[string]interface{})
				intervalMap["duration"] = interval.Duration
				intervalMap["queries"] = interval.Queries
				intervalMap["query_selects"] = interval.QuerySelects
				intervalMap["query_inserts"] = interval.QueryInserts
				intervalMap["errors"] = interval.Errors
				intervalMap["result_rows"] = interval.ResultRows
				intervalMap["read_rows"] = interval.ReadRows
				intervalMap["execution_time"] = interval.ExecutionTime
				intervals = append(intervals, intervalMap)
			}
			quotaInterval["interval"] = intervals
			quotasMap[quota.Name] = quotaInterval
		}
		output["quotas"] = quotasMap
	}
	return output
}

func roles(userRoles []model.Role) map[string]interface{} {
	output := make(map[string]interface{})
	if len(userRoles) > 0 {
		quotasMap := make(map[string]interface{})
		for _, role := range userRoles {
			quotasMap[role.Name] = map[string]interface{}{
				"grants": map[string]interface{}{
					"query": role.Grants.Query,
				},
			}
		}
		output["roles"] = quotasMap
	}
	return output
}

func GenerateUsersXML(filename string, conf *model.CKManClickHouseConfig, info HostInfo) (string, error) {
	rootTag := "yandex"
	if common.CompareClickHouseVersion(conf.Version, "22.x") >= 0 {
		rootTag = "clickhouse"
	}
	userconf := make(map[string]interface{})
	mergo.Merge(&userconf, expert(conf.UsersConf.Expert))
	mergo.Merge(&userconf, users(conf))
	mergo.Merge(&userconf, profiles(conf.UsersConf.Profiles, info, conf.Version))
	mergo.Merge(&userconf, quotas(conf.UsersConf.Quotas))
	mergo.Merge(&userconf, roles(conf.UsersConf.Roles))
	xml := common.NewXmlFile(filename)
	xml.Begin(rootTag)
	xml.Merge(userconf)
	xml.End(rootTag)
	if err := xml.Dump(); err != nil {
		return filename, err
	}
	return filename, nil
}
