package ckconfig

import (
	"testing"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
	"github.com/stretchr/testify/assert"
)

func TestUsers(t *testing.T) {
	user := []model.User{
		{
			Name:     "eoitek",
			Password: "123456",
			Quota:    "readonly",
			Profile:  "default",
			Networks: model.Networks{
				IPs:         []string{"192.168.110.8", "192.168.110.10", "192.168.110.12"},
				Hosts:       []string{"ck08", "ck10", "ck12"},
				HostRegexps: []string{"^example\\d\\d-\\d\\d-\\d\\.host\\.ru$"},
			},
			DbRowPolices: []model.DbRowPolicy{
				{
					Database: "test",
					TblRowPolicies: []model.TblRowPolicy{
						{"tbl_t1", "id > 100"},
						{"tbl_t2", "id > 5000"},
					}},
				{
					Database: "itoa",
					TblRowPolicies: []model.TblRowPolicy{
						{"tbl_t3", "id <= 30000"},
					}},
			},
		},
		{
			Name:     "test",
			Password: "123123",
		}, {
			Name:     "abc",
			Password: "1234",
		},
	}
	conf := model.CKManClickHouseConfig{
		Password:    "qaz-wsx",
		EncryptType: common.SHA256_HEX,
		UsersConf: model.UsersConf{
			Users: user,
		},
	}

	xml := common.NewXmlFile("users_fake.xml")
	xml.Begin("clickhouse")
	xml.Merge(users(&conf))
	xml.End("clickhouse")
	err := xml.Dump()
	assert.Nil(t, err)
}

func TestProfiles(t *testing.T) {
	info := HostInfo{
		MemoryTotal: 34359738368,
	}

	profile := []model.Profile{
		{
			Name:                        "readwrite",
			ReadOnly:                    1,
			AllowDDL:                    0,
			MaxThreads:                  32,
			MaxMemoryUsage:              2147483648,
			MaxMemoryUsageForAllQueries: 2576980377,
			MaxExecutionTime:            30,
		},
		{
			Name: "readonly",
			Expert: map[string]string{
				"readonly":    "0",
				"allow_ddl":   "1",
				"max_threads": "16",
			},
		},
	}

	xml := common.NewXmlFile("profiles_fake.xml")
	xml.Begin("clickhouse")
	xml.Merge(profiles(profile, info, "23.8.9.54"))
	xml.End("clickhouse")
	err := xml.Dump()
	assert.Nil(t, err)
}

func TestQuotas(t *testing.T) {
	quota := []model.Quota{
		{
			Name: "foo",
			Intervals: []model.Interval{
				{
					Duration:      3600,
					Queries:       0,
					QuerySelects:  0,
					QueryInserts:  0,
					Errors:        0,
					ResultRows:    0,
					ReadRows:      0,
					ExecutionTime: 0,
				},
				{
					Duration:      86400,
					Queries:       100000000,
					QuerySelects:  100000000,
					QueryInserts:  100000000,
					Errors:        10000,
					ResultRows:    99999999,
					ReadRows:      100000000,
					ExecutionTime: 10000,
				},
			},
		},
		{
			Name: "bar",
			Intervals: []model.Interval{
				{
					Duration:      3600,
					Queries:       0,
					QuerySelects:  0,
					QueryInserts:  0,
					Errors:        0,
					ResultRows:    0,
					ReadRows:      0,
					ExecutionTime: 0,
				},
				{
					Duration:      86400,
					Queries:       100000000,
					QuerySelects:  100000000,
					QueryInserts:  100000000,
					Errors:        10000,
					ResultRows:    99999999,
					ReadRows:      100000000,
					ExecutionTime: 10000,
				},
			},
		},
	}

	xml := common.NewXmlFile("quotas_fake.xml")
	xml.Begin("clickhouse")
	xml.Merge(quotas(quota))
	xml.End("clickhouse")
	err := xml.Dump()
	assert.Nil(t, err)
}

func TestUsersXML(t *testing.T) {
	GenerateUsersXML("users.xml", &model.CKManClickHouseConfig{
		Password:    "123456",
		EncryptType: common.DOUBLE_SHA1_HEX,
		UsersConf: model.UsersConf{
			Users: []model.User{
				{
					Name:     "eoitek",
					Password: "123456",
					Quota:    "readonly",
					Profile:  "default",
					Networks: model.Networks{
						IPs:         []string{"192.168.110.8", "192.168.110.10", "192.168.110.12"},
						Hosts:       []string{"ck08", "ck10", "ck12"},
						HostRegexps: []string{"^example\\d\\d-\\d\\d-\\d\\.host\\.ru$"},
					},
					DbRowPolices: []model.DbRowPolicy{
						{
							Database: "test",
							TblRowPolicies: []model.TblRowPolicy{
								{"tbl_t1", "id > 100"},
								{"tbl_t2", "id > 5000"},
							},
						},
						{
							Database: "itoa",
							TblRowPolicies: []model.TblRowPolicy{
								{"tbl_t3", "id <= 30000"},
							},
						},
					},
				},
				{
					Name:     "test",
					Password: "123123",
				},
			},
			Profiles: []model.Profile{
				{
					Name:                        "readwrite",
					ReadOnly:                    1,
					AllowDDL:                    0,
					MaxThreads:                  32,
					MaxMemoryUsage:              2147483648,
					MaxMemoryUsageForAllQueries: 2576980377,
					MaxExecutionTime:            30,
				},
				{
					Name: "readonly",
					Expert: map[string]string{
						"readonly":    "0",
						"allow_ddl":   "1",
						"max_threads": "16",
					},
				},
			},
			Quotas: []model.Quota{
				{
					Name: "foo",
					Intervals: []model.Interval{
						{
							Duration:      3600,
							Queries:       0,
							QuerySelects:  0,
							QueryInserts:  0,
							Errors:        0,
							ResultRows:    0,
							ReadRows:      0,
							ExecutionTime: 0,
						},
						{
							Duration:      86400,
							Queries:       100000000,
							QuerySelects:  100000000,
							QueryInserts:  100000000,
							Errors:        10000,
							ResultRows:    99999999,
							ReadRows:      100000000,
							ExecutionTime: 10000,
						},
					},
				},
				{
					Name: "bar",
					Intervals: []model.Interval{
						{
							Duration:      3600,
							Queries:       0,
							QuerySelects:  0,
							QueryInserts:  0,
							Errors:        0,
							ResultRows:    0,
							ReadRows:      0,
							ExecutionTime: 0,
						},
						{
							Duration:      86400,
							Queries:       100000000,
							QuerySelects:  100000000,
							QueryInserts:  100000000,
							Errors:        10000,
							ResultRows:    99999999,
							ReadRows:      100000000,
							ExecutionTime: 10000,
						},
					},
				},
			},
			Roles: []model.Role{
				model.Role{
					Name: "eoitek",
					Grants: model.Grants{
						Query: []string{
							"GRANT SHOW ON *.*",
							"GRANT CREATE ON *.* WITH GRANT OPTION",
							"GRANT SELECT ON system.*",
						},
					},
				},
			},
			Expert: map[string]string{
				"profiles/default/max_partitions_insert_block": "500",
			},
		},
	}, HostInfo{
		MemoryTotal: 1046576000,
	})
}
