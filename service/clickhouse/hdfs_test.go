package clickhouse_test

import (
	_ "github.com/ClickHouse/clickhouse-go"
)

// func TestHDFS(t *testing.T) {
// 	ops := hdfs.ClientOptions{
// 		Addresses: []string{"sea.hub:8020"},
// 		User:      "hdfs",
// 	}
// 	hc, err := hdfs.NewClient(ops)
// 	assert.Nil(t, err)
// 	entry, err := hc.ReadDir("/")
// 	assert.Nil(t, err)
// 	for _, e := range entry {
// 		fmt.Println(e.Mode(), e.Name(), e.Size())
// 	}
// }
