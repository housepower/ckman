package clickhouse_test

import (
	"fmt"
	"testing"

	"github.com/colinmarc/hdfs/v2"
	"github.com/stretchr/testify/assert"
)

func TestHDFS(t *testing.T) {
	ops := hdfs.ClientOptions{
		Addresses: []string{"sea.hub:8020"},
		User:      "hdfs",
	}
	hc, err := hdfs.NewClient(ops)
	assert.Nil(t, err)
	entry, err := hc.ReadDir("/")
	assert.Nil(t, err)
	for _, e := range entry {
		fmt.Println(e.Mode(), e.Name(), e.Size())
	}
}
