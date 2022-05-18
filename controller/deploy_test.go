package controller

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEnsurePathNonPrefix(t *testing.T) {
	localPaths := []string{
		"/data01/ck1/",
		"/data01/ck2/",
		"/data01/",
		"/data/clickhouse/",
		"/var/lib/",
	}
	err := EnsurePathNonPrefix(localPaths)
	assert.NotNil(t, err)
	fmt.Println(err)
	hdfsEndpoints := []string{
		"hdfs://clickhouse/data123/",
		"hdfs://clickhouse/data1/",
		"hdfs://clickhouse/data3/",
		"hdfs://clickhouse/",
	}
	err = EnsurePathNonPrefix(hdfsEndpoints)
	assert.NotNil(t, err)
	fmt.Println(err)
	paths := []string{
		"/data01/clickhouse01/",
		"/data02/",
		"/data01/clickhouse03/",
	}
	err = EnsurePathNonPrefix(paths)
	assert.Nil(t, err)
	path1 := []string{
		"/var/lib/",
		"/var/lib/",
	}
	err = EnsurePathNonPrefix(path1)
	assert.NotNil(t, err)
	fmt.Println(err)
	path2 := []string{}
	err = EnsurePathNonPrefix(path2)
	assert.Nil(t, err)
	path3 := []string{"/var/lib/"}
	err = EnsurePathNonPrefix(path3)
	assert.Nil(t, err)
}
