package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertDisk(t *testing.T) {
	assert.Equal(t, "388.00B", ConvertDisk(388))
	assert.Equal(t, "48.21KB", ConvertDisk(49367))
	assert.Equal(t, "231.99MB", ConvertDisk(243255465))
	assert.Equal(t, "33.73GB", ConvertDisk(36212124557))
	assert.Equal(t, "1.23TB", ConvertDisk(1352446565432))
	assert.Equal(t, "29.60PB", ConvertDisk(33323244132400112))
}

func TestArrayDistinct(t *testing.T) {
	arr := []string{"test1", "test1", "test2", "test3", "test2", "test1"}
	out := ArrayDistinct(arr)
	assert.Equal(t, out, []string{"test1", "test2", "test3"})
}

func TestFormatReadableTime(t *testing.T) {
	fmt.Println(FormatReadableTime(26))
	fmt.Println(FormatReadableTime(68))
	fmt.Println(FormatReadableTime(0))
	fmt.Println(FormatReadableTime(60))
	fmt.Println(FormatReadableTime(262))
	fmt.Println(FormatReadableTime(3593))
	fmt.Println(FormatReadableTime(3600))
	fmt.Println(FormatReadableTime(3604))
	fmt.Println(FormatReadableTime(77435))
	fmt.Println(FormatReadableTime(86400))
	fmt.Println(FormatReadableTime(86402))
	fmt.Println(FormatReadableTime(858389))
}

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
