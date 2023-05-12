package common

import (
	"fmt"
	"os"
	"testing"
	"time"

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

func TestPackages(t *testing.T) {
	pkgs := GetAllPackages()
	assert.Equal(t, 0, len(pkgs))
}

func TestGetWorkDirectory(t *testing.T) {
	fmt.Println(GetWorkDirectory())
}

func TestPassword(t *testing.T) {
	assert.NotNil(t, VerifyPassword("123456"))
	assert.NotNil(t, VerifyPassword("123456789"))
	assert.NotNil(t, VerifyPassword("abcdefghijk"))
	assert.NotNil(t, VerifyPassword("???##@<<>>}{}"))
	assert.Nil(t, VerifyPassword("Ckman123456!"))

	assert.Equal(t, true, ComparePassword("$2a$10$T5zSwqNoJonoMNvt91zBIOkKgAo4htPFLANHmoJsHT6Sz4vpXNePy", "63cb91a2ceb9d4f7c8b1ba5e50046f52"))
	fmt.Println(HashPassword("Ckman123456!"))
}

func TestEnvVar(t *testing.T) {
	os.Setenv("TESTSTR", "ckman")
	os.Setenv("TESTBOOL", "true")
	os.Setenv("TESTINT", "8808")
	var s string
	var b bool
	var i int
	EnvStringVar(&s, "TESTSTR")
	EnvBoolVar(&b, "TESTBOOL")
	EnvIntVar(&i, "TESTINT")
	assert.Equal(t, "ckman", s)
	assert.Equal(t, true, b)
	assert.Equal(t, 8808, i)
}

func TestTempFile(t *testing.T) {
	tf, err := NewTempFile("/tmp", "unit_test")
	fmt.Println(tf.BaseName, tf.FullName)
	assert.Nil(t, err)
	_, err = os.Stat(tf.FullName)
	assert.Nil(t, err)
	os.RemoveAll(tf.FullName)
}

func TestDeepCopyByGob(t *testing.T) {
	type A struct {
		A1 int
		A2 string
		A3 map[string]string
		A4 struct {
			B1 int
			B2 bool
		}
		A5 *string
	}

	a5 := "world"
	a := A{
		A1: 10,
		A2: "hello",
		A3: map[string]string{
			"k1": "v1",
			"k2": "v2",
		},
		A4: struct {
			B1 int
			B2 bool
		}{
			B1: 20,
			B2: true,
		},
		A5: &a5,
	}
	var b A
	err := DeepCopyByGob(&b, &a)
	assert.Nil(t, err)
	assert.Equal(t, a.A1, b.A1)
	assert.Equal(t, a.A2, b.A2)
	assert.Equal(t, a.A3["k1"], b.A3["k1"])
	assert.Equal(t, a.A3["k2"], b.A3["k2"])
	assert.Equal(t, a.A4.B1, b.A4.B1)
	assert.Equal(t, a.A4.B2, b.A4.B2)
	assert.Equal(t, *a.A5, *b.A5)
}

func TestGetStringwithDefault(t *testing.T) {
	assert.Equal(t, "a", GetStringwithDefault("", "a"))
	assert.Equal(t, "b", GetStringwithDefault("b", "a"))
}

func TestGetIntegerwithDefault(t *testing.T) {
	assert.Equal(t, 100, GetIntegerwithDefault(0, 100))
	assert.Equal(t, 300, GetIntegerwithDefault(300, 100))
}

func TestTernaryExpression(t *testing.T) {
	assert.Equal(t, "yes", TernaryExpression(true, "yes", "no").(string))
	assert.Equal(t, false, TernaryExpression(1 > 2, true, false).(bool))
}

func TestGetOutboundIP(t *testing.T) {
	assert.NotEqual(t, "127.0.0.1", GetOutboundIP().String())
}

func TestConvertDuration(t *testing.T) {
	start, err := time.Parse("2006-01-02T15:04:05", "2006-01-02T15:04:05")
	assert.Nil(t, err)
	end, err := time.Parse("2006-01-02T15:04:05", "2006-01-17T18:28:56")
	assert.Nil(t, err)
	end2, err := time.Parse("2006-01-02T15:04:05", "2006-01-02T18:04:56")
	assert.Nil(t, err)
	assert.Equal(t, "363h 24m 51s", ConvertDuration(start, end))
	assert.Equal(t, "0s", ConvertDuration(start, start))
	assert.Equal(t, "3h 0m 51s", ConvertDuration(start, end2))
}

func TestReplaceTemplateString(t *testing.T) {
	str := "[{\"outputParams\":{\"port\":7070,\"max_client\":10,\"hosts\":[\"{{.router_hosts}}\"],\"fields\":{\"@topic\":\"flow_test2020\",\"@ip\":\"{{.agent_ip}}\"}},\"outputType\":\"prom.lumberjack.output\"}]"
	replace := make(map[string]interface{})
	replace["router_hosts"] = "192.168.31.28"
	replace["agent_ip"] = "192.168.32.37"
	err := ReplaceTemplateString(&str, replace)
	assert.Nil(t, err)
	expect := `[{"outputParams":{"port":7070,"max_client":10,"hosts":["192.168.31.28"],"fields":{"@topic":"flow_test2020","@ip":"192.168.32.37"}},"outputType":"prom.lumberjack.output"}]`
	assert.Equal(t, expect, str)
}

func TestShuffle(t *testing.T) {
	lists := []string{"aaa", "bbb", "ccc", "ddd"}
	fmt.Println(Shuffle(lists))
}
