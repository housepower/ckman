package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestXMLFile_XMLWrite(t *testing.T) {
	macros := NewXmlFile("macros.xml")
	macros.Begin("yandex")
	macros.Comment("macros configuration, you can select from system.macros")
	macros.Begin("macros")
	macros.Write("replica", "/clickhouse/tables/1/default/t123/replica")

	macros.Write("shard", 3)
	macros.End("macros")
	macros.End("yandex")
	macros.Dump()
}

func TestXMLFile_Merge(t *testing.T) {
	input := map[string]interface{}{
		"a/b/c":                             "bar",
		"a/b/d":                             "foo",
		"a/e":                               "baz",
		"a/g/k[@id=13]":                     "hello",
		"d":                                 true,
		"title[@lang='en', @size=4]/header": "header123",
		"volumes/disk":                      []string{"hdfs1", "hdfs2", "local"},
		"m/n":                               "[1,2,3,4]",
	}

	xml := NewXmlFile("merge_test.xml")
	xml.Begin("yandex")
	xml.Merge(input)
	xml.End("yandex")
	assert.Nil(t, xml.Dump())
}
