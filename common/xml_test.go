package common

import "testing"

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
