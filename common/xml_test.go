package common

import "testing"

func TestXMLFile_XMLWrite(t *testing.T) {
	macros := NewXmlFile("macros.xml")
	macros.XMLBegin("yandex", 0)
	macros.XMLComment("macros configuration, you can select from system.macros", 1)
	macros.XMLBegin("macros", 1)
	macros.XMLWrite("replica", "/clickhouse/tables/1/default/t123/replica", 2)

	macros.XMLWrite("shard", 3, 2)
	macros.XMLEnd("macros", 1)
	macros.XMLEnd("yandex", 0)
	macros.XMLDump()
}
