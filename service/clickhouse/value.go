package clickhouse

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

const (
	Unknown = iota
	Bool
	Int8
	Int16
	Int32
	Int64
	UInt8
	UInt16
	UInt32
	UInt64
	Float32
	Float64
	Decimal
	DateTime
	String
)

var (
	typeInfo map[string]model.TypeInfo
)

// GetTypeName returns the column type in ClickHouse
func GetTypeName(typ int) (name string) {
	switch typ {
	case Bool:
		name = "Bool"
	case Int8:
		name = "Int8"
	case Int16:
		name = "Int16"
	case Int32:
		name = "Int32"
	case Int64:
		name = "Int64"
	case UInt8:
		name = "UInt8"
	case UInt16:
		name = "UInt16"
	case UInt32:
		name = "UInt32"
	case UInt64:
		name = "UInt64"
	case Float32:
		name = "Float32"
	case Float64:
		name = "Float64"
	case Decimal:
		name = "Decimal"
	case DateTime:
		name = "DateTime"
	case String:
		name = "String"
	default:
		name = "Unknown"
	}
	return
}

func WhichType(typ string) model.TypeInfo {
	var t model.TypeInfo
	ti, ok := typeInfo[typ]
	if ok {
		return ti
	}
	origTyp := typ
	t.Nullable = strings.HasPrefix(typ, "Nullable(")
	t.Array = strings.HasPrefix(typ, "Array(")
	if t.Nullable {
		typ = typ[len("Nullable(") : len(typ)-1]
	} else if t.Array {
		typ = typ[len("Array(") : len(typ)-1]
	}
	if strings.HasPrefix(typ, "DateTime64") {
		t.Type = DateTime
	} else if strings.HasPrefix(typ, "Decimal") {
		t.Type = Decimal
	} else if strings.HasPrefix(typ, "FixedString") {
		t.Type = String
	} else if strings.HasPrefix(typ, "Enum8(") {
		t.Type = String
	} else if strings.HasPrefix(typ, "Enum16(") {
		t.Type = String
	} else {
		log.Logger.Fatal(fmt.Sprintf("ClickHouse column type %v is not inside supported ones: %v", origTyp, typeInfo))
	}
	typeInfo[origTyp] = t
	return t
}

func init() {
	typeInfo = make(map[string]model.TypeInfo)
	for _, t := range []int{Bool, Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, DateTime, String} {
		tn := GetTypeName(t)
		typeInfo[tn] = model.TypeInfo{Type: t}
		nullTn := fmt.Sprintf("Nullable(%s)", tn)
		typeInfo[nullTn] = model.TypeInfo{Type: t, Nullable: true}
		arrTn := fmt.Sprintf("Array(%s)", tn)
		typeInfo[arrTn] = model.TypeInfo{Type: t, Array: true}
	}
	typeInfo["UUID"] = model.TypeInfo{Type: String}
	typeInfo["Nullable(UUID)"] = model.TypeInfo{Type: String, Nullable: true}
	typeInfo["Array(UUID)"] = model.TypeInfo{Type: String, Array: true}
	typeInfo["Date"] = model.TypeInfo{Type: DateTime}
	typeInfo["Nullable(Date)"] = model.TypeInfo{Type: DateTime, Nullable: true}
	typeInfo["Array(Date)"] = model.TypeInfo{Type: DateTime, Array: true}
}

func ShardingFunc(k model.RebalanceShardingkey) string {
	var f string
	switch k.ShardingType.Type {
	case Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, Decimal, DateTime:
		f = fmt.Sprintf("CAST(`%s`, 'UInt64')", k.ShardingKey)
	case String:
		f = fmt.Sprintf("xxHash64(`%s`)", k.ShardingKey)
	}
	return f
}

func GetColsPointer(ctp driver.ColumnType) interface{} {
	switch ctp.ScanType().Kind() {
	case reflect.Bool:
		var col bool
		return col
	case reflect.Int8:
		var col int8
		return col
	case reflect.Int16:
		var col int16
		return col
	case reflect.Int32:
		var col int32
		return col
	case reflect.Int64:
		var col int64
		return col
	case reflect.Uint8:
		var col uint8
		return col
	case reflect.Uint16:
		var col uint16
		return col
	case reflect.Uint32:
		var col uint32
		return col
	case reflect.Uint64:
		var col uint64
		return col
	case reflect.Float32:
		var col float32
		return col
	case reflect.Float64:
		var col float64
		return col
	case reflect.String:
		var col string
		return col
	default:
		var col interface{}
		return col
	}
}
