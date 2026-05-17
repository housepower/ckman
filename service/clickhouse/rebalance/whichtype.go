package rebalance

import (
	"fmt"
	"strings"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

var typeInfo map[string]model.TypeInfo

// GetTypeName returns the canonical ClickHouse column type name for an internal
// type discriminator.
func GetTypeName(typ int) string {
	switch typ {
	case Bool:
		return "Bool"
	case Int8:
		return "Int8"
	case Int16:
		return "Int16"
	case Int32:
		return "Int32"
	case Int64:
		return "Int64"
	case UInt8:
		return "UInt8"
	case UInt16:
		return "UInt16"
	case UInt32:
		return "UInt32"
	case UInt64:
		return "UInt64"
	case Float32:
		return "Float32"
	case Float64:
		return "Float64"
	case Decimal:
		return "Decimal"
	case DateTime:
		return "DateTime"
	case String:
		return "String"
	default:
		return "Unknown"
	}
}

// WhichType maps a ClickHouse column type string (possibly wrapped in
// Nullable() or Array()) to the rebalance package's internal discriminator,
// which the sharding hash selection then consumes.
func WhichType(typ string) model.TypeInfo {
	if ti, ok := typeInfo[typ]; ok {
		return ti
	}
	origTyp := typ
	var t model.TypeInfo
	t.Nullable = strings.HasPrefix(typ, "Nullable(")
	t.Array = strings.HasPrefix(typ, "Array(")
	if t.Nullable {
		typ = typ[len("Nullable(") : len(typ)-1]
	} else if t.Array {
		typ = typ[len("Array(") : len(typ)-1]
	}
	switch {
	case strings.HasPrefix(typ, "DateTime64"):
		t.Type = DateTime
	case strings.HasPrefix(typ, "Decimal"):
		t.Type = Decimal
	case strings.HasPrefix(typ, "FixedString"):
		t.Type = String
	case strings.HasPrefix(typ, "Enum8("), strings.HasPrefix(typ, "Enum16("):
		t.Type = String
	default:
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
		typeInfo[fmt.Sprintf("Nullable(%s)", tn)] = model.TypeInfo{Type: t, Nullable: true}
		typeInfo[fmt.Sprintf("Array(%s)", tn)] = model.TypeInfo{Type: t, Array: true}
	}
	typeInfo["UUID"] = model.TypeInfo{Type: String}
	typeInfo["Nullable(UUID)"] = model.TypeInfo{Type: String, Nullable: true}
	typeInfo["Array(UUID)"] = model.TypeInfo{Type: String, Array: true}
	typeInfo["Date"] = model.TypeInfo{Type: DateTime}
	typeInfo["Nullable(Date)"] = model.TypeInfo{Type: DateTime, Nullable: true}
	typeInfo["Array(Date)"] = model.TypeInfo{Type: DateTime, Array: true}
}
