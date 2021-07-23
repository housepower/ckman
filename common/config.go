package common

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Considering the following scenario:
// There are multiple webpages to manage(CRUD) the complex configuration of a backend subsystem.
// Each webpage needs a different view of the configuration. For example, some items are auto filled at creation, and not allowed to modify after then.
// There is many repeated code if we define a struct for real config and each API.
// What about we define only one struct?
// How each API gets a different view of the same struct?
// How frontend webpage avoiding hard-code config items those need to be displayed and modified?

// Here's a solution of frontend-backend decoulping
// - Backend defines only one struct
// - Backend annotates all items on how frontend handle them, and defines a group for each view.
// - Backend provides an API to return the schema of configuration view for each veiw.
// - For each API, Frontend gets the view schema and current view value. Users interact with frontend according to the schema.

// Parameter - Name: field name
type Parameter struct {
	Label              string
	Description        string
	Candidates         []string
	DefaultValue       string
	Range              string
	AvailableCondition string //empty means: true(available)
	RequiredCondition  string //empty means: true(required) iff field type is Ptr
}

func MarshalConfigSchema(v interface{}, params map[string]*Parameter) (data string, err error) {
	rt := reflect.TypeOf(v)
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	var sb strings.Builder
	sb.WriteString("{")
	prefix := rt.PkgPath() + "." + rt.Name() + "."
	var num_fields int
	for i := 0; i < rt.NumField(); i++ {
		rtf := rt.Field(i)
		key := prefix + rtf.Name
		if param, ok := params[key]; ok {
			if num_fields != 0 {
				sb.WriteString(", ")
			}
			num_fields++
			sb.WriteString(strconv.Quote(rtf.Name) + ": ")
			if err = marshalSchemaRecursive(params, rtf.Type, param, &sb); err != nil {
				return
			}
		}
	}
	if num_fields <= 0 {
		err = errors.Errorf("Type %s.%s has no fields registered as a paratemter", rt.PkgPath(), rt.Name())
		return
	}
	sb.WriteString("}")
	data = sb.String()
	return
}

func getType(rt reflect.Type) (str_type string, typ_struct reflect.Type, err error) {
	var has_elem bool
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	switch rt.Kind() {
	case reflect.Bool:
		str_type = "bool"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		str_type = "number"
	case reflect.String:
		str_type = "string"
	case reflect.Map:
		str_type = "map"
		elem := rt.Elem()
		if elem.Kind() != reflect.String {
			err = errors.Errorf("Type %s.%s is map-struct, unsupported", rt.PkgPath(), rt.Name())
			return
		}
	case reflect.Array, reflect.Slice:
		str_type = "list"
		has_elem = true
	case reflect.Struct:
		str_type = "struct"
		typ_struct = rt
	default:
		err = errors.Errorf("Type %s.%s kind %s, unsupported", rt.PkgPath(), rt.Name(), rt.Kind().String())
		return
	}
	if has_elem {
		var str_type2 string
		elem := rt.Elem()
		if str_type2, typ_struct, err = getType(elem); err != nil {
			return
		}
		str_type += "-" + str_type2
	}
	return
}

func nullableString(s string) string {
	if s == "" {
		return "null"
	}
	return strconv.Quote(s)
}

func marshalSchemaRecursive(params map[string]*Parameter, rt reflect.Type, param *Parameter, sb *strings.Builder) (err error) {
	sb.WriteByte(byte('{'))
	sb.WriteString(`"label": `)
	sb.WriteString(nullableString(param.Label))
	sb.WriteString(`, "description": `)
	sb.WriteString(nullableString(param.Description))
	if len(param.Candidates) == 0 {
		sb.WriteString(`, "candinates": null`)
	} else {
		sb.WriteString(`, "candinates": [`)
		for i, cand := range param.Candidates {
			if i != 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(nullableString(cand))
		}
		sb.WriteByte(byte(']'))
	}
	sb.WriteString(`, "defaultValue": `)
	sb.WriteString(nullableString(param.DefaultValue))
	sb.WriteString(`, "range": `)
	sb.WriteString(nullableString(param.Range))
	sb.WriteString(`, "availableCondition": `)
	if param.AvailableCondition == "" {
		param.AvailableCondition = "true"
	}
	sb.WriteString(nullableString(param.AvailableCondition))
	if param.RequiredCondition == "" {
		if rt.Kind() == reflect.Ptr {
			param.RequiredCondition = "false"
		} else {
			param.RequiredCondition = "true"
		}
	}
	sb.WriteString(`, "requireCondition": `)
	sb.WriteString(nullableString(param.RequiredCondition))
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	var str_type string
	var typ_struct reflect.Type
	if str_type, typ_struct, err = getType(rt); err != nil {
		return
	}
	sb.WriteString(`, "type": `)
	sb.WriteString(nullableString(str_type))
	if typ_struct != nil {
		sb.WriteString(`, "struct": {`)
		prefix := typ_struct.PkgPath() + "." + typ_struct.Name() + "."
		var num_fields int
		for i := 0; i < typ_struct.NumField(); i++ {
			rtf := typ_struct.Field(i)
			key := prefix + rtf.Name
			if param, ok := params[key]; ok {
				if num_fields != 0 {
					sb.WriteString(", ")
				}
				num_fields++
				sb.WriteString(strconv.Quote(rtf.Name) + ": ")
				if err = marshalSchemaRecursive(params, rtf.Type, param, sb); err != nil {
					return
				}
			}
		}
		if num_fields <= 0 {
			err = errors.Errorf("Cannot use %s.%s as a paratemter", typ_struct.PkgPath(), typ_struct.Name())
			return
		}
		sb.WriteByte(byte('}'))
	}
	sb.WriteByte(byte('}'))
	return
}

// Inspired by https://stackoverflow.com/a/31302688/319936
func MarshalConfig(v interface{}, params map[string]*Parameter) (data string, err error) {
	rv := reflect.ValueOf(v)
	rt := reflect.TypeOf(v)
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
		rv = rv.Elem()
	}
	size := rv.NumField()
	var bs strings.Builder
	bs.WriteByte(byte('{'))
	prefix := rt.PkgPath() + "." + rt.Name() + "."
	var num_fields int
	for i := 0; i < size; i++ {
		rvf := rv.Field(i)
		fieldName := rt.Field(i).Name
		key := prefix + fieldName
		if _, ok := params[key]; ok {
			if num_fields != 0 {
				bs.WriteString(", ")
			}
			num_fields++
			bs.WriteString(strconv.Quote(fieldName) + ": ")
			if err = marshalConfigRecursive(params, rvf, &bs); err != nil {
				return
			}
		}
	}
	if num_fields <= 0 {
		err = errors.Errorf("Cannot use %s.%s as a paratemter", rt.PkgPath(), rt.Name())
		return
	}
	bs.WriteByte(byte('}'))
	data = bs.String()
	return
}

func marshalConfigRecursive(params map[string]*Parameter, rv reflect.Value, sb *strings.Builder) (err error) {
	var rt reflect.Type
	for {
		if !rv.IsValid() {
			sb.WriteString("null")
			return
		}
		rt = rv.Type()
		if rt.Kind() != reflect.Ptr {
			break
		}
		rv = rv.Elem()
	}
	switch rt.Kind() {
	case reflect.Bool:
		sb.WriteString(strconv.FormatBool(rv.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		sb.WriteString(strconv.FormatInt(rv.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		sb.WriteString(strconv.FormatUint(rv.Uint(), 10))
	case reflect.Float32, reflect.Float64:
		sb.WriteString(strconv.FormatFloat(rv.Float(), byte('e'), 3, 64))
	case reflect.String:
		sb.WriteString(strconv.Quote(rv.String()))
	case reflect.Array, reflect.Slice:
		sb.WriteByte(byte('['))
		num := rv.Len()
		for i := 0; i < num; i++ {
			if i != 0 {
				sb.WriteString(", ")
			}
			if err = marshalConfigRecursive(params, rv.Index(i), sb); err != nil {
				return
			}
		}
		sb.WriteByte(byte(']'))
	case reflect.Map:
		sb.WriteByte(byte('{'))
		mi := rv.MapRange()
		is_first := true
		for mi.Next() {
			if !is_first {
				sb.WriteString(", ")
			} else {
				is_first = false
			}
			sb.WriteString(strconv.Quote(mi.Key().String()))
			sb.WriteString(": ")
			if err = marshalConfigRecursive(params, mi.Value(), sb); err != nil {
				return
			}
		}
		sb.WriteByte(byte('}'))
	case reflect.Struct:
		sb.WriteByte(byte('{'))
		prefix := rt.PkgPath() + "." + rt.Name() + "."
		var num_fields int
		for i := 0; i < rv.NumField(); i++ {
			rtf := rt.Field(i)
			rvf := rv.FieldByName(rtf.Name)
			key := prefix + rtf.Name
			if _, ok := params[key]; ok {
				if num_fields != 0 {
					sb.WriteString(", ")
				}
				num_fields++
				sb.WriteString(strconv.Quote(rtf.Name) + ": ")
				if err = marshalConfigRecursive(params, rvf, sb); err != nil {
					return
				}
			}
		}
		sb.WriteByte(byte('}'))
	default:
		err = errors.Errorf("Cannot use %s.%s as a paratemter", rt.PkgPath(), rt.Name())
		return
	}
	return
}

func CompareConfig(v1, v2 interface{}, params map[string]*Parameter) (equals bool, first_diff string) {
	equals, first_diff = compareConfigRecursive(v1, v2, params, "")
	return
}

func compareConfigRecursive(v1, v2 interface{}, params map[string]*Parameter, json_path string) (equals bool, first_diff string) {
	rv1 := reflect.ValueOf(v1)
	rt1 := reflect.TypeOf(v1)
	for rt1.Kind() == reflect.Ptr {
		rt1 = rt1.Elem()
		rv1 = rv1.Elem()
	}
	rv2 := reflect.ValueOf(v2)
	rt2 := reflect.TypeOf(v2)
	for rt2.Kind() == reflect.Ptr {
		rt2 = rt2.Elem()
		rv2 = rv2.Elem()
	}
	if rt1 != rt2 {
		first_diff = fmt.Sprintf("json path %s, left type %s.%s, right type %s.%s", json_path, rt1.PkgPath(), rt1.Name(), rt2.PkgPath(), rt2.Name())
		return
	}
	if rv1.IsValid() != rv2.IsValid() {
		first_diff = fmt.Sprintf("json_path %s, Isvalid differ, left %v, right %v", json_path, rv1.IsValid(), rv2.IsValid())
		return
	}
	if !rv1.IsValid() {
		equals = true
		return
	}

	switch rt1.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.String:
		if !reflect.DeepEqual(rv1.Interface(), rv2.Interface()) {
			first_diff = fmt.Sprintf("json_path %s, values differ, %v != %v", json_path, rv1.Interface(), rv2.Interface())
			return
		}
	case reflect.Array, reflect.Slice:
		if rv1.Len() != rv2.Len() {
			first_diff = fmt.Sprintf("json_path %s, list lengths differ, %v != %v", json_path, rv1.Len(), rv2.Len())
			return
		}
		for i := 0; i < rv1.Len(); i++ {
			if equals, first_diff = compareConfigRecursive(rv1.Index(i), rv2.Index(i), params, fmt.Sprintf("%s[%d]", json_path, i)); !equals {
				return
			}
		}
	case reflect.Map:
		fields1 := make(map[string]reflect.Value)
		fields2 := make(map[string]reflect.Value)
		mi1 := rv1.MapRange()
		for mi1.Next() {
			fields1[mi1.Key().String()] = mi1.Value()
		}
		mi2 := rv2.MapRange()
		for mi2.Next() {
			fields2[mi2.Key().String()] = mi2.Value()
		}
		if equals, first_diff = compareMap(fields1, fields2, params, json_path); !equals {
			return
		}
	case reflect.Struct:
		if rv1.IsValid() != rv2.IsValid() {
			first_diff = fmt.Sprintf("json_path %s, struct pointer Isvalid differ, %v != %v", json_path, rv1.IsValid(), rv2.IsValid())
			return
		}
		if rv1.IsValid() {
			fields1 := getStructFields(rv1, params)
			fields2 := getStructFields(rv2, params)
			if equals, first_diff = compareMap(fields1, fields2, params, json_path); !equals {
				return
			}
		}
	}
	equals = true
	return
}

func getStructFields(rv reflect.Value, params map[string]*Parameter) (fields map[string]reflect.Value) {
	rt := rv.Type()
	fields = make(map[string]reflect.Value)
	prefix := rt.PkgPath() + "." + rt.Name() + "."
	for i := 0; i < rv.NumField(); i++ {
		rtf := rt.Field(i)
		rvf := rv.FieldByName(rtf.Name)
		key := prefix + rtf.Name
		if _, ok := params[key]; ok {
			fields[rtf.Name] = rvf
		}
	}
	return
}

func compareMap(m1, m2 map[string]reflect.Value, params map[string]*Parameter, json_path string) (equals bool, first_diff string) {
	if len(m1) != len(m2) {
		first_diff = fmt.Sprintf("json_path %s, number of elements differ, %v != %v", json_path, len(m1), len(m2))
		return
	}
	for fld_name, fld1 := range m1 {
		fld2, ok := m2[fld_name]
		if !ok {
			first_diff = fmt.Sprintf("json_path %s.%s, left present, right missing", json_path, fld_name)
			return
		}
		if equals, first_diff = compareConfigRecursive(fld1.Interface(), fld2.Interface(), params, fmt.Sprintf("%s.%s", json_path, fld_name)); !equals {
			return
		}
	}
	equals = true
	return
}
