package common

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/valyala/fastjson"
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
	// https://meta.wikimedia.org/wiki/Template:List_of_language_names_ordered_by_code
	LabelEN       string
	LabelZH       string
	DescriptionEN string
	DescriptionZH string
	Visiable      string //empty means: true(visiable)
	Required      string //empty means: false(optional) iff field type is Ptr
	InputType     string
	Filter        string //js code segment, work with Candidates list
	Candidates    []Candidate
	Default       string
	Regexp        string
	Range         *Range
	Editable      string
}

const (
	InputText     = "text"
	InputTextArea = "textarea"
	InputPassword = "password"
)

type Range struct {
	Min  float64
	Max  float64
	Step float64
}

type Candidate struct {
	Value   string
	LabelEN string
	LabelZH string
}

type ConfigParams map[string]*Parameter

func (params ConfigParams) MustRegister(v interface{}, field string, param *Parameter) {
	rt := reflect.TypeOf(v)
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if _, found := rt.FieldByName(field); !found {
		panic(fmt.Sprintf("%s.%s has no field named %s", rt.PkgPath(), rt.Name(), field))
	}
	pth := fmt.Sprintf("%s.%s.%s", rt.PkgPath(), rt.Name(), field)
	params[pth] = param
}

func (params ConfigParams) MarshalSchema(v interface{}) (data string, err error) {
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
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		str_type = "int"
	case reflect.Float32, reflect.Float64:
		str_type = "float"
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
		err = errors.Errorf("Cannot use %s.%s as a paratemter， unsupported kind %s", rt.PkgPath(), rt.Name(), rt.Kind().String())
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
	sb.WriteString(`"label_en": `)
	sb.WriteString(nullableString(param.LabelEN))
	sb.WriteString(`, "label_zh": `)
	sb.WriteString(nullableString(param.LabelZH))
	sb.WriteString(`, "description_en": `)
	sb.WriteString(nullableString(param.DescriptionEN))
	sb.WriteString(`, "description_zh": `)
	sb.WriteString(nullableString(param.DescriptionZH))
	sb.WriteString(`, "visiable": `)
	if param.Visiable == "" {
		param.Visiable = "true"
	}
	sb.WriteString(nullableString(param.Visiable))
	if param.Required == "" {
		if rt.Kind() == reflect.Ptr {
			param.Required = "false"
		} else {
			param.Required = "true"
		}
	}
	sb.WriteString(`, "required": `)
	sb.WriteString(nullableString(param.Required))
	if param.Editable == "" {
		param.Editable = "true"
	}
	sb.WriteString(`, "editable": `)
	sb.WriteString(nullableString(param.Editable))
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
	if str_type == "string" || str_type == "int" || str_type == "float" {
		sb.WriteString(`, "input_type": `)
		if len(param.InputType) == 0 {
			param.InputType = InputText
		}
		sb.WriteString(nullableString(param.InputType))
		if len(param.Candidates) == 0 {
			sb.WriteString(`, "candidates": null`)
		} else {
			sb.WriteString(`, "candidates": [`)
			for i, cand := range param.Candidates {
				if i != 0 {
					sb.WriteString(", ")
				}
				if cand.Value == "" {
					err = errors.Errorf("Type %s.%s candidate %v value shall not be empty", rt.PkgPath(), rt.Name(), i)
					return
				}
				sb.WriteString(fmt.Sprintf(`{"label_en": %v, "label_zh": %v, "value": %v}`, nullableString(cand.LabelEN), nullableString(cand.LabelZH), nullableString(cand.Value)))
			}
			sb.WriteByte(byte(']'))
			sb.WriteString(`, "filter": `)
			sb.WriteString(nullableString(param.Filter))
		}
		sb.WriteString(`, "default": `)
		sb.WriteString(nullableString(param.Default))
		if str_type == "string" {
			sb.WriteString(`, "regexp": `)
			sb.WriteString(nullableString(param.Regexp))
		} else {
			if param.Range == nil {
				switch rt.Kind() {
				case reflect.Int8:
					param.Range = &Range{math.MinInt8, math.MaxInt8, 1}
				case reflect.Int16:
					param.Range = &Range{math.MinInt16, math.MaxInt16, 1}
				case reflect.Int32:
					param.Range = &Range{math.MinInt32, math.MaxInt32, 1}
				case reflect.Int64:
					param.Range = &Range{math.MinInt64, math.MaxInt64, 1}
				case reflect.Uint8:
					param.Range = &Range{0, math.MaxUint8, 1}
				case reflect.Uint16:
					param.Range = &Range{0, math.MaxUint16, 1}
				case reflect.Uint32:
					param.Range = &Range{0, math.MaxUint32, 1}
				case reflect.Uint64:
					param.Range = &Range{0, math.MaxUint64, 1}
				case reflect.Float32:
					param.Range = &Range{-math.MaxFloat32, math.MaxFloat32, 1}
				case reflect.Float64:
					param.Range = &Range{-math.MaxFloat64, math.MaxFloat64, 1}
				}
			}
			if param.Range != nil {
				sb.WriteString(fmt.Sprintf(`, "range": {"min": %v, "max": %v, "step": %v}`, param.Range.Min, param.Range.Max, param.Range.Step))
			}
		}
	} else if typ_struct != nil {
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
			err = errors.Errorf("Type %s.%s has no fields registered as a paratemter", typ_struct.PkgPath(), typ_struct.Name())
			return
		}
		sb.WriteByte(byte('}'))
	}
	sb.WriteByte(byte('}'))
	return
}

// Inspired by https://stackoverflow.com/a/31302688/319936
func (params ConfigParams) MarshalConfig(v interface{}) (data string, err error) {
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
		err = errors.Errorf("Type %s.%s has no fields registered as a paratemter", rt.PkgPath(), rt.Name())
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
		if num_fields <= 0 {
			err = errors.Errorf("Type %s.%s has no fields registered as a paratemter", rt.PkgPath(), rt.Name())
			return
		}
		sb.WriteByte(byte('}'))
	default:
		err = errors.Errorf("Cannot use %s.%s as a paratemter， unsupported kind %s", rt.PkgPath(), rt.Name(), rt.Kind().String())
		return
	}
	return
}

func (params ConfigParams) UnmarshalConfig(data string, v interface{}) (err error) {
	var fjv *fastjson.Value
	if fjv, err = fastjson.Parse(data); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	rv := reflect.ValueOf(v)
	rt := reflect.TypeOf(v)
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
		rv = rv.Elem()
	}
	size := rv.NumField()
	prefix := rt.PkgPath() + "." + rt.Name() + "."
	var num_fields int
	for i := 0; i < size; i++ {
		rvf := rv.Field(i)
		fieldName := rt.Field(i).Name
		key := prefix + fieldName
		if _, ok := params[key]; ok {
			num_fields++
			if err = unmarshalConfigRecursive(params, rvf, fjv.Get(fieldName)); err != nil {
				return
			}
		}
	}
	if num_fields <= 0 {
		err = errors.Errorf("Type %s.%s has no fields registered as a paratemter", rt.PkgPath(), rt.Name())
		return
	}
	return
}

func unmarshalConfigRecursive(params map[string]*Parameter, rv reflect.Value, fjv *fastjson.Value) (err error) {
	if fjv == nil || fjv.Type() == fastjson.TypeNull {
		rv = indirect(rv, true)
		switch rv.Kind() {
		case reflect.Interface, reflect.Ptr, reflect.Map, reflect.Slice:
			rv.Set(reflect.Zero(rv.Type()))
			// otherwise, ignore null for primitives/string
		}
		return
	}
	rv = indirect(rv, false)
	rt := rv.Type()
	switch rt.Kind() {
	case reflect.Bool:
		switch fjv.Type() {
		case fastjson.TypeTrue:
			rv.SetBool(true)
		case fastjson.TypeFalse:
			rv.SetBool(false)
		default:
			err = errors.Errorf("unmarshal error")
			return
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var val int64
		if val, err = fjv.Int64(); err != nil {
			err = errors.Wrapf(err, "unmarshal error")
			return
		}
		rv.SetInt(val)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		var val uint64
		if val, err = fjv.Uint64(); err != nil {
			err = errors.Wrapf(err, "unmarshal error")
			return
		}
		rv.SetUint(val)
	case reflect.Float32, reflect.Float64:
		var val float64
		if val, err = fjv.Float64(); err != nil {
			err = errors.Wrapf(err, "unmarshal error")
			return
		}
		rv.SetFloat(val)
	case reflect.String:
		var val []byte
		if val, err = fjv.StringBytes(); err != nil {
			err = errors.Wrapf(err, "unmarshal error")
			return
		}
		rv.SetString(string(val))
	case reflect.Array, reflect.Slice:
		var fja []*fastjson.Value
		if fja, err = fjv.Array(); err != nil {
			err = errors.Wrapf(err, "unmarshal error")
			return
		}
		rv.Set(reflect.MakeSlice(rt, len(fja), len(fja)))
		for i := 0; i < len(fja); i++ {
			if err = unmarshalConfigRecursive(params, rv.Index(i), fja[i]); err != nil {
				return
			}
		}
	case reflect.Map:
		var fjo *fastjson.Object
		if fjo, err = fjv.Object(); err != nil {
			err = errors.Wrapf(err, "unmarshal error")
			return
		}
		rv.Set(reflect.MakeMapWithSize(rt, fjo.Len()))
		// refers to encoding/json/decode.go#772, (*decodeState).object
		kt := rt.Key()
		fjo.Visit(func(key []byte, v *fastjson.Value) {
			var rvk reflect.Value
			str_key := string(key)
			switch kt.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				var val int64
				if val, err = strconv.ParseInt(str_key, 10, 64); err != nil {
					err = errors.Wrapf(err, "unmarshal error")
					return
				}
				rvk = reflect.ValueOf(val).Convert(kt)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				var val uint64
				if val, err = strconv.ParseUint(str_key, 10, 64); err != nil {
					err = errors.Wrapf(err, "unmarshal error")
					return
				}
				rvk = reflect.ValueOf(val).Convert(kt)
			case reflect.String:
				rvk = reflect.ValueOf(str_key).Convert(kt)
			default:
				err = errors.Errorf("unmarshal error")
				return
			}
			rvv := reflect.New(rt.Elem())
			if err = unmarshalConfigRecursive(params, rvv, v); err != nil {
				return
			}
			rv.SetMapIndex(rvk, rvv.Elem())
		})
	case reflect.Struct:
		prefix := rt.PkgPath() + "." + rt.Name() + "."
		var num_fields int
		for i := 0; i < rv.NumField(); i++ {
			rtf := rt.Field(i)
			rvf := rv.FieldByName(rtf.Name)
			key := prefix + rtf.Name
			if _, ok := params[key]; ok {
				num_fields++
				if err = unmarshalConfigRecursive(params, rvf, fjv.Get(rtf.Name)); err != nil {
					return
				}
			}
		}
		if num_fields <= 0 {
			err = errors.Errorf("Type %s.%s has no fields registered as a paratemter", rt.PkgPath(), rt.Name())
			return
		}
	default:
		err = errors.Errorf("Cannot use %s.%s as a paratemter， unsupported kind %s", rt.PkgPath(), rt.Name(), rt.Kind().String())
		return
	}
	return
}

// refers to encoding/json/decode.go#425, indirect
// indirect walks down v allocating pointers as needed,
// until it gets to a non-pointer.
// If decodingNull is true, indirect stops at the first settable pointer so it
// can be set to nil.
func indirect(v reflect.Value, decodingNull bool) reflect.Value {
	// Issue #24153 indicates that it is generally not a guaranteed property
	// that you may round-trip a reflect.Value by calling Value.Addr().Elem()
	// and expect the value to still be settable for values derived from
	// unexported embedded struct fields.
	//
	// The logic below effectively does this when it first addresses the value
	// (to satisfy possible pointer methods) and continues to dereference
	// subsequent pointers as necessary.
	//
	// After the first round-trip, we set v back to the original value to
	// preserve the original RW flags contained in reflect.Value.
	v0 := v
	haveAddr := false

	// If v is a named type and is addressable,
	// start with its address, so that if the type has pointer methods,
	// we find them.
	if v.Kind() != reflect.Ptr && v.Type().Name() != "" && v.CanAddr() {
		haveAddr = true
		v = v.Addr()
	}
	for {
		// Load value from interface, but only if the result will be
		// usefully addressable.
		if v.Kind() == reflect.Interface && !v.IsNil() {
			e := v.Elem()
			if e.Kind() == reflect.Ptr && !e.IsNil() && (!decodingNull || e.Elem().Kind() == reflect.Ptr) {
				haveAddr = false
				v = e
				continue
			}
		}

		if v.Kind() != reflect.Ptr {
			break
		}

		if decodingNull && v.CanSet() {
			break
		}

		// Prevent infinite loop if v is an interface pointing to its own address:
		//     var v interface{}
		//     v = &v
		if v.Elem().Kind() == reflect.Interface && v.Elem().Elem() == v {
			v = v.Elem()
			break
		}
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}

		if haveAddr {
			v = v0 // restore original value after round-trip Value.Addr().Elem()
			haveAddr = false
		} else {
			v = v.Elem()
		}
	}
	return v
}

func (params ConfigParams) CompareConfig(v1, v2 interface{}) (equals bool, first_diff string) {
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
