package common

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

type XMLFile struct {
	name    string
	context string
	indent  int
}

type XMLAttr struct {
	Key   string
	Value interface{}
}

func NewXmlFile(name string) *XMLFile {
	return &XMLFile{
		name:   name,
		indent: 0,
	}
}

func escape(context string) string {
	context = strings.ReplaceAll(context, "&", "&amp;")
	context = strings.ReplaceAll(context, "<", "&lt;")
	context = strings.ReplaceAll(context, ">", "&gt;")
	context = strings.ReplaceAll(context, "'", "&apos;")
	context = strings.ReplaceAll(context, "\"", "&quot;")
	return context
}

func finalValue(value interface{}) interface{} {
	if _, ok := value.(string); ok {
		value = escape(value.(string))
	}
	rv := reflect.ValueOf(value)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	return rv.Interface()
}

func (xml *XMLFile) GetContext() string {
	return xml.context
}

func (xml *XMLFile) SetContext(context string) {
	xml.context = context
}

func (xml *XMLFile) GetIndent() int {
	return xml.indent
}

func (xml *XMLFile) SetIndent(indent int) {
	xml.indent = indent
}

func (xml *XMLFile) Write(tag string, value interface{}) {
	value = finalValue(value)
	if value == nil {
		return
	}
	xml.context += fmt.Sprintf("%s<%s>%v</%s>\n", strings.Repeat(" ", xml.indent*4), tag, value, tag)
}

func (xml *XMLFile) WritewithAttr(tag string, value interface{}, attrs []XMLAttr) {
	value = finalValue(value)
	if value == nil {
		return
	}

	xml.context += fmt.Sprintf("%s<%s", strings.Repeat(" ", xml.indent*4), tag)
	for idx, attr := range attrs {
		if idx < len(attrs) {
			xml.context += " "
		}
		xml.context += fmt.Sprintf("%s=\"%v\"", attr.Key, finalValue(attr.Value))
	}
	xml.context += fmt.Sprintf(">%v</%s>\n", value, tag)
}

func (xml *XMLFile) Begin(tag string) {
	xml.context += fmt.Sprintf("%s<%s>\n", strings.Repeat(" ", xml.indent*4), tag)
	xml.indent++
}

func (xml *XMLFile) BeginwithAttr(tag string, attrs []XMLAttr) {
	xml.context += fmt.Sprintf("%s<%s", strings.Repeat(" ", xml.indent*4), tag)
	for idx, attr := range attrs {
		if idx < len(attrs) {
			xml.context += " "
		}
		xml.context += fmt.Sprintf("%s=\"%v\"", attr.Key, finalValue(attr.Value))
	}
	xml.context += ">\n"
	xml.indent++
}

func (xml *XMLFile) End(tag string) {
	xml.indent--
	xml.context += fmt.Sprintf("%s</%s>\n", strings.Repeat(" ", xml.indent*4), tag)
}

func (xml *XMLFile) Comment(comment string) {
	xml.context += fmt.Sprintf("%s<!-- %s -->\n", strings.Repeat(" ", xml.indent*4), comment)
}

func (xml *XMLFile) Append(context string) {
	xml.context += context
}

func (xml *XMLFile) Dump() error {
	if xml.name == "" {
		return errors.Errorf("xml name is not exist")
	}
	if xml.context == "" {
		return errors.Errorf("xml %s context is empty", xml.name)
	}
	fi, err := os.OpenFile(xml.name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return errors.Wrap(err, "")
	}
	defer fi.Close()
	nbytes, err := fi.WriteString(xml.context)
	if err != nil {
		return errors.Wrap(err, "")
	}
	if nbytes != len(xml.context) {
		return errors.Errorf("write xml file %s failed.", xml.name)
	}
	return nil
}

func ConvertMapping(input map[string]interface{}) map[string]interface{} {
	output := map[string]interface{}{}

	for k, v := range input {
		rv := reflect.ValueOf(v)
		//parse [1,2,3,4] => []interface{1,2,3,4}
		if rv.Kind() == reflect.String {
			value := strings.TrimSpace(rv.String())
			if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
				v = strings.Split(value[1:len(value)-1], ",")
			}
		}
		keys := strings.Split(strings.TrimSpace(k), "/")
		if len(keys) == 1 {
			output[k] = v
		} else {
			current := output
			for i, key := range keys {
				if i == len(keys)-1 {
					current[key] = v
				} else {
					if _, ok := current[key]; !ok {
						current[key] = map[string]interface{}{}
					}
					current = current[key].(map[string]interface{})
				}
			}
		}
	}
	return output
}

func parseTags(key string) (string, []XMLAttr) {
	var tag string
	var attrs []XMLAttr
	if strings.Contains(key, "[") {
		// title[@lang='en', @size=4]/header:header123 => <title lang="en" size="4"><header>header123</header></title>
		index := strings.Index(key, "[")
		tag = key[:index]
		attrArr := strings.Split(strings.TrimRight(strings.TrimLeft(key[index:], "["), "]"), ",")
		for _, attr := range attrArr {
			kv := strings.Split(attr, "=")
			if len(kv) == 2 {
				xmlattr := XMLAttr{
					Key:   strings.TrimPrefix(strings.TrimSpace(kv[0]), "@"),
					Value: strings.Trim(strings.Trim(kv[1], "'"), "\""),
				}
				attrs = append(attrs, xmlattr)
			}
		}
	} else {
		tag = key
		attrs = nil
	}
	return tag, attrs
}

func (xml *XMLFile) mapping(output map[string]interface{}) {
	keys := make([]string, 0)
	for k := range output {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := output[k]
		tag, attrs := parseTags(k)
		rt := reflect.TypeOf(v)
		switch rt.Kind() {
		case reflect.Map:
			xml.BeginwithAttr(tag, attrs)
			xml.mapping(v.(map[string]interface{}))
			xml.End(tag)
		case reflect.Array, reflect.Slice:
			rv := reflect.ValueOf(v)
			if rt.Elem().Kind() == reflect.Map {
				xml.BeginwithAttr(tag, attrs)
			}
			for i := 0; i < rv.Len(); i++ {
				if rt.Elem().Kind() == reflect.Map {
					xml.mapping(rv.Index(i).Interface().(map[string]interface{}))
				} else {
					xml.WritewithAttr(tag, rv.Index(i), attrs)
				}
			}
			if rt.Elem().Kind() == reflect.Map {
				xml.End(tag)
			}
		default:
			xml.WritewithAttr(tag, v, attrs)
		}
	}
}

func (xml *XMLFile) Merge(input map[string]interface{}) {
	output := ConvertMapping(input)
	xml.mapping(output)
}
