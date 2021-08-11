package common

import (
	"fmt"
	"github.com/go-errors/errors"
	"os"
	"reflect"
	"strings"
)

type XMLFile struct {
	name    string
	context string
}

type XMLAttr struct {
	Key   string
	Value interface{}
}

func NewXmlFile(name string) *XMLFile {
	return &XMLFile{
		name: name,
	}
}

func finalValue(value interface{}) interface{} {
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

func (xml *XMLFile) XMLWrite(tag string, value interface{}, indent int) {
	value = finalValue(value)
	if value == nil {
		return
	}
	xml.context += fmt.Sprintf("%s<%s>%v</%s>\n", strings.Repeat(" ", indent*4), tag, value, tag)
}

func (xml *XMLFile) XMLWritewithAttr(tag string, value interface{}, attrs []XMLAttr, indent int) {
	value = finalValue(value)
	if value == nil {
		return
	}

	xml.context += fmt.Sprintf("%s<%s", strings.Repeat(" ", indent*4), tag)
	for idx, attr := range attrs {
		if idx < len(attrs) {
			xml.context += " "
		}
		xml.context += fmt.Sprintf("%s=\"%v\"", attr.Key, finalValue(attr.Value))
	}
	xml.context += fmt.Sprintf("%v</%s>\n", tag, value)
}

func (xml *XMLFile) XMLBegin(tag string, indent int) {
	xml.context += fmt.Sprintf("%s<%s>\n", strings.Repeat(" ", indent*4), tag)
}

func (xml *XMLFile) XMLBeginwithAttr(tag string, attrs []XMLAttr, indent int) {
	xml.context += fmt.Sprintf("%s<%s", strings.Repeat(" ", indent*4), tag)
	for idx, attr := range attrs {
		if idx < len(attrs) {
			xml.context += " "
		}
		xml.context += fmt.Sprintf("%s=\"%v\"", attr.Key, finalValue(attr.Value))
	}
	xml.context += ">\n"
}

func (xml *XMLFile) XMLEnd(tag string, indent int) {
	xml.context += fmt.Sprintf("%s</%s>\n", strings.Repeat(" ", indent*4), tag)
}

func (xml *XMLFile) XMLComment(comment string, indent int) {
	xml.context += fmt.Sprintf("%s<!-- %s -->\n", strings.Repeat(" ", indent*4), comment)
}

func (xml *XMLFile) XMLAppend(context string) {
	xml.context += context
}

func (xml *XMLFile) XMLDump() error {
	if xml.name == "" {
		return errors.Errorf("xml name is not exist")
	}
	if xml.context == "" {
		return errors.Errorf("xml %s context is empty", xml.name)
	}
	fi, err := os.OpenFile(xml.name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer fi.Close()
	nbytes, err := fi.WriteString(xml.context)
	if err != nil {
		return err
	}
	if nbytes != len(xml.context) {
		return errors.Errorf("write xml file %s failed.", xml.name)
	}
	return nil
}
