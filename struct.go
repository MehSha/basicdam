package basicdam

import (
	"reflect"
	"strings"
)

func getFieldType(val interface{}, name string) string {
	tval := reflect.TypeOf(val)
	if tval.Kind() == reflect.Ptr {
		tval = tval.Elem()
	}

	typeField, ok := tval.FieldByName(name)
	if !ok {
		//field does not exists
		return ""
	}
	dbtype := typeField.Tag.Get("dbtype")
	kind := typeField.Type.Kind().String()

	var typeOfField string
	//if the field is a pointer or an struct, there should be dbtype tag to be used
	if kind != "struct" && kind != "ptr" {
		typeOfField = kind
	} else if dbtype != "" {
		typeOfField = dbtype
	}
	// log.Infof("examining field:%s, Kind: %s, dbtype:%s, type: %s", name, kind, dbtype, typeOfField)
	return typeOfField
}

func getFieldsByTag(val interface{}, tag, prop string) []string {
	arrFields := []string{}
	rval := reflect.ValueOf(val)
	if rval.Kind() == reflect.Ptr {
		rval = rval.Elem()
	}

	for i := 0; i < rval.NumField(); i++ {
		typeField := rval.Type().Field(i)

		if strings.Contains(typeField.Tag.Get(tag), prop) {
			arrFields = append(arrFields, typeField.Name)
		}
	}
	return arrFields
}
