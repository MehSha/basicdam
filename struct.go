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
	if dbtype != "" {
		return dbtype
	}
	return typeField.Type.Kind().String()
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
func getRequiredExtensions(obj interface{}) []string {
	extensions := []string{}
	rval := reflect.ValueOf(obj)
	if rval.Kind() == reflect.Ptr {
		rval = rval.Elem()
	}

	for i := 0; i < rval.NumField(); i++ {
		typeField := rval.Type().Field(i)

		requiredExtension := typeField.Tag.Get("dbextension")
		if requiredExtension != "" {
			extensions = append(extensions, requiredExtension)
		}
	}
	return extensions
}
func TrimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}

type fieldData struct {
	Name       string
	Value      interface{}
	JsonName   string
	PGType     string
	PGName     string
	PrimaryKey bool
	FieldType  reflect.StructField
}

type parsedData []fieldData

func parseObjectData(instance interface{}) parsedData {
	flMap := make([]fieldData, 0)
	rval := reflect.ValueOf(instance)
	if rval.Kind() == reflect.Ptr {
		rval = rval.Elem()
	}
	for i := 0; i < rval.NumField(); i++ {
		typeField := rval.Type().Field(i)
		goType := getFieldType(instance, typeField.Name)
		if goType == "struct" {
			subMap := parseObjectData(rval.FieldByName(typeField.Name).Interface())
			//merge 2 maps
			for _, v := range subMap {
				flMap = append(flMap, v)
			}
		} else {
			data := &fieldData{}
			data.Name = typeField.Name
			data.PGName = getPgName(typeField.Name, typeField.Tag.Get("db"))
			if strings.Contains(typeField.Tag.Get("props"), "primaryKey") {
				data.PrimaryKey = true
			}
			data.PGType = goType2Pg(goType)
			data.Value = rval.FieldByName(typeField.Name).Interface()
			data.FieldType = typeField
			//json
			data.JsonName = typeField.Name
			jsonTag := typeField.Tag.Get("json")
			if jsonTag != "" {
				data.JsonName = jsonTag
			}
			flMap = append(flMap, *data)
		}
	}
	return flMap
}
