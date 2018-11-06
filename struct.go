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
	// log.Infof("field kind is: %s for %s", kind, name)

	//if the field is a pointer or an struct, there should be dbtype tag to be used
	if kind != "struct" && kind != "ptr" {
		return kind
	}
	//this is struct or pointer
	if dbtype != "" {
		return dbtype
	}
	//struct or pointer that has no dbtye tag

	// log.Infof("examining field:%s, Kind: %s, dbtype:%s, type: %s", name, kind, dbtype, typeOfField)
	return "embedded"
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
func TrimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}

type fieldData struct {
	Name      string
	Value     interface{}
	JsonName  string
	PGType    string
	PGName    string
	FieldType reflect.StructField
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
		if goType == "embedded" {
			subMap := parseObjectData(rval.FieldByName(typeField.Name).Interface())
			//merge 2 maps
			for _, v := range subMap {
				flMap = append(flMap, v)
			}
		} else {
			data := &fieldData{}
			data.Name = typeField.Name
			data.PGName = getPgName(typeField.Name, typeField.Tag.Get("db"))
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
