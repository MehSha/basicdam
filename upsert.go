package basicdam

import (
	"reflect"
	"strconv"
)

func (dam *BasicDAM) Insert(obj interface{}) (int, error) {
	rval := reflect.ValueOf(obj)
	if rval.Kind() == reflect.Ptr {
		rval = rval.Elem()
	}
	var strKeys, strParams string
	values := make([]interface{}, 0)

	counter := 1

	for i := 0; i < rval.NumField(); i++ {
		field := rval.Type().Field(i)
		dbtag := field.Tag.Get("db")
		if dbtag == "-" || field.Name == "ID" {
			continue
		}
		// typeOfField := getFieldType(obj, field.Name)

		strKeys = strKeys + getPgName(field.Name, dbtag) + ","
		values = append(values, rval.Field(i).Interface())
		// val := PrepareValue(rval.Field(i).Interface(), typeOfField)

		strParams = strParams + "$" + strconv.Itoa(counter) + ","
		counter++
	}

	query := " insert into " + dam.TableName + "(" + TrimSuffix(strKeys, ",") + ") values(" + TrimSuffix(strParams, ",") + ") RETURNING id;"
	// log.Info("query for inserting object is", query)
	var id int
	err := dam.DB.QueryRow(query, values...).Scan(&id)
	return id, err
}
