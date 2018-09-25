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
		strKeys = strKeys + getPgName(field.Name, dbtag) + ","
		values = append(values, rval.Field(i).Interface())
		strParams = strParams + "$" + strconv.Itoa(counter) + ","
		counter++
	}

	query := " insert into " + dam.TableName + "(" + TrimSuffix(strKeys, ",") + ") values(" + TrimSuffix(strParams, ",") + ") RETURNING id;"
	// log.Info("query for inserting object is", query)
	var id int
	err := dam.DB.QueryRow(query, values...).Scan(&id)
	return id, err
}

func (dam *BasicDAM) Update(id int, obj interface{}) error {
	columns := getFieldsByTag(dam.Instance, "props", "editable")
	// log.Info("here is the editable columns:%+v", columns)
	rval := reflect.ValueOf(obj)
	if rval.Kind() == reflect.Ptr {
		rval = rval.Elem()
	}
	updateQuery := ""
	counter := 1
	values := make([]interface{}, 0)
	for i := 0; i < len(columns); i++ {
		typeField, _ := rval.Type().FieldByName(columns[i])
		dbtag := typeField.Tag.Get("db")
		if dbtag == "-" {
			continue
		}

		updateQuery = updateQuery + getPgName(typeField.Name, dbtag) + "=" + "$" + strconv.Itoa(counter) + ","
		values = append(values, rval.FieldByName(columns[i]).Interface())
		counter++
	}
	values = append(values, id)

	strQ := " update " + dam.TableName + " set " + TrimSuffix(updateQuery, ",")
	strQ += " where id=" + "$" + strconv.Itoa(counter) + ";"

	// log.Infof("update query is...%s, %+v", strQ, values)

	_, err := dam.DB.Exec(strQ, values...)
	return err
}
