package basicdam

import (
	"database/sql"
	"encoding/json"
	"reflect"

	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
)

func CatchExecErr(r sql.Result, err error) error {
	if err != nil {
		return errors.Wrap(ErrDB, err.Error())
	}
	rows, err := r.RowsAffected()
	if err != nil {
		return errors.Wrap(ErrDB, "can not get affected rows")
	}
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func PatchJson(obj interface{}, rawJson string) (interface{}, error) {
	rval := reflect.ValueOf(obj)
	if rval.Kind() == reflect.Ptr {

		rval = rval.Elem()
	}
	rtype := rval.Type()

	nrvalPtr := reflect.New(rtype)
	err := json.Unmarshal([]byte(rawJson), nrvalPtr.Interface())
	if err != nil {
		return obj, err
	}
	nrval := nrvalPtr.Elem()

	for i := 0; i < rtype.NumField(); i++ {
		typeField := rtype.Field(i)
		//check field availability in provided object
		jsonName := typeField.Name
		if jsonTag := typeField.Tag.Get("json"); jsonTag != "" {
			jsonName = jsonTag
		}
		inpVal := gjson.Get(rawJson, jsonName)
		if inpVal.Raw != "" {
			rval.FieldByName(typeField.Name).Set(nrval.FieldByName(typeField.Name))
		}
		// if inpVal.Raw == "" {
		// 	//we should keep the old one
		// 	log.Infof("updating %s, keeping old version. input: %s", typeField.Name, inpVal)
		// 	output.FieldByName(typeField.Name).Set(rval.FieldByName(typeField.Name))
		// } else {
		// 	// we should apply the new one
		// 	log.Infof("updating %s, applying new version. input: %s", typeField.Name, inpVal)
		// 	rval.FieldByName(typeField.Name).Set(nrval.FieldByName(typeField.Name))
		// }

	}
	return rval.Interface(), nil
}
