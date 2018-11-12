package basicdam

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

func (dam *BasicDAM) Insert(obj interface{}) (string, error) {
	counter := 1
	var strKeys, strParams string
	values := make([]interface{}, 0)
	fieldValues := parseObjectData(obj)
	for _, fData := range fieldValues {
		if fData.PrimaryKey || fData.PGName == "-" {
			continue
		}
		strKeys = strKeys + fData.PGName + ","
		values = append(values, fData.Value)
		strParams = strParams + "$" + strconv.Itoa(counter) + ","
		counter++
	}

	query := " insert into " + dam.TableName + "(" + TrimSuffix(strKeys, ",") + ") values(" + TrimSuffix(strParams, ",") + ") RETURNING id;"
	// log.Infof("insert query: %s, %+v", query, values)
	var id string
	err := dam.DB.QueryRow(query, values...).Scan(&id)
	return id, err
}

func (dam *BasicDAM) ValidateUpdate(obj interface{}, json string) error {
	fields := parseObjectData(obj)
	for _, fData := range fields {

		inpVal := gjson.Get(json, fData.JsonName)
		inpEmpty := inpVal.Raw == ""
		// log.Printf("checking field: %s, provided value: %s, empty?:%t ", typeField.Name, inpVal, inpEmpty)

		props := fData.FieldType.Tag.Get("props")

		if !inpEmpty {
			//something is provided
			if !strings.Contains(props, "editable") {
				return errors.Wrap(ErrInvalid, fmt.Sprintf("field: %s is not editable", fData.JsonName))
			}
		} else {
			//provided data is Null
			if strings.Contains(props, "editable") && strings.Contains(props, "notNull") {
				return errors.Wrap(ErrInvalid, fmt.Sprintf("field: %s can not be empty/zero", fData.JsonName))
			}
		} //end of field logic
	}
	return nil
}

func (dam *BasicDAM) Update(id string, obj interface{}) error {
	updateQuery := ""
	counter := 1
	values := make([]interface{}, 0)

	fields := parseObjectData(obj)
	for _, fData := range fields {
		props := fData.FieldType.Tag.Get("props")
		if fData.PGName == "-" || !strings.Contains(props, "editable") {
			continue
		}

		updateQuery = updateQuery + fData.PGName + "=" + "$" + strconv.Itoa(counter) + ","
		values = append(values, fData.Value)
		counter++
	}
	values = append(values, id)

	strQ := " update " + dam.TableName + " set " + TrimSuffix(updateQuery, ",")
	strQ += " where id=" + "$" + strconv.Itoa(counter) + ";"

	// log.Infof("update query is...%s, %+v", strQ, values)

	_, err := dam.DB.Exec(strQ, values...)
	return err
}

func (dam *BasicDAM) ValidatePatch(obj interface{}, json string) error {
	fields := parseObjectData(obj)
	for _, fData := range fields {
		inpVal := gjson.Get(json, fData.JsonName)
		inpEmpty := inpVal.Raw == ""
		// log.Printf("checking field: %s, provided value: %s, empty?:%t ", typeField.Name, inpVal, inpEmpty)

		props := fData.FieldType.Tag.Get("props")

		if !inpEmpty {
			//something is provided
			if !strings.Contains(props, "editable") {
				return errors.Wrap(ErrInvalid, fmt.Sprintf("field: %s is not editable", fData.JsonName))
			}
		}
	}
	return nil
}

func (dam *BasicDAM) Patch(id string, obj interface{}, json string) error {
	updateQuery := ""
	counter := 1
	values := make([]interface{}, 0)
	fields := parseObjectData(obj)
	for _, fData := range fields {
		log.Infof("check fdata, %s, %s", fData.PGName, fData.JsonName)
		if fData.PGName == "-" {
			continue
		}
		inpVal := gjson.Get(json, fData.JsonName)
		if inpVal.Raw == "" {
			//filed is not provded to get updated
			continue
		}
		updateQuery = updateQuery + fData.PGName + "=" + "$" + strconv.Itoa(counter) + ","
		values = append(values, fData.Value)
		counter++
	}
	values = append(values, id)

	strQ := " update " + dam.TableName + " set " + TrimSuffix(updateQuery, ",")
	strQ += " where id=" + "$" + strconv.Itoa(counter) + ";"

	log.Infof("patch query is...%s, %+v", strQ, values)

	_, err := dam.DB.Exec(strQ, values...)
	return err
}
