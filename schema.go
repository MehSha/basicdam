package basicdam

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jmoiron/sqlx"
)

func (dam *BasicDAM) SyncDB() error {
	var err error
	err = addTable(dam.TableName(), dam.DB, dam.Instance)
	if err != nil {
		log.Error("Error in Creating Database Table: ", err)
		return err
	}
	err = syncSchema(dam.TableName(), dam.DB, dam.Instance)
	if err != nil {
		log.Error("Error in Synch Database: ", err)
		return err
	}

	return nil
}

// adds table if not exists
func addTable(tablename string, db *sqlx.DB, instance interface{}) error {
	rval := reflect.ValueOf(instance)
	if rval.Kind() == reflect.Ptr {
		rval = rval.Elem()
	}
	var strQ string
	strQ = " CREATE TABLE IF NOT EXISTS " + tablename + " ( "

	for i := 0; i < rval.NumField(); i++ {
		typeField := rval.Type().Field(i)
		dbtag := typeField.Tag.Get("db")
		if dbtag == "-" {
			continue
		}

		typ := goType2Pg(getFieldType(instance, typeField.Name))
		if typ == "" {
			return errors.New(fmt.Sprintf(
				"type of field: %s can not be inferred. if it is a struct please add dbtype tag", typeField.Name))
		}
		//log.Infof("field info, name:%s, type: %s, pgtype:%s", typeField.Name, typeOfField, typ)

		if typeField.Name == "ID" {
			strQ = strQ + " " + typeField.Name + " SERIAL PRIMARY KEY " + ","
		} else {
			strQ = strQ + " " + typeField.Name + " " + typ + ","
		}

	}

	strQ = TrimSuffix(strQ, ",") + " ) "
	log.Info("create table query: ", strQ)
	_, err := db.Exec(strQ)
	return err
}

func syncSchema(tableName string, db *sqlx.DB, instance interface{}) error {
	ival := reflect.ValueOf(instance)
	if ival.Kind() == reflect.Ptr {
		ival = ival.Elem()
	}

	//this map checks if the field exists on pg table or not
	structFields := make(map[string]bool)
	for j := 0; j < ival.NumField(); j++ {
		typeField := ival.Type().Field(j)
		//postgres converts all names to lower case
		filedName := strings.ToLower(typeField.Name)
		//check if the field exists in db or not
		exists, _ := checkPgColumn(tableName, db, filedName)
		dbtag := typeField.Tag.Get("db")

		//this is a hack to not to try to dropp the column later while it is not in db
		if dbtag != "-" {
			structFields[filedName] = true
		}
		//if it is not in db and not excluded from struct add field to database
		if !exists && dbtag != "-" {

			typ := goType2Pg(getFieldType(instance, typeField.Name))
			log.Infof("adding field: %s", filedName)
			err := addPostgresColumn(tableName, db, filedName, typ)
			if err != nil {
				return errors.New("can not add row to table: " + err.Error())
			}
		}
	}

	var dbcols []string
	//get list of table columns
	err := db.Select(&dbcols, "SELECT Column_Name FROM information_schema.columns WHERE table_name='"+tableName+"'")
	if err != nil {
		return err
	}
	for k := 0; k < len(dbcols); k++ {
		//drop database column if don't exist in struct
		_, exists := structFields[dbcols[k]]
		if !exists {
			log.Infof("dropping field: %s", dbcols[k])
			dropPostgresColumn(tableName, db, dbcols[k])
		}
	}
	return nil
}

func checkPgColumn(tableName string, db *sqlx.DB, field string) (bool, error) {
	var result int
	err := db.Get(&result, "SELECT count(*) FROM information_schema.columns WHERE table_name='"+tableName+"' and column_name='"+field+"'")
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}

	if result == 1 {
		return true, nil
	}

	return false, nil
}

func addPostgresColumn(tableName string, db *sqlx.DB, field, dbtype string) error {
	//withut default value, older rows would be set to null which results in error when scanning
	defaultVal := getDefaultPgValue(dbtype)

	strQ := "alter table " + tableName + " add column " + field + " " + dbtype + " default " + defaultVal
	_, err := db.Exec(strQ)
	return err
}

func dropPostgresColumn(tableName string, db *sqlx.DB, field string) error {
	_, err := db.Exec("alter table " + tableName + " drop column " + field)
	return err
}

func goType2Pg(typeOfField string) string {
	var pgtyp string
	if typeOfField == "string" {
		pgtyp = "text"
	} else if typeOfField == "int" {
		pgtyp = "integer"
	} else if typeOfField == "bool" {
		pgtyp = "boolean"
	} else if typeOfField == "JSONB" {
		pgtyp = "JSONB"
	} else if typeOfField == "time" {
		pgtyp = "timestamp with time zone"
	} else if typeOfField == "float32" || typeOfField == "float64" {
		pgtyp = "real"
	}

	return pgtyp
}

func getDefaultPgValue(pgtype string) string {
	defaultVal := ""
	if pgtype == "text" {
		defaultVal = "''"
	} else if pgtype == "integer" || pgtype == "real" {
		defaultVal = "0"
	} else if pgtype == "boolean" {
		defaultVal = "false"
	} else if pgtype == "JSONB" {
		defaultVal = "'{}'"
	} else if pgtype == "timestamp with time zone" {
		defaultVal = "'0001-01-01 03:25:44+03:25:44'"
	}
	return defaultVal
}

func TrimSuffix(s, suffix string) string {
	if strings.HasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}
