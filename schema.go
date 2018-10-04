package basicdam

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

//TODO support array filedName
func (dam *BasicDAM) SyncDB() error {
	var err error
	err = addTable(dam.TableName, dam.DB, dam.Instance)
	if err != nil {
		log.Error("Error in Creating Database Table: ", err)
		return err
	}
	err = syncSchema(dam.TableName, dam.DB, dam.Instance)
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
		fieldName := getPgName(typeField.Name, dbtag)
		if typeField.Name == "ID" {
			strQ = strQ + " " + fieldName + " SERIAL PRIMARY KEY " + ","
		} else {
			strQ = strQ + " " + fieldName + " " + typ + ","
		}

	}

	strQ = TrimSuffix(strQ, ",") + " ) "
	log.Infof("creating table: %s ", tablename)
	// log.Info("create table query: ", strQ)
	_, err := db.Exec(strQ)
	return err
}
func getPgName(name, dbtag string) string {
	if dbtag != "" {
		return dbtag
	}
	//postgres converts all names to lower case
	return strings.ToLower(name)
}

type dbColumn struct {
	Column_Name string
	Data_Type   string
}

func syncSchema(tableName string, db *sqlx.DB, instance interface{}) error {
	ival := reflect.ValueOf(instance)
	if ival.Kind() == reflect.Ptr {
		ival = ival.Elem()
	}

	//this map checks if the field exists on pg table or not
	structFields := make(map[string]string)
	for j := 0; j < ival.NumField(); j++ {
		typeField := ival.Type().Field(j)
		dbtag := typeField.Tag.Get("db")
		if dbtag == "-" {
			continue
		}
		filedName := getPgName(typeField.Name, dbtag)
		//check if the field exists in db or not
		exists, err := checkPgColumn(tableName, db, filedName)
		if err != nil {
			return errors.New("can not check for existance of column, " + err.Error())
		}

		typ := goType2Pg(getFieldType(instance, typeField.Name))
		//we keep track of which filed must be present in db and of which data type
		structFields[filedName] = typ
		//if it is not in db we should create the column
		if !exists {
			log.Infof("adding field: %s to table %s", filedName, tableName)
			err := addPostgresColumn(tableName, db, filedName, typ)
			if err != nil {
				return errors.New("can not add column to table: " + err.Error())
			}
		}
	}
	// now try to remove extra fields
	dbcolumns := make([]*dbColumn, 0)
	//get list of table columns
	err := db.Select(&dbcolumns, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='"+tableName+"'")
	if err != nil {
		return err
	}
	for k := 0; k < len(dbcolumns); k++ {
		//drop database column if don't exist in struct
		desiredtype, shouldExist := structFields[dbcolumns[k].Column_Name]
		if !shouldExist {
			log.Infof("dropping field: %s from table %s", dbcolumns[k].Column_Name, tableName)
			err := dropPostgresColumn(tableName, db, dbcolumns[k].Column_Name)
			if err != nil {
				return errors.New("can not remove column from table: " + err.Error())
			}
		}
		//if type changes, we need to handle it
		if shouldExist && desiredtype != dbcolumns[k].Data_Type {
			log.Infof("type changed in field: %s in table %s from %s to %s",
				dbcolumns[k].Column_Name, tableName, dbcolumns[k].Data_Type, desiredtype)
			err := dropAddPostgresColumn(tableName, db, dbcolumns[k].Column_Name, desiredtype)
			if err != nil {
				return errors.New("can not remove/add column to table: " + err.Error())
			}
		}
	}
	return nil
}

func dropAddPostgresColumn(tableName string, db *sqlx.DB, field, dbtype string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec("alter table " + tableName + " drop column " + field)
	if err != nil {
		return err
	}
	defaultVal := getDefaultPgValue(dbtype)
	strQ := "alter table " + tableName + " add column " + field + " " + dbtype + " default " + defaultVal
	_, err = tx.Exec(strQ)
	if err != nil {
		return err
	}
	return tx.Commit()
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
	} else if typeOfField == "jsonb" {
		pgtyp = "jsonb"
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
	} else if pgtype == "jsonb" {
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
