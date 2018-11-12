package basicdam

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

//TODO support array filedName
func (dam *BasicDAM) SyncDB() error {
	var err error
	ParsedObj := parseObjectData(dam.Instance)
	// log.Infof("parsed object data is: %+v", ParsedObj)
	err = addTable(dam.Instance, dam.TableName, dam.DB, ParsedObj)
	if err != nil {
		log.Error("Error in Creating Database Table: ", err)
		return err
	}
	err = syncSchema(dam.TableName, dam.DB, ParsedObj)
	if err != nil {
		log.Error("Error in Synch Database: ", err)
		return err
	}

	return nil
}

// adds table if not exists
func addTable(obj interface{}, tablename string, db *sqlx.DB, fields parsedData) error {
	//first install extensions...
	var strQ string
	extensions := getRequiredExtensions(obj)
	for _, ext := range extensions {
		strQ += "CREATE EXTENSION IF NOT EXISTS \"" + ext + "\";"
	}
	strQ += " CREATE TABLE IF NOT EXISTS " + tablename + " ( "
	for _, fData := range fields {
		if fData.PGName == "-" {
			continue
		}
		//check primary key
		if fData.PrimaryKey {
			if fData.PGType == "integer" {
				strQ = strQ + " " + fData.PGName + " SERIAL PRIMARY KEY " + ","
			} else if fData.PGType == "uuid" {
				strQ = strQ + " " + fData.PGName + " uuid DEFAULT uuid_generate_v4() PRIMARY KEY,"
			} else {
				strQ = strQ + " " + fData.PGName + " " + fData.PGType + " PRIMARY KEY,"
			}
			continue
		}
		//ordinary column
		strQ = strQ + " " + fData.PGName + " " + fData.PGType + ","
	}
	strQ = TrimSuffix(strQ, ",") + " ) "
	// log.Infof("creating table: %s query: %s", tablename, strQ)
	log.Info("create table query: ", strQ)
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

func findFieldByPGName(fields parsedData, pgName string) (fieldData, bool) {
	for _, fData := range fields {
		if fData.PGName == pgName {
			return fData, true
		}
	}
	return fieldData{}, false
}
func syncSchema(tableName string, db *sqlx.DB, fields parsedData) error {
	// add missing fields
	for _, fData := range fields {
		if fData.PGName == "-" {
			continue
		}
		//check if the field exists in db or not
		exists, err := checkPgColumn(tableName, db, fData.PGName)
		if err != nil {
			return errors.New("can not check for existance of column, " + err.Error())
		}
		//if it is not in db we should create the column
		if !exists {
			log.Infof("adding field: %s of type: %s to table %s", fData.PGName, fData.PGType, tableName)
			err := addPostgresColumn(tableName, db, fData.PGName, fData.PGType)
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
		objDat, shouldExist := findFieldByPGName(fields, dbcolumns[k].Column_Name)
		desiredtype := objDat.PGType
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
	log.Println("query for adding column ", strQ)
	_, err := db.Exec(strQ)
	return err
}

func dropPostgresColumn(tableName string, db *sqlx.DB, field string) error {
	_, err := db.Exec("alter table " + tableName + " drop column " + field)
	return err
}

func goType2Pg(typeOfField string) string {
	if typeOfField == "string" {
		return "text"
	} else if typeOfField == "int" {
		return "integer"
	} else if typeOfField == "bool" {
		return "boolean"
	} else if typeOfField == "time" {
		return "timestamp with time zone"
	} else if typeOfField == "float32" || typeOfField == "float64" {
		return "real"
	} else {
		//return same as input
		return typeOfField
	}
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
	} else if pgtype == "uuid" {
		defaultVal = "uuid_generate_v4()"
	}
	return defaultVal
}
