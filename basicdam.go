package basicdam

import (
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

type BasicDAM struct {
	DB        *sqlx.DB
	Instance  interface{}
	TableName string
}

func NewDAM(instance interface{}, db *sqlx.DB) *BasicDAM {
	return &BasicDAM{
		Instance:  instance,
		DB:        db,
		TableName: getTableName(instance),
	}
}

func getTableName(instance interface{}) string {
	tblName := ""
	if t := reflect.TypeOf(instance); t.Kind() == reflect.Ptr {
		tblName = t.Elem().Name()
	} else {
		tblName = t.Name()
	}
	return strings.ToLower(tblName) + "s"
}

func (dam *BasicDAM) Delete(id string) error {
	err := CatchExecErr(dam.DB.Exec("delete from "+dam.TableName+" where id=$1", id))
	return err
}
