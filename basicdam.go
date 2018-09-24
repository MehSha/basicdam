package basicdam

import (
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

type BasicDAM struct {
	DB       *sqlx.DB
	Instance interface{}
}

func (dam *BasicDAM) TableName() string {
	tblName := ""
	if t := reflect.TypeOf(dam.Instance); t.Kind() == reflect.Ptr {
		tblName = t.Elem().Name()
	} else {
		tblName = t.Name()
	}
	return strings.ToLower(tblName)
}
