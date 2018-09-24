package basicdam

import (
	"reflect"

	"github.com/jmoiron/sqlx"
)

type BasicDAM struct {
	DB       *sqlx.DB
	Instance interface{}
}

func (dam *BasicDAM) TableName() string {
	if t := reflect.TypeOf(dam.Instance); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}
