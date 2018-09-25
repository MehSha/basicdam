package basicdam

import (
	"database/sql"

	"github.com/pkg/errors"
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
