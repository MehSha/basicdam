package basicdam

import (
	"github.com/pkg/errors"
)

var ErrDB = errors.New("Database Operation Failed")
var ErrNotFound = errors.New("Entity Not Found")
var ErrFormat = errors.New("Data format is Wrong")
var ErrInvalid = errors.New("Data is invalid")
