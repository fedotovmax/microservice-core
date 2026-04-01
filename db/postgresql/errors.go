package postgresql

import "errors"

var ErrWantToCallMethodsAfterInitPool = errors.New("you will be able to call database pool methods only after the connection has been created and initialized")

var ErrNoRows = errors.New("no rows")
