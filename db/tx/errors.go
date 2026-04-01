package tx

import "errors"

var ErrConnRequiredForTx = errors.New("pool connection is required for create transaction manager")
var ErrTxClosed = errors.New("transaction already closed")
