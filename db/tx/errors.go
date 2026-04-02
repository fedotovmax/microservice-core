package tx

import "errors"

var ErrConnRequiredForTx = errors.New("pool connection is required for create transaction manager")

var ErrManagerIsNotInit = errors.New("manager is not initialized: call New() before using methods")

var ErrTxClosed = errors.New("transaction already closed")
