package redis

import "errors"

var ErrKeyNotExists = errors.New("key not in cache")

var ErrKeyExists = errors.New("key already exists")
