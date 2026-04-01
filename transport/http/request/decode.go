package request

import (
	"encoding/json"
	"io"
)

func DecodeJSON(r io.Reader, v any) error {
	defer io.Copy(io.Discard, r)
	return json.NewDecoder(r).Decode(v)
}
