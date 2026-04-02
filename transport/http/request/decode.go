package request

import (
	"encoding/json"
	"fmt"
	"io"
)

func DecodeJSON(r io.Reader, v any) error {

	const op = "core.transport.http.request.DecodeJSON"

	defer io.Copy(io.Discard, r)

	err := json.NewDecoder(r).Decode(v)

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}
