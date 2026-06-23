package agentclient

import (
	"encoding/json"
	"fmt"
	"io"
)

func WriteJSON(w io.Writer, value any, raw bool) error {
	encoder := json.NewEncoder(w)
	if !raw {
		encoder.SetIndent("", "  ")
	}
	return encoder.Encode(value)
}

func WriteNDJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	return encoder.Encode(value)
}

func WriteError(w io.Writer, message string) {
	_, _ = fmt.Fprintln(w, message)
}
