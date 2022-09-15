package cflog

import (
	"encoding/json"
	"fmt"
)

type ErrEntryLen struct {
	Data json.RawMessage
}

func (e ErrEntryLen) Error() string {
	return fmt.Sprintf("message exceeds maximum length (%d > %d)", len(e.Data), MaxEntryLen)
}
