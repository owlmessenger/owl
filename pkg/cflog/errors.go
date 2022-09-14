package cflog

import (
	"encoding/json"
	"fmt"
)

type ErrMessageLen struct {
	Data json.RawMessage
}

func (e ErrMessageLen) Error() string {
	return fmt.Sprintf("message exceeds maximum length (%d > %d)", len(e.Data), MaxMessageLen)
}
