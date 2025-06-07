package owl

import (
	"database/sql/driver"
	"encoding/binary"
	"strconv"
	"strings"
)

// Path identifies an event within a channel
// Paths will always have a length > 0.
// If the length is > 1, then all but the last element are considered the ThreadID
type Path []uint64

func (p Path) Value() (driver.Value, error) {
	return p.Marshal(), nil
}

// ThreadID is the component of the index which referes to a thread.
// ThreadID will be nil for messages in the root.
func (mi Path) ThreadID() []uint64 {
	l := len(mi)
	return mi[:l]
}

func (mi Path) Marshal() []byte {
	out := make([]byte, len(mi)*8)
	for i := range mi {
		binary.BigEndian.PutUint64(out[i*8:], mi[i])
	}
	return out
}

func (mi Path) String() string {
	sb := strings.Builder{}
	for i, n := range mi {
		if i > 0 {
			sb.WriteString(".")
		}
		sb.WriteString(strconv.FormatUint(n, 10))
	}
	return sb.String()
}
