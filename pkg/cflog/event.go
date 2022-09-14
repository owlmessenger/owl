package cflog

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-tai64"
	"github.com/owlmessenger/owl/pkg/feeds"
	"golang.org/x/crypto/sha3"
	"golang.org/x/exp/slices"
)

type Path []uint64

func (p Path) Successor() (ret Path) {
	if len(p) == 0 {
		return Path{0}
	}
	ret = append(ret, p...)
	ret[len(ret)-1]++
	return ret
}

func (p Path) String() string {
	sb := strings.Builder{}
	for i := range p {
		if i > 0 {
			sb.WriteString(".")
		}
		fmt.Fprintf(&sb, "%x", p[i])
	}
	return sb.String()
}

func PathCompare(a, b Path) int {
	return slices.Compare(a, b)
}

// Event is a single item in the log
type Event struct {
	Thread Path        `json:"thread,omitempty"`
	After  []cadata.ID `json:"after,omitempty"`

	Author    feeds.PeerID    `json:"a"`
	Timestamp tai64.TAI64N    `json:"ts"`
	Data      json.RawMessage `json:"data"`
}

func (e *Event) AsString() (ret string, _ error) {
	err := e.into(&ret)
	return ret, err
}

func (e *Event) into(x interface{}) error {
	return json.Unmarshal(e.Data, x)
}

func (ev *Event) ID() (ret cadata.ID) {
	sha3.ShakeSum256(ret[:], jsonMarshal(ev))
	return ret
}

func (a *Event) Lt(b *Event) bool {
	if len(a.Thread) > 0 || len(b.Thread) > 0 {
		return PathCompare(a.Thread, b.Thread) < 0
	}
	if a.Timestamp != b.Timestamp {
		return a.Timestamp.Before(b.Timestamp)
	}
	return false
}

// Pair is a (Path, Event) pair
type Pair struct {
	Path  Path  `json:"path"`
	Event Event `json:"event"`
}

func jsonMarshal(x interface{}) []byte {
	data, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return data
}

func parseEvent(data []byte) (*Event, error) {
	var ev Event
	if err := json.Unmarshal(data, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}
