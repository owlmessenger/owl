package cflog

import (
	"encoding/json"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-tai64"
	"github.com/owlmessenger/owl/pkg/feeds"
	"golang.org/x/crypto/sha3"
)

// Entry is a single item in the log
type Entry struct {
	Thread Path        `json:"thread,omitempty"`
	After  []cadata.ID `json:"after,omitempty"`

	Author    feeds.PeerID    `json:"a"`
	Timestamp tai64.TAI64N    `json:"ts"`
	Data      json.RawMessage `json:"data"`
}

func (e *Entry) AsString() (ret string, _ error) {
	err := e.into(&ret)
	return ret, err
}

func (e *Entry) into(x interface{}) error {
	return json.Unmarshal(e.Data, x)
}

func (ev *Entry) ID() (ret cadata.ID) {
	sha3.ShakeSum256(ret[:], jsonMarshal(ev))
	return ret
}

func (a *Entry) Lt(b *Entry) bool {
	if len(a.Thread) > 0 || len(b.Thread) > 0 {
		return PathCompare(a.Thread, b.Thread) < 0
	}
	if a.Timestamp != b.Timestamp {
		return a.Timestamp.Before(b.Timestamp)
	}
	return false
}

// Pair is a (Path, Entry) pair
type Pair struct {
	Path  Path  `json:"path"`
	Entry Entry `json:"entry"`
}

func jsonMarshal(x interface{}) []byte {
	data, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return data
}

func parseEntry(data []byte) (*Entry, error) {
	var ev Entry
	if err := json.Unmarshal(data, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}
