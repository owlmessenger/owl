package cflog

import (
	"encoding/json"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-tai64"
	"github.com/owlmessenger/owl/pkg/owldag"
	"golang.org/x/crypto/sha3"
)

// wireEntry is the wire format for the log
type wireEntry struct {
	Parent *cadata.ID `json:"parent,omitempty"`

	Author    owldag.PeerID   `json:"a"`
	Timestamp tai64.TAI64N    `json:"ts"`
	Data      json.RawMessage `json:"data"`
}

func (e *wireEntry) AsString() (ret string, _ error) {
	err := e.into(&ret)
	return ret, err
}

func (e *wireEntry) into(x interface{}) error {
	return json.Unmarshal(e.Data, x)
}

func (ev *wireEntry) ID() (ret cadata.ID) {
	sha3.ShakeSum256(ret[:], jsonMarshal(ev))
	return ret
}

func (a *wireEntry) Lt(b *wireEntry) bool {
	if a.Timestamp != b.Timestamp {
		return a.Timestamp.Before(b.Timestamp)
	}
	return false
}

func jsonMarshal(x interface{}) []byte {
	data, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return data
}

func parseWireEntry(data []byte) (*wireEntry, error) {
	var ev wireEntry
	if err := json.Unmarshal(data, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}

func ltWireEntry(a, b *wireEntry) bool {
	if a.Timestamp != b.Timestamp {
		return a.Timestamp.Before(b.Timestamp)
	}
	return a.ID().Compare(b.ID()) < 0
}
