package channel

import (
	"encoding/json"

	"github.com/brendoncarroll/go-tai64"
	"github.com/owlmessenger/owl/pkg/feeds"
	"golang.org/x/exp/slices"
)

type EventID []uint64

func (mid EventID) Next(i int) (ret EventID) {
	ret = append(ret, mid...)
	ret[i]++
	return ret
}

func IDCompare(a, b EventID) int {
	return slices.Compare(a, b)
}

type Event struct {
	From      feeds.PeerID `json:"from"`
	Timestamp tai64.TAI64N `json:"ts"`

	Message     json.RawMessage `json:"msg,omitempty"`
	PeerAdded   *feeds.PeerID   `json:"peer_add,omitempty"`
	PeerRemoved *feeds.PeerID   `json:"peer_remove,omitempty"`
}

// Pair is an (ID, Event) pair
type Pair struct {
	ID    EventID `json:"id"`
	Event Event   `json:"event"`
}
