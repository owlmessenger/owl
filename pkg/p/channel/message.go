package channel

import (
	"github.com/brendoncarroll/go-tai64"
	"github.com/owlmessenger/owl/pkg/feeds"
)

type Message struct {
	From      feeds.PeerID `json:"from"`
	Timestamp tai64.TAI64N `json:"ts"`
	Body      string       `json:"body"`
}
