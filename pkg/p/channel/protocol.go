package channel

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/owlmessenger/owl/pkg/feeds"
)

var _ feeds.Protocol[State] = &Protocol{}

type Protocol struct {
	op    Operator
	store cadata.Store
}

func NewProtocol(store cadata.Store) *Protocol {
	return &Protocol{
		op:    New(),
		store: store,
	}
}

func (p *Protocol) CanRead(ctx context.Context, x State, peer feeds.PeerID) (bool, error) {
	panic("not implemented")
}

func (p *Protocol) Validate(ctx context.Context, author feeds.PeerID, prev, next State) error {
	panic("not implemented")
}

func (p *Protocol) Merge(ctx context.Context, xs []State) (State, error) {
	panic("not implemented")
}

func (p *Protocol) ListPeers(ctx context.Context, x State) ([]feeds.PeerID, error) {
	panic("not implemented")
}
