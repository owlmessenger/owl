package persona

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/owlmessenger/owl/pkg/feeds"
)

var _ feeds.Protocol[State] = &Protocol{}

type Protocol struct {
	op    *Operator
	store cadata.Store
}

func NewProtocol(store cadata.Store) *Protocol {
	kvop := gotkv.NewOperator(1<<12, 1<<16)
	return &Protocol{
		op: New(&kvop),
	}
}

func (p *Protocol) Merge(ctx context.Context, xs []State) (State, error) {
	return p.op.Merge(ctx, xs)
}

func (p *Protocol) Validate(ctx context.Context, author feeds.PeerID, prev, next State) error {
	return p.op.Validate(ctx, p.store, author, prev, next)
}

func (p *Protocol) CanRead(ctx context.Context, x State, id feeds.PeerID) (bool, error) {
	return true, nil
}

func (p *Protocol) ListPeers(ctx context.Context, x State) ([]feeds.PeerID, error) {
	return nil, nil
}
