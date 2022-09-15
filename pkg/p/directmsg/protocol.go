package directmsg

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/owlmessenger/owl/pkg/feeds"
	"golang.org/x/exp/slices"
)

var _ feeds.Protocol[State] = &Protocol{}

type Protocol struct {
	op       Operator
	store    cadata.Store
	getPeers func(ctx context.Context) ([]feeds.PeerID, error)
}

func NewProtocol(s cadata.Store, getPeers func(ctx context.Context) ([]feeds.PeerID, error)) *Protocol {
	return &Protocol{
		op:       New(),
		store:    s,
		getPeers: getPeers,
	}
}

func (p *Protocol) CanRead(ctx context.Context, x State, peer feeds.PeerID) (bool, error) {
	peers, err := p.ListPeers(ctx, x)
	if err != nil {
		return false, err
	}
	return slices.Contains(peers, peer), nil
}

func (p *Protocol) ListPeers(ctx context.Context, x State) ([]feeds.PeerID, error) {
	return p.getPeers(ctx)
}

func (p *Protocol) Validate(ctx context.Context, author feeds.PeerID, prev, next State) error {
	return p.op.Validate(ctx, p.store, author, prev, next)
}

func (p *Protocol) Merge(ctx context.Context, xs []State) (State, error) {
	return p.op.Merge(ctx, p.store, xs)
}
