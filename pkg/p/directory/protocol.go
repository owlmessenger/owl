package directory

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/owlmessenger/owl/pkg/feeds"
	"golang.org/x/exp/slices"
)

var _ feeds.Protocol[State] = &Protocol{}

type Protocol struct {
	op    Operator
	store cadata.Store
	peers []feeds.PeerID
}

func NewProtocol(s cadata.Store, peers []feeds.PeerID) *Protocol {
	return &Protocol{
		op:    New(),
		store: s,
		peers: peers,
	}
}

func (p *Protocol) Merge(ctx context.Context, xs []State) (State, error) {
	return p.op.Merge(ctx, p.store, xs)
}

func (p *Protocol) Validate(ctx context.Context, author feeds.PeerID, prev, next State) error {
	return p.op.Validate(ctx, p.store, author, prev, next)
}

func (p *Protocol) CanRead(ctx context.Context, x State, peer feeds.PeerID) (bool, error) {
	return slices.Contains(p.peers, peer), nil
}

func (p *Protocol) ListPeers(ctx context.Context, x State) ([]feeds.PeerID, error) {
	return p.peers, nil
}
