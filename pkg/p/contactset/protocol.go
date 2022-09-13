package contactset

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/owlmessenger/owl/pkg/feeds"
	"golang.org/x/exp/slices"
)

var _ feeds.Protocol[State] = &Protocol{}

type Protocol struct {
	op    Operator
	store cadata.Store
	peers []PeerID
}

func NewProtocol(store cadata.Store, peers []PeerID) *Protocol {
	return &Protocol{
		op:    New(),
		store: store,
		peers: peers,
	}
}

func (p *Protocol) CanRead(ctx context.Context, x State, peer feeds.PeerID) (bool, error) {
	return slices.Contains(p.peers, peer), nil
}

func (p *Protocol) Validate(ctx context.Context, author feeds.PeerID, prev, next State) error {
	if !slices.Contains(p.peers, author) {
		return fmt.Errorf("contactset: invalid transition")
	}
	return nil
}

func (p *Protocol) Merge(ctx context.Context, xs []State) (State, error) {
	return p.op.Merge(ctx, p.store, xs)
}

func (p *Protocol) ListPeers(ctx context.Context, x State) ([]PeerID, error) {
	return p.peers, nil
}
