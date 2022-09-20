package directmsg

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/owlmessenger/owl/pkg/owldag"
	"golang.org/x/exp/slices"
)

var _ owldag.Scheme[State] = &Scheme{}

type Scheme struct {
	op       Operator
	getPeers func(ctx context.Context) ([]owldag.PeerID, error)
}

func NewScheme(getPeers func(ctx context.Context) ([]owldag.PeerID, error)) *Scheme {
	return &Scheme{
		op:       New(),
		getPeers: getPeers,
	}
}

func (p *Scheme) Validate(ctx context.Context, s cadata.Getter, consult owldag.ConsultFunc, x State) error {
	return p.op.Validate(ctx, s, consult, x)
}

func (p *Scheme) ValidateStep(ctx context.Context, s cadata.Getter, consult owldag.ConsultFunc, prev, next State) error {
	return p.Validate(ctx, s, consult, next)
}

func (p *Scheme) Merge(ctx context.Context, s cadata.Store, xs []State) (*State, error) {
	return p.op.Merge(ctx, s, xs)
}

func (p *Scheme) Sync(ctx context.Context, src cadata.Getter, dst cadata.Store, x State) error {
	return p.op.Sync(ctx, src, dst, x)
}

func (p *Scheme) CanRead(ctx context.Context, x State, peer owldag.PeerID) (bool, error) {
	peers, err := p.ListPeers(ctx, x)
	if err != nil {
		return false, err
	}
	return slices.Contains(peers, peer), nil
}

func (p *Scheme) ListPeers(ctx context.Context, x State) ([]owldag.PeerID, error) {
	return p.getPeers(ctx)
}
