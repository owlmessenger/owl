package directory

import (
	"context"
	"errors"

	"github.com/gotvc/got/pkg/gotkv"
	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/exp/slices"

	"github.com/owlmessenger/owl/pkg/owldag"
)

var _ owldag.Scheme[State] = &Scheme{}

type Scheme struct {
	op    Operator
	peers []owldag.PeerID
}

func NewScheme(peers []owldag.PeerID) *Scheme {
	return &Scheme{
		op:    New(),
		peers: peers,
	}
}

func (p *Scheme) Validate(ctx context.Context, s cadata.Getter, consult owldag.ConsultFunc, x State) error {
	for _, peer := range p.peers {
		if consult(peer) {
			return nil
		}
	}
	return errors.New("directory: invalid state")
}

func (p *Scheme) ValidateStep(ctx context.Context, s cadata.Getter, consult owldag.ConsultFunc, prev, next State) error {
	return p.Validate(ctx, s, consult, next)
}

func (p *Scheme) Merge(ctx context.Context, s cadata.Store, xs []State) (*State, error) {
	return p.op.Merge(ctx, s, xs)
}

func (p *Scheme) Sync(ctx context.Context, src cadata.Getter, dst cadata.Store, x State) error {
	return p.op.gotkv.Sync(ctx, src, dst, x, func(gotkv.Entry) error { return nil })
}

func (p *Scheme) CanRead(ctx context.Context, s cadata.Getter, x State, peer owldag.PeerID) (bool, error) {
	return slices.Contains(p.peers, peer), nil
}

func (p *Scheme) ListPeers(ctx context.Context, s cadata.Getter, x State) ([]owldag.PeerID, error) {
	return p.peers, nil
}
