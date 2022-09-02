package memberset

import (
	"context"
	"errors"
	"fmt"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/owlmessenger/owl/pkg/feeds"
)

type PeerID = feeds.PeerID

type Peer struct {
	ID       PeerID
	CanWrite bool
}

type State = gotkv.Root

type Operator struct {
	gotkv *gotkv.Operator
}

func New(kvop *gotkv.Operator) Operator {
	return Operator{gotkv: kvop}
}

func (o *Operator) NewEmpty(ctx context.Context, s cadata.Store, peers []PeerID) (*State, error) {
	b := o.gotkv.NewBuilder(s)
	for _, peer := range peers {
		if err := b.Put(ctx, peer[:], nil); err != nil {
			return nil, err
		}
	}
	return b.Finish(ctx)
}

func (o *Operator) Get(ctx context.Context, s cadata.Store, x State, id PeerID) (*Peer, error) {
	panic("")
}

func (o *Operator) AddPeers(ctx context.Context, s cadata.Store, x State, peers []Peer, nonce feeds.NodeID) (*State, error) {
	var muts []gotkv.Mutation
	for _, peer := range peers {
		if exists, err := o.Exists(ctx, s, x, peer.ID); err != nil {
			return nil, err
		} else if exists {
			return nil, fmt.Errorf("peer already exists")
		}
		k := appendAddKey(nil, peer.ID, nonce)
		mut := gotkv.Mutation{
			Span:    gotkv.SingleKeySpan(k),
			Entries: []gotkv.Entry{{Key: k}},
		}
		muts = append(muts, mut)
	}
	return o.gotkv.Mutate(ctx, s, x, muts...)
}

func (o *Operator) RemovePeers(ctx context.Context, s cadata.Store, x State, peers []PeerID) (*State, error) {
	var muts []gotkv.Mutation
	for _, peerID := range peers {
		if err := o.gotkv.ForEach(ctx, s, x, gotkv.PrefixSpan(appendAddPrefix(nil, peerID)), func(ent gotkv.Entry) error {
			var nonce feeds.NodeID
			copy(nonce[:], ent.Value)
			k := appendRemoveKey(nil, peerID, nonce)
			mut := gotkv.Mutation{
				Span:    gotkv.SingleKeySpan(k),
				Entries: []gotkv.Entry{{Key: k, Value: nonce[:]}},
			}
			muts = append(muts, mut)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return o.gotkv.Mutate(ctx, s, x, muts...)
}

func (o *Operator) Exists(ctx context.Context, s cadata.Store, x State, peer PeerID) (bool, error) {
	_, err := o.gotkv.Get(ctx, s, x, peer[:])
	if errors.Is(err, state.ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

func (o *Operator) List(ctx context.Context, s cadata.Store, x State, peer PeerID) ([]PeerID, error) {
	return nil, nil
}

// Validate determines if the transision is valid
func (o *Operator) Validate(ctx context.Context, s cadata.Store, authorID PeerID, prev, next State) error {
	peer, err := o.Get(ctx, s, prev, authorID)
	if err != nil {
		return err
	}
	if !peer.CanWrite {
		return fmt.Errorf("%v cannot modify members", authorID)
	}
	return nil
}

func (o *Operator) Merge(ctx context.Context, s cadata.Store, xs []State) (*State, error) {
	var its []kvstreams.Iterator
	for _, x := range xs {
		its = append(its, o.gotkv.NewIterator(s, x, gotkv.TotalSpan()))
	}
	m := kvstreams.NewMerger(s, its)
	b := o.gotkv.NewBuilder(s)
	if err := gotkv.CopyAll(ctx, b, m); err != nil {
		return nil, err
	}
	return b.Finish(ctx)
}

func appendInfoKey(out []byte, x PeerID) []byte {
	out = append(out, 'i')
	out = append(out, x[:]...)
	return out
}

func appendAddKey(out []byte, x PeerID, nonce feeds.NodeID) []byte {
	out = append(out, '+')
	out = append(out, x[:]...)
	out = append(out, nonce[:]...)
	return out
}

func appendAddPrefix(out []byte, x PeerID) []byte {
	out = append(out, '+')
	out = append(out, x[:]...)
	return out
}

func appendRemoveKey(out []byte, x PeerID, nonce feeds.NodeID) []byte {
	out = append(out, '-')
	out = append(out, x[:]...)
	out = append(out, nonce[:]...)
	return out
}
