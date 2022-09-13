package feeds

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/owlmessenger/owl/pkg/slices2"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type ID = cadata.ID

type Protocol[S any] interface {
	// Validate checks that the transition from prev to next is valid
	Validate(ctx context.Context, author PeerID, prevs, next S) error

	// Merge combines multiple states into 1.
	Merge(ctx context.Context, states []S) (S, error)

	// CanRead returns true if id is allowed to read the feed with state X.
	CanRead(ctx context.Context, x S, id PeerID) (bool, error)

	// ListPeers returns the peers who should receive updates
	ListPeers(ctx context.Context, x S) ([]PeerID, error)
}

type Feed[T any] struct {
	protocol Protocol[T]
	store    cadata.Store

	state State[T]
}

func New[T any](protocol Protocol[T], state State[T], store cadata.Store) *Feed[T] {
	return &Feed[T]{
		protocol: protocol,
		store:    store,
		state:    state,
	}
}

// NewInit returns a new initial feed
func NewInit[T any](ctx context.Context, protocol Protocol[T], s cadata.Store, init T) (*Feed[T], error) {
	state, err := InitialState(ctx, s, init, nil)
	if err != nil {
		return nil, err
	}
	return &Feed[T]{
		protocol: protocol,
		state:    *state,
	}, nil
}

// GetRoot returns the ID of the root of the feed.
func (f *Feed[T]) GetRoot() ID {
	return f.state.ID
}

// SetHeads sets the heads for peer to heads
// SetHeads will fail if the structure transitively reachable by heads contains dangling references.
// Use AddNode to ensure that the nodes exist.
func (f *Feed[T]) SetHeads(ctx context.Context, heads []Ref) error {
	// ensure all the ids refer to valid nodes.
	nodes, err := getAllNodes[T](ctx, f.store, heads)
	if err != nil {
		return err
	}
	if err := checkSenders(nodes); err != nil {
		return err
	}
	f.state.Heads = append(f.state.Heads[:0], heads...)
	return nil
}

// GetHeads returns the source nodes for peer
func (f *Feed[T]) GetHeads() IDSet[Ref] {
	return f.state.Heads
}

func (f *Feed[T]) GetState() State[T] {
	return State[T]{
		ID:    f.state.ID,
		Heads: slices.Clone(f.state.Heads),

		Max:   f.state.Max,
		State: f.state.State,
		PeerN: maps.Clone(f.state.PeerN),
	}
}

// AddNode checks that the node is valid, which entails checking it only references valid nodes,
// and then posts the node to s.
// AddNode assumes that s only contains valid nodes.
func (f *Feed[T]) AddNode(ctx context.Context, from PeerID, node Node[T]) error {
	if err := f.CheckNode(ctx, f.store, from, node); err != nil {
		return err
	}
	nodes, err := getAllNodes[T](ctx, f.store, node.Previous)
	if err != nil {
		return err
	}
	states := slices2.Map(nodes, func(x Node[T]) T { return x.State })
	prevState, err := f.protocol.Merge(ctx, states)
	if err != nil {
		return err
	}
	if err := f.protocol.Validate(ctx, from, prevState, node.State); err != nil {
		return err
	}
	_, err = PostNode(ctx, f.store, node)
	return err
}

// Modify calls fn to modify the state of the feed.
func (f *Feed[T]) Modify(ctx context.Context, actor PeerID, fn func(prev []Ref, current T) (*T, error)) error {
	prevIDs := append([]Ref{}, f.state.Heads...)
	prevState := f.state.State
	nextState, err := fn(prevIDs, prevState)
	if err != nil {
		return err
	}
	if err := f.protocol.Validate(ctx, actor, prevState, *nextState); err != nil {
		return err
	}
	y, err := Modify(ctx, f.store, f.state, actor, *nextState)
	if err != nil {
		return err
	}
	f.state = *y
	return nil
}

func (f *Feed[T]) View(ctx context.Context, s cadata.Store, actor PeerID) (*T, error) {
	prevNodes, err := getAllNodes[T](ctx, s, f.state.Heads)
	if err != nil {
		return nil, err
	}
	prevStates := slices2.Map(prevNodes, func(x Node[T]) T { return x.State })
	currentState, err := f.protocol.Merge(ctx, prevStates)
	if err != nil {
		return nil, err
	}
	if yes, err := f.protocol.CanRead(ctx, currentState, actor); err != nil {
		return nil, err
	} else if !yes {
		return nil, fmt.Errorf("peer %v is not allowed to view feed", actor)
	}
	return &currentState, nil
}

// CheckNode determines if the entry can be applied to the feed in its current state.
func (f *Feed[T]) CheckNode(ctx context.Context, s cadata.Getter, from PeerID, node Node[T]) error {
	if err := CheckNode(ctx, s, node); err != nil {
		return err
	}
	prevNodes, err := getAllNodes[T](ctx, s, node.Previous)
	if err != nil {
		return err
	}
	prevStates := slices2.Map(prevNodes, func(x Node[T]) T { return x.State })
	prevState, err := f.protocol.Merge(ctx, prevStates)
	if err != nil {
		return err
	}
	if err := f.protocol.Validate(ctx, from, prevState, node.State); err != nil {
		return err
	}
	return nil
}

func (f *Feed[T]) SyncHeads(ctx context.Context, dst cadata.Store, src cadata.Getter, heads []Ref) error {
	for _, id := range heads {
		exists, err := cadata.Exists(ctx, dst, id)
		if err != nil {
			return err
		}
		if !exists {
			node, err := GetNode[T](ctx, src, id)
			if err != nil {
				return err
			}
			if err := f.SyncHeads(ctx, dst, src, node.Previous); err != nil {
				return err
			}
			if err := cadata.Copy(ctx, dst, src, id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *Feed[T]) CanRead(ctx context.Context, peer PeerID) (bool, error) {
	return f.protocol.CanRead(ctx, f.state.State, peer)
}
