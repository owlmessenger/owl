package owldag

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/owlmessenger/owl/pkg/slices2"
	"golang.org/x/exp/constraints"
	"golang.org/x/sync/errgroup"
)

type (
	PrivateKey = inet256.PrivateKey
	PublicKey  = inet256.PublicKey
)

type DAG[T any] struct {
	gotkv                *gotkv.Operator
	scheme               Scheme[T]
	dagStore, innerStore cadata.Store

	state State[T]
}

// New creates a new DAG using dagStore to store nodes, and passing innerStore to
// the underlying scheme as needed.
func New[T any](sch Scheme[T], dagStore, innerStore cadata.Store, state State[T]) *DAG[T] {
	kvop := gotkv.NewOperator(1<<12, 1<<16)
	return &DAG[T]{
		gotkv:      &kvop,
		scheme:     sch,
		dagStore:   dagStore,
		innerStore: innerStore,

		state: state,
	}
}

func (d *DAG[T]) SaveBytes() []byte {
	return d.state.Marshal()
}

// Modify calls fn to modify the DAG's state.
func (d *DAG[T]) Modify(ctx context.Context, privKey PrivateKey, fn func(s cadata.Store, x T) (*T, error)) error {
	x := d.View()
	y, err := fn(d.innerStore, x)
	if err != nil {
		return err
	}
	id, err := PostNode(ctx, d.dagStore, Node[T]{
		N:        d.state.Max + 1,
		Previous: d.state.PrevRefs(),

		Sigs:  d.state.Heads,
		State: *y,
	})
	if err != nil {
		return err
	}
	h, err := NewHead(privKey, *id)
	if err != nil {
		return err
	}
	nextHeads, err := addHead(ctx, d.gotkv, d.dagStore, d.state.Heads, h)
	if err != nil {
		return err
	}
	d.state = State[T]{
		Max:    d.state.Max + 1,
		Prev:   []Head{h},
		X:      *y,
		Heads:  *nextHeads,
		Epochs: d.state.Epochs,
	}
	return nil
}

// View returns the DAG's state.
func (d *DAG[T]) View() T {
	return d.state.X
}

func (d *DAG[T]) GetHeads() []Head {
	return d.state.Prev
}

func (d *DAG[T]) GetHeadRefs() []Ref {
	return d.state.PrevRefs()
}

// AddHead adds a head from another peer.
func (d *DAG[T]) AddHead(ctx context.Context, h Head) error {
	if err := h.Verify(); err != nil {
		return err
	}
	// ensure that we can get the node. if we can then it was previously valid.
	node, err := GetNode[T](ctx, d.dagStore, h.Ref)
	if err != nil {
		return err
	}

	// Check if it is a repeat (already reachable from a head we know about)
	if yes, err := AnyHasAncestor(ctx, d.dagStore, d.state.PrevRefs(), h.Ref); err != nil {
		return err
	} else if yes {
		// exit early, don't need to do anything.
		return nil
	}

	var eg errgroup.Group
	eg.Go(func() error {
		// Check that this is not a replayed head.
		// Not allowed to add a head which is reachable from any previously signed head.
		return checkReplay(ctx, d.gotkv, d.dagStore, d.state.Heads, h)
	})
	var heads2 *gotkv.Root
	eg.Go(func() error {
		// update heads
		var err error
		heads2, err = addHead(ctx, d.gotkv, d.innerStore, d.state.Heads, h)
		return err
	})
	var prev2 []Head
	eg.Go(func() error {
		// create new previous
		// take all of the old previous nodes, and check if they are reachable from the new node
		// if they are then drop them
		for _, head := range d.state.Prev {
			if yes, err := AnyHasAncestor(ctx, d.dagStore, NewIDSet(h.Ref), head.Ref); err != nil {
				return err
			} else if !yes {
				prev2 = append(prev2, head)
			}
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}

	// remerge the state.
	prevNodes, err := getAllNodes[T](ctx, d.dagStore, slices2.Map(prev2, func(x Head) Ref { return x.Ref }))
	if err != nil {
		return err
	}
	xs := slices2.Map(prevNodes, func(x Node[T]) T { return x.State })
	x, err := d.scheme.Merge(ctx, d.innerStore, xs)
	if err != nil {
		return err
	}

	d.state = State[T]{
		Max:    max(d.state.Max, node.N),
		Prev:   prev2,
		Heads:  *heads2,
		X:      *x,
		Epochs: d.state.Epochs,
	}
	return nil
}

func (d *DAG[T]) GetEpoch(ctx context.Context) (*Ref, error) {
	return &d.state.Epochs[0], nil
	//return NCA(ctx, d.dagStore, d.state.PrevRefs())
}

// Pull takes a head and syncs the data structure from src.
func (d *DAG[T]) Pull(ctx context.Context, src cadata.Getter, h Head) error {
	if err := h.Verify(); err != nil {
		return err
	}
	if err := d.pullNode(ctx, src, h.Ref); err != nil {
		return err
	}
	return d.AddHead(ctx, h)
}

func (d *DAG[T]) pullNode(ctx context.Context, src cadata.Getter, ref Ref) error {
	if exists, err := cadata.Exists(ctx, d.dagStore, ref); err != nil {
		return err
	} else if exists {
		return nil
	}
	node, err := GetNode[T](ctx, src, ref)
	if err != nil {
		return err
	}
	eg := &errgroup.Group{}
	for _, prevRef := range node.Previous {
		prevRef := prevRef
		eg.Go(func() error {
			return d.pullNode(ctx, src, prevRef)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	if err := CheckNode(ctx, src, *node); err != nil {
		return err
	}
	// At this point all the previous nodes will be in s.dagStore

	if err := d.pullHeadTree(ctx, src, node.Sigs); err != nil {
		return err
	}

	// Validate using the schema
	consult := func(PeerID) bool {
		// TODO: check the heads tree
		return true
	}
	// TODO: not sure about if we have to merge here
	// Maybe we should document the guarantee that:
	//   Validate(Merge(x1 ... xn)) = Validate(x1) & ... & Validate(xn)
	eg, ctx2 := errgroup.WithContext(ctx)
	for _, prevRef := range node.Previous {
		prevRef := prevRef
		eg.Go(func() error {
			pn, err := GetNode[T](ctx2, src, prevRef)
			if err != nil {
				return err
			}
			return d.scheme.ValidateStep(ctx2, d.innerStore, consult, pn.State, node.State)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	_, err = PostNode(ctx, d.dagStore, *node)
	return err
}

func (d *DAG[T]) pullHeadTree(ctx context.Context, src cadata.Getter, x gotkv.Root) error {
	// TODO: validate
	return d.gotkv.Sync(ctx, src, d.dagStore, x, func(gotkv.Entry) error { return nil })
}

func (d *DAG[T]) ListPeers(ctx context.Context) ([]PeerID, error) {
	return d.scheme.ListPeers(ctx, d.innerStore, d.View())
}

func (d *DAG[T]) CanRead(ctx context.Context, peer PeerID) (bool, error) {
	return d.scheme.CanRead(ctx, d.dagStore, d.View(), peer)
}

func max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}
