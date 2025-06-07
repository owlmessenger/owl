package owldag

import (
	"context"
	"encoding/json"
	"errors"
	"math"

	"github.com/gotvc/got/pkg/gotkv"
	"github.com/owlmessenger/owl/src/internal/slices2"
	"go.brendoncarroll.net/exp/heaps"
	"go.brendoncarroll.net/state/cadata"
	"go.inet256.org/inet256/pkg/inet256"
	"golang.org/x/crypto/sha3"
)

const MaxNodeSize = 1 << 16

type Ref = cadata.ID

func Hash(x []byte) (ret Ref) {
	sha3.ShakeSum256(ret[:], x)
	return ret
}

func NewRef[T any](e Node[T]) Ref {
	return Hash(e.Marshal())
}

func RefFromBytes(x []byte) Ref {
	return cadata.IDFromBytes(x)
}

// Node is a Node/Vertex in the DAG
type Node[T any] struct {
	N        uint64     `json:"n"`
	Previous IDSet[Ref] `json:"prevs"`

	Salt  []byte     `json:"salt,omitempty"`
	State T          `json:"state"`
	Sigs  gotkv.Root `json:"sigs"`
}

func ParseNode[T any](data []byte) (*Node[T], error) {
	var x Node[T]
	if err := json.Unmarshal(data, &x); err != nil {
		return nil, err
	}
	return &x, nil
}

func (n *Node[T]) Marshal() []byte {
	data, err := json.Marshal(n)
	if err != nil {
		panic(err)
	}
	return data
}

func PostNode[T any](ctx context.Context, s cadata.Poster, n Node[T]) (*Ref, error) {
	data := n.Marshal()
	if len(data) > MaxNodeSize {
		return nil, errors.New("owldag: node too big")
	}
	id, err := s.Post(ctx, data)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// GetNode retreives the node at ref from s
func GetNode[T any](ctx context.Context, s cadata.Getter, ref Ref) (*Node[T], error) {
	var node *Node[T]
	if err := cadata.GetF(ctx, s, ref, func(data []byte) error {
		var err error
		node, err = ParseNode[T](data)
		return err
	}); err != nil {
		return nil, err
	}
	return node, nil
}

// CheckNode runs context independent checks on the node.
// CheckNode:
// - ensures that all the nodes which are referenced by node exist.
// - that the node's N is exactly 1 greater than the max of the previous nodes.
// - is NOT recursive.  It is assumed that nodes in s are already valid.
func CheckNode[T any](ctx context.Context, s cadata.Getter, node Node[T]) error {
	if node.N > 0 && len(node.Previous) == 0 {
		return errors.New("nodes with N > 0 must reference another node")
	}
	if node.N == 0 && len(node.Salt) == 0 {
		return errors.New("initial node missing salt")
	}
	if node.N > 0 && node.Salt != nil {
		return errors.New("non-initial nodes cannot have a salt")
	}

	previous, err := getAllNodes[T](ctx, s, node.Previous)
	if err != nil {
		return err
	}
	maxN := slices2.FoldLeft(previous, 0, func(acc uint64, x Node[T]) uint64 {
		return max(acc, x.N)
	})
	expectedN := maxN + 1
	if node.N != expectedN {
		return ErrBadN[T]{Have: node.N, Want: expectedN, Node: node}
	}
	return nil
}

func getAllNodes[T any](ctx context.Context, s cadata.Getter, ids []Ref) ([]Node[T], error) {
	nodes := make([]Node[T], len(ids))
	for i := range ids {
		id := ids[i]
		node, err := GetNode[T](ctx, s, id)
		if err != nil {
			return nil, err
		}
		nodes[i] = *node
	}
	return nodes, nil
}

type PeerID = inet256.ID

// ForEachDesc traverses the DAG in descending order of N, starting with ids.
// Nodes with the same value of N can be visited in any order.
func ForEachDesc[T any](ctx context.Context, s cadata.Getter, ids []Ref, fn func(Ref, Node[T]) error) error {
	nodes, err := getAllNodes[T](ctx, s, ids)
	if err != nil {
		return err
	}
	lt := func(a, b Node[T]) bool {
		return a.N < b.N
	}
	for len(nodes) > 0 {
		var node Node[T]
		node, nodes = heaps.PopFunc(nodes, lt)
		if err := fn(NewRef(node), node); err != nil {
			return err
		}
		nodes2, err := getAllNodes[T](ctx, s, node.Previous)
		if err != nil {
			return err
		}
		for _, node := range nodes2 {
			var exists bool
			nodeID := NewRef(node)
			for _, existing := range nodes {
				if NewRef(existing) == nodeID {
					exists = true
					break
				}
			}
			if !exists {
				nodes = heaps.PushFunc(nodes, node, lt)
			}
		}
	}
	return nil
}

type Pair[T any] struct {
	ID   Ref
	Node Node[T]
}

// ForEachDescGroup calls fn with all the nodes in the graph, reachable from ids, with a given value of N
// for each value of N descending down to 0.
func ForEachDescGroup[T any](ctx context.Context, s cadata.Getter, ids []Ref, fn func(uint64, []Pair[T]) error) error {
	var n uint64 = math.MaxUint64
	var group []Pair[T]
	if err := ForEachDesc(ctx, s, ids, func(id Ref, node Node[T]) error {
		if group != nil && node.N < n {
			if err := fn(n, group); err != nil {
				return err
			}
			group = group[:0]
		}
		n = node.N
		group = append(group, Pair[T]{ID: id, Node: node})
		return nil
	}); err != nil {
		return err
	}
	if group != nil {
		if err := fn(n, group); err != nil {
			return err
		}
	}
	return nil
}

// AnyHasAncestor determines if any of srcs have an ancestor target.
// Nodes are considered to be their own ancestor
func AnyHasAncestor(ctx context.Context, s cadata.Getter, srcs IDSet[Ref], ancRef cadata.ID) (bool, error) {
	target, err := GetNode[json.RawMessage](ctx, s, ancRef)
	if err != nil {
		return false, err
	}
	return hasAncestor(ctx, s, srcs, ancRef, target.N)
}

// HasAncestors returns true if srcRef has ancRef as an ancestor.
// Nodes are considered to be their own ancestor.
func HasAncestor(ctx context.Context, s cadata.Getter, srcRef Ref, ancRef cadata.ID) (bool, error) {
	target, err := GetNode[json.RawMessage](ctx, s, ancRef)
	if err != nil {
		return false, err
	}
	return hasAncestor(ctx, s, NewIDSet(srcRef), ancRef, target.N)
}

func hasAncestor(ctx context.Context, s cadata.Getter, srcs IDSet[Ref], target Ref, targetN uint64) (bool, error) {
	if srcs.Contains(target) {
		return true, nil
	}
	nodes, err := getAllNodes[json.RawMessage](ctx, s, srcs)
	if err != nil {
		return false, err
	}
	for _, node := range nodes {
		if node.N <= targetN {
			continue
		}
		yes, err := hasAncestor(ctx, s, node.Previous, target, targetN)
		if err != nil {
			return false, err
		} else if yes {
			return true, nil
		}
	}
	return false, nil
}

// NCA finds the nearest common ancestor of xs
func NCA(ctx context.Context, s cadata.Getter, xs []Ref) (*Ref, error) {
	panic("not implemented")
}
