package feeds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/owlmessenger/owl/pkg/heap"
	"golang.org/x/crypto/sha3"
)

const MaxNodeSize = 1 << 16

type Ref = cadata.ID

// Node is an entry in the Feed.
// Also a Node/Vertex in the DAG
type Node[T any] struct {
	N        uint64     `json:"n"`
	Previous IDSet[Ref] `json:"prevs"`
	Author   PeerID     `json:"author,omitempty"`

	Salt  []byte `json:"salt,omitempty"`
	State T      `json:"state"`
}

func Hash(x []byte) Ref {
	return sha3.Sum256(x)
}

func NewRef[T any](e Node[T]) Ref {
	return Hash(e.Marshal())
}

func IDFromBytes(x []byte) Ref {
	return cadata.IDFromBytes(x)
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
	id, err := s.Post(ctx, n.Marshal())
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// GetNode retreives the node with id from s.
func GetNode[T any](ctx context.Context, s cadata.Getter, id Ref) (*Node[T], error) {
	var node *Node[T]
	if err := cadata.GetF(ctx, s, id, func(data []byte) error {
		var err error
		node, err = ParseNode[T](data)
		return err
	}); err != nil {
		return nil, err
	}
	if node.N > 0 && node.Author.IsZero() {
		return nil, fmt.Errorf("node is missing author")
	}
	if node.N == 0 && !node.Author.IsZero() {
		return nil, fmt.Errorf("initial node should not have author")
	}
	return node, nil
}

// CheckNode runs context independent checks on the node.
func CheckNode[T any](ctx context.Context, s cadata.Getter, node Node[T]) error {
	if node.N > 0 && len(node.Previous) == 0 {
		return errors.New("nodes with N > 0 must reference another node")
	}
	previous, err := getAllNodes[T](ctx, s, node.Previous)
	if err != nil {
		return err
	}
	// Check N
	var expectedN uint64
	for _, node2 := range previous {
		if node2.N+1 > expectedN {
			expectedN = node2.N + 1
		}
	}
	if node.N != expectedN {
		return ErrBadN[T]{Have: node.N, Want: expectedN, Node: node}
	}
	if err := checkSenders(previous); err != nil {
		return err
	}
	return nil
}

// checkSenders ensures that each Node in previous has a unique sender.
func checkSenders[T any](previous []Node[T]) error {
	senders := map[PeerID]struct{}{}
	for _, node := range previous {
		if _, exists := senders[node.Author]; exists {
			return errors.New("a valid set of heads can only contain one head from each sender")
		}
		senders[node.Author] = struct{}{}
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
func ForEachDesc[T any](ctx context.Context, s cadata.Store, ids []Ref, fn func(Ref, Node[T]) error) error {
	nodes, err := getAllNodes[T](ctx, s, ids)
	if err != nil {
		return err
	}
	lt := func(a, b Node[T]) bool {
		return a.N < b.N
	}
	for len(nodes) > 0 {
		var node Node[T]
		node, nodes = heap.Pop(nodes, lt)
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
				nodes = heap.Push(nodes, node, lt)
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
func ForEachDescGroup[T any](ctx context.Context, s cadata.Store, ids []Ref, fn func(uint64, []Pair[T]) error) error {
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

// NearestUnity finds a point in the history of ids where a value of N has a single node.
func NearestUnity(ctx context.Context, s cadata.Store, ids IDSet[Ref]) (*Ref, error) {
	stopIter := errors.New("stop iteration")
	var ret *Ref
	if err := ForEachDescGroup(ctx, s, ids, func(_ uint64, pairs []Pair[json.RawMessage]) error {
		if len(pairs) == 1 {
			ret = &pairs[0].ID
			return stopIter
		}
		return nil
	}); err != nil && !errors.Is(err, stopIter) {
		return nil, err
	}
	return ret, nil
}

func reachableFrom(ctx context.Context, s cadata.Getter, srcs IDSet[Ref], target cadata.ID, targetN uint64) (bool, error) {
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
		yes, err := reachableFrom(ctx, s, node.Previous, target, targetN)
		if err != nil {
			return false, err
		}
		if yes {
			return true, nil
		}
	}
	return false, nil
}
