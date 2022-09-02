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

type NodeID = cadata.ID

// Node is an entry in the Feed.
// Also a Node/Vertex in the DAG
type Node struct {
	N        uint64        `json:"n"`
	Min      uint64        `json:"min"`
	Previous IDSet[NodeID] `json:"prevs"`
	Author   PeerID        `json:"author"`

	Salt     []byte        `json:"salt"`
	State json.RawMessage `json:"state"`
}

func Hash(x []byte) NodeID {
	return sha3.Sum256(x)
}

func NewNodeID(e Node) NodeID {
	return Hash(e.Marshal())
}

func ParseNode(data []byte) (*Node, error) {
	var x Node
	if err := json.Unmarshal(data, &x); err != nil {
		return nil, err
	}
	return &x, nil
}

func (n *Node) Marshal() []byte {
	data, err := json.Marshal(n)
	if err != nil {
		panic(err)
	}
	return data
}

func peerIDFromBytes(x []byte) (PeerID, error) {
	if len(x) != 32 {
		return PeerID{}, fmt.Errorf("wrong length for PeerID HAVE: %d WANT: %d", len(x), 32)
	}
	return inet256.AddrFromBytes(x), nil
}

func nodeIDFromBytes(x []byte) (NodeID, error) {
	if len(x) != 32 {
		return NodeID{}, fmt.Errorf("wrong length for NodeID HAVE: %d WANT: %d", len(x), 32)
	}
	return cadata.IDFromBytes(x), nil
}

func postNode(ctx context.Context, s cadata.Poster, n Node) (*NodeID, error) {
	id, err := s.Post(ctx, n.Marshal())
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// GetNode retreives the node with id from s.
func GetNode(ctx context.Context, s cadata.Getter, id NodeID) (*Node, error) {
	var node *Node
	if err := cadata.GetF(ctx, s, id, func(data []byte) error {
		var err error
		node, err = ParseNode(data)
		return err
	}); err != nil {
		return nil, err
	}
	return node, nil
}

func getAllNodes(ctx context.Context, s cadata.Getter, ids []NodeID) ([]Node, error) {
	nodes := make([]Node, len(ids))
	for i := range ids {
		id := ids[i]
		node, err := GetNode(ctx, s, id)
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
func ForEachDesc(ctx context.Context, s cadata.Store, ids []NodeID, fn func(NodeID, Node) error) error {
	nodes, err := getAllNodes(ctx, s, ids)
	if err != nil {
		return err
	}
	lt := func(a, b Node) bool {
		return a.N < b.N
	}
	for len(nodes) > 0 {
		var node Node
		node, nodes = heap.Pop(nodes, lt)
		if err := fn(NewNodeID(node), node); err != nil {
			return err
		}
		nodes2, err := getAllNodes(ctx, s, node.Previous)
		if err != nil {
			return err
		}
		for _, node := range nodes2 {
			var exists bool
			nodeID := NewNodeID(node)
			for _, existing := range nodes {
				if NewNodeID(existing) == nodeID {
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

type Pair struct {
	ID   NodeID
	Node Node
}

// ForEachDescGroup calls fn with all the nodes in the graph, reachable from ids, with a given value of N
// for each value of N descending down to 0.
func ForEachDescGroup(ctx context.Context, s cadata.Store, ids []NodeID, fn func(uint64, []Pair) error) error {
	var n uint64 = math.MaxUint64
	var group []Pair
	if err := ForEachDesc(ctx, s, ids, func(id NodeID, node Node) error {
		if group != nil && node.N < n {
			if err := fn(n, group); err != nil {
				return err
			}
			group = group[:0]
		}
		n = node.N
		group = append(group, Pair{ID: id, Node: node})
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

func findMaxN(nodes []Node) (int, uint64) {
	var max uint64
	index := -1
	for i := range nodes {
		if nodes[i].N > max {
			max = nodes[i].N
			index = i
		}
	}
	return index, max
}

// NearestUnity finds a point in the history of ids where a value of N has a single node.
func NearestUnity(ctx context.Context, s cadata.Store, ids IDSet[NodeID]) (*NodeID, error) {
	stopIter := errors.New("stop iteration")
	var ret *NodeID
	if err := ForEachDescGroup(ctx, s, ids, func(_ uint64, pairs []Pair) error {
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

func reachableFrom(ctx context.Context, s cadata.Getter, srcs IDSet[NodeID], target cadata.ID, targetN uint64) (bool, error) {
	if srcs.Contains(target) {
		return true, nil
	}
	nodes, err := getAllNodes(ctx, s, srcs)
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
