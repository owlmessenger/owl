package feeds

import (
	"context"
	"fmt"
	"math"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/owlmessenger/owl/pkg/feeds/internal/wire"
	"github.com/owlmessenger/owl/pkg/heap"
	"golang.org/x/crypto/sha3"
	"google.golang.org/protobuf/proto"
)

const MaxNodeSize = 1 << 16

type NodeID = cadata.ID

// Node is an entry in the Feed.
// Also a Node/Vertex in the DAG
type Node struct {
	N        uint64
	Previous IDSet[NodeID]
	Author   PeerID

	Init       *Init
	AddPeer    *AddPeer
	RemovePeer *RemovePeer
	Data       []byte
}

type Init struct {
	Peers IDSet[PeerID]
	Salt  [32]byte
}

type AddPeer struct {
	Peer PeerID
}

type RemovePeer struct {
	Peer PeerID
}

func Hash(x []byte) NodeID {
	return sha3.Sum256(x)
}

func NewNodeID(e Node) NodeID {
	return Hash(e.Marshal())
}

func ParseNode(data []byte) (*Node, error) {
	var x wire.Node
	if err := proto.Unmarshal(data, &x); err != nil {
		return nil, err
	}
	var err error
	n := &Node{
		N: x.N,
	}
	// author
	if n.Author, err = peerIDFromBytes(x.Author); err != nil {
		return nil, err
	}
	// previous
	for i := range x.Previous {
		prev, err := nodeIDFromBytes(x.Previous[i])
		if err != nil {
			return nil, err
		}
		n.Previous = append(n.Previous, prev)
	}
	switch v := x.Value.(type) {
	case *wire.Node_Init:
		var peers []PeerID
		for i := range v.Init.Peers {
			peerID, err := peerIDFromBytes(v.Init.Peers[i])
			if err != nil {
				return nil, err
			}
			peers = append(peers, peerID)
		}
		var salt [32]byte
		copy(salt[:], v.Init.Salt)
		n.Init = &Init{
			Salt:  salt,
			Peers: peers,
		}
	case *wire.Node_AddPeer:
		peerID, err := peerIDFromBytes(v.AddPeer.Peer)
		if err != nil {
			return nil, err
		}
		n.AddPeer = &AddPeer{Peer: peerID}
	case *wire.Node_RemovePeer:
		peerID, err := peerIDFromBytes(v.RemovePeer.Peer)
		if err != nil {
			return nil, err
		}
		n.RemovePeer = &RemovePeer{Peer: peerID}
	case *wire.Node_Data:
		n.Data = v.Data
		if n.Data == nil {
			n.Data = []byte{}
		}
	default:
		return nil, fmt.Errorf("node cannot be empty")
	}
	return n, nil
}

func (n *Node) Marshal() []byte {
	// TODO: actual deterministic serialization
	m := proto.MarshalOptions{
		Deterministic: true,
	}
	var previous [][]byte
	for i := range n.Previous {
		previous = append(previous, n.Previous[i][:])
	}
	w := &wire.Node{
		N:        n.N,
		Author:   n.Author[:],
		Previous: previous,
	}
	switch {
	case n.Init != nil:
		var peers [][]byte
		for i := range n.Init.Peers {
			peers = append(peers, n.Init.Peers[i][:])
		}
		w.Value = &wire.Node_Init{Init: &wire.Init{
			Salt:  n.Init.Salt[:],
			Peers: peers,
		}}
	case n.AddPeer != nil:
		w.Value = &wire.Node_AddPeer{AddPeer: &wire.AddPeer{
			Peer: n.AddPeer.Peer[:],
		}}
	case n.RemovePeer != nil:
		w.Value = &wire.Node_RemovePeer{RemovePeer: &wire.RemovePeer{
			Peer: n.RemovePeer.Peer[:],
		}}
	case n.Data != nil:
		w.Value = &wire.Node_Data{Data: n.Data}
	default:
		panic("empty node")
	}
	data, err := m.Marshal(w)
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

func PostNode(ctx context.Context, s cadata.Poster, n Node) (*NodeID, error) {
	id, err := s.Post(ctx, n.Marshal())
	if err != nil {
		return nil, err
	}
	return &id, nil
}

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
