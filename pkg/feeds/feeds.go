package feeds

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/brendoncarroll/go-state/cadata"
	"golang.org/x/exp/maps"
)

type Protocol[S any] interface {
	Validate(ctx context.Context, author PeerID, prevs, next S) error
	Merge(ctx context.Context, states []S) (S, error)
}

type State[S any] struct {
	ID    NodeID                   `json:"id"`
	Peers map[PeerID]*PeerState[S] `json:"peers"`
}

type Feed[S any] struct {
	state    State[S]
	protocol Protocol[S]
}

func NewInit[S any](ctx context.Context, s cadata.Store, init S) (*State[S], error) {
	salt := [32]byte{}
	if _, err := rand.Read(salt[:]); err != nil {
		return nil, err
	}
	state, err := json.Marshal(init)
	if err != nil {
		return nil, err
	}
	node := Node{
		State: state,
		Salt:  salt[:],
	}
	id, err := postNode(ctx, s, node)
	if err != nil {
		return nil, err
	}
	return &State[S]{ID: *id}, nil
}

func FromState[T any](s State[T], p Protocol[T]) *Feed[T] {
	return &Feed[T]{state: s, protocol: p}
}

func (f *Feed[S]) State() State[S] {
	return f.state
}

func (f *Feed[S]) String() string {
	sb := &strings.Builder{}
	sb.WriteString("{ID: ")
	sb.WriteString(f.state.ID.String() + ", Peers: {")
	for peerID, peerState := range f.state.Peers {
		fmt.Fprintf(sb, "%v: {Heads: %v} ", peerID, peerState.Heads)
	}
	sb.WriteString("} }")
	return sb.String()
}

func (f *Feed[S]) GetID() NodeID {
	return f.state.ID
}

// SetHeads sets the heads for peer to heads
// SetHeads will fail if the structure transitively reachable by heads contains dangling references.
// Use AddNode to ensure that the nodes exist.
func (f *Feed[S]) SetHeads(ctx context.Context, s cadata.Store, peer PeerID, heads []NodeID) error {
	if _, exists := f.state.Peers[peer]; !exists {
		return ErrPeerNotInFeed{Peer: peer}
	}
	// ensure all the ids refer to valid nodes.
	nodes, err := getAllNodes(ctx, s, heads)
	if err != nil {
		return err
	}
	if err := checkSenders(nodes); err != nil {
		return err
	}
	ps := f.state.Peers[peer]
	ps.setHeads(heads)
	return nil
}

// GetHeads returns the source nodes for peer
func (f *Feed[S]) GetHeads(peer PeerID) []NodeID {
	return f.state.Peers[peer].Heads
}

func (f *Feed[S]) GetState(peer PeerID) State[S] {
	return f.state.Peers[peer].State
}

func (f *Feed[S]) HasPeer(accordingTo, target PeerID) bool {
	return f.state.Peers[accordingTo].State.HasPeer(target)
}

// AddNode checks that the node is valid, which entails checking it only references valid nodes,
// and then posts the node to s.
// AddNode assumes that s only contains valid nodes.
func (f *Feed[S]) AddNode(ctx context.Context, s cadata.Store, peer PeerID, node Node) error {
	if err := f.CheckNode(ctx, s, peer, node); err != nil {
		return err
	}
	_, err := postNode(ctx, s, node)
	return err
}

// AdoptHeads checks that each of target's heads are valid, including their history,
// Any of target's heads which are valid, and not reachable by existing heads will be added to actors view of the feed.
func (f *Feed[S]) AdoptHeads(ctx context.Context, s cadata.Store, actor, target PeerID) error {
	panic("")
}

// Trust causes actor to accept target's heads as true.
func (f *Feed[S]) Trust(ctx context.Context, s cadata.Store, actor, target PeerID) error {
	panic("")
}

func (f *Feed[S]) append(ctx context.Context, s cadata.Store, actor PeerID, node Node) error {
	node.Author = actor
	node.Previous = f.Peers[actor].Heads
	prevNodes, err := getAllNodes(ctx, s, node.Previous)
	if err != nil {
		return err
	}
	_, maxN := findMaxN(prevNodes)
	node.N = maxN + 1
	if err := f.CheckNode(ctx, s, actor, node); err != nil {
		return err
	}
	id, err := postNode(ctx, s, node)
	if err != nil {
		return err
	}
	return f.Peers[actor].append(*id, node)
}

func (f *Feed[S]) AllHeads() []NodeID {
	return headsFromPeerStates(f.Peers)
}

func (f *Feed[S]) Members() []PeerID {
	return maps.Keys(f.Peers)
}

// func (f *Feed[S]) Verify(ctx context.Context, s cadata.Getter, trust []PeerID, target cadata.ID) error {
// 	targetNode, err := getNode(ctx, s, target)
// 	if err != nil {
// 		return err
// 	}
// 	trust = append
// 	for _, peer := range trust {
// 		yes, err := f.HasPeerVerified(ctx, s, peer, target)
// 		if err != nil {
// 			return err
// 		}
// 	}
// }

// HasPeerVerified returns true if peer has included target in their view of the feed.
func (f *Feed[S]) HasPeerVerified(ctx context.Context, s cadata.Getter, peer PeerID, target cadata.ID) (bool, error) {
	ps := f.Peers[peer]
	targetNode, err := GetNode(ctx, s, target)
	if err != nil {
		return false, err
	}
	return reachableFrom(ctx, s, ps.Heads, target, targetNode.N)
}

// CheckNode determines if the entry can be applied to the feed in its current state.
func (f *Feed[S]) CheckNode(ctx context.Context, s cadata.Getter, from PeerID, node Node) error {
	_, exists := f.Peers[from]
	if !exists {
		return ErrPeerNotInFeed{Peer: from}
	}
	if node.N > 0 && len(node.Previous) == 0 {
		return errors.New("nodes with N > 0 must reference another node.")
	}
	previous, err := getAllNodes(ctx, s, node.Previous)
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
		return ErrBadN{Have: node.N, Want: expectedN, Node: node}
	}
	if err := checkSenders(previous); err != nil {
		return err
	}
	// Try evolving the state and check that it is valid.
	if _, err := f.Peers[from].State.Evolve(node); err != nil {
		return err
	}
	return err
}

func (f *Feed[S]) isHead(id NodeID) bool {
	for _, ps := range f.Peers {
		if ps.Heads.Contains(id) {
			return true
		}
	}
	return false
}

func (f *Feed[S]) CanRead(from PeerID) bool {
	_, exists := f.state.Peers[from]
	return exists
}

type PeerState[T any] struct {
	Heads IDSet[NodeID] `json:"heads"`
	State State[T]      `json:"state"`
}

func (ps *PeerState[T]) append(id NodeID, x Node) error {
	next, err := ps.State.Evolve(x)
	if err != nil {
		return err
	}
	ps.setHeads([]NodeID{id})
	ps.State = *next
	return nil
}

func (ps *PeerState[T]) setHeads(heads []NodeID) {
	ps.Heads = append(ps.Heads[:0], heads...)
}

func headsFromPeerStates(x map[PeerID]*PeerState[T]) IDSet[NodeID] {
	ret := NewIDSet[NodeID]()
	for _, ps := range x {
		ret = Union(ret, ps.Heads)
	}
	return ret
}

func (f *Feed[S]) SyncHeads(ctx context.Context, dst cadata.Store, src cadata.Getter, heads []NodeID) error {
	for _, id := range heads {
		exists, err := cadata.Exists(ctx, dst, id)
		if err != nil {
			return err
		}
		if !exists {
			node, err := GetNode(ctx, src, id)
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

// checkSenders ensures that each Node in previous has a unique sender.
func checkSenders(previous []Node) error {
	senders := map[PeerID]struct{}{}
	for _, node := range previous {
		if _, exists := senders[node.Author]; exists {
			return errors.New("a valid set of heads can only contain one head from each sender")
		}
		senders[node.Author] = struct{}{}
	}
	return nil
}
