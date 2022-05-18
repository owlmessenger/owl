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

type Feed struct {
	ID    NodeID                `json:"id"`
	Peers map[PeerID]*peerState `json:"peers"`
}

func NewEmpty(ctx context.Context, s cadata.Store, peers []PeerID) (*Feed, error) {
	salt := [32]byte{}
	if _, err := rand.Read(salt[:]); err != nil {
		return nil, err
	}
	init := Init{
		Salt:  salt,
		Peers: NewIDSet(peers...),
	}
	node := Node{
		Init: &init,
	}
	id, err := PostNode(ctx, s, node)
	if err != nil {
		return nil, err
	}
	peerStates := map[PeerID]*peerState{}
	for _, peer := range init.Peers {
		peerStates[peer] = &peerState{
			Heads: []NodeID{*id},
			State: *NewState(node),
		}
	}
	return &Feed{
		ID:    *id,
		Peers: peerStates,
	}, nil
}

func ParseFeed(data []byte) (*Feed, error) {
	var feed Feed
	if err := json.Unmarshal(data, &feed); err != nil {
		return nil, err
	}
	return &feed, nil
}

func (f *Feed) String() string {
	sb := &strings.Builder{}
	sb.WriteString("{ID: ")
	sb.WriteString(f.ID.String() + ", Peers: {")
	for peerID, peerState := range f.Peers {
		fmt.Fprintf(sb, "%v: {Heads: %v} ", peerID, peerState.Heads)
	}
	sb.WriteString("} }")
	return sb.String()
}

func (f *Feed) Marshal() []byte {
	data, err := json.Marshal(f)
	if err != nil {
		panic(err)
	}
	return data
}

func (f *Feed) GetID() NodeID {
	return f.ID
}

// SetHeads sets the heads for peer to heads
// SetHeads will fail if the structure transitively reachable by heads contains dangling references.
func (f *Feed) SetHeads(ctx context.Context, s cadata.Store, peer PeerID, heads []NodeID) error {
	if _, exists := f.Peers[peer]; !exists {
		return ErrPeerNotInFeed{Peer: peer}
	}
	// ensure all the ids refer to valid nodes.
	nodes, err := getAllNodes(ctx, s, heads)
	if err != nil {
		return err
	}
	senders := map[PeerID]struct{}{}
	for _, node := range nodes {
		if _, exists := senders[node.Author]; exists {
			return errors.New("a valid set of heads can only contain one head from each sender")
		}
		senders[node.Author] = struct{}{}
	}
	f.Peers[peer].setHeads(heads)
	return nil
}

// GetHeads returns the source nodes for peer
func (f *Feed) GetHeads(peer PeerID) []NodeID {
	return f.Peers[peer].Heads
}

func (f *Feed) HasPeer(accordingTo, target PeerID) bool {
	return f.Peers[accordingTo].State.HasPeer(target)
}

// AddNode checks that the node is valid, which entails checking it only references valid nodes,
// and then posts the node to s.
// AddNode assumes that s only contains valid nodes.
func (f *Feed) AddNode(ctx context.Context, s cadata.Store, peer PeerID, node Node) error {
	if err := f.CheckNode(ctx, s, peer, node); err != nil {
		return err
	}
	_, err := PostNode(ctx, s, node)
	return err
}

// AddPeer adds peers to the feed as actor
func (f *Feed) AddPeer(ctx context.Context, s cadata.Store, actor PeerID, peer PeerID, perms uint8) error {
	return f.append(ctx, s, actor, Node{
		AddPeer: &AddPeer{
			Peer: peer,
		},
	})
}

// RemovePeer removes peers from the feed as actor
func (f *Feed) RemovePeer(ctx context.Context, s cadata.Store, actor PeerID, peer PeerID) error {
	// TODO: Check if peer exists in actor's version of the feed
	return f.append(ctx, s, actor, Node{
		RemovePeer: &RemovePeer{
			Peer: peer,
		},
	})
}

// AppendData adds data to the feed as actor
func (f *Feed) AppendData(ctx context.Context, s cadata.Store, actor PeerID, data []byte) error {
	return f.append(ctx, s, actor, Node{
		Data: data,
	})
}

// AdoptHeads checks that each of target's heads are valid, including their history,
// Any of target's heads which are valid, and not reachable by existing heads will be added to actors view of the feed.
func (f *Feed) AdoptHeads(ctx context.Context, s cadata.Store, actor, target PeerID) error {
	panic("")
}

func (f *Feed) append(ctx context.Context, s cadata.Store, actor PeerID, node Node) error {
	node.Author = actor
	node.Previous = f.Peers[actor].Heads
	prevNodes, err := getAllNodes(ctx, s, node.Previous)
	if err != nil {
		return err
	}
	_, maxN := findMaxN(prevNodes)
	node.N = maxN + 1
	id, err := PostNode(ctx, s, node)
	if err != nil {
		return err
	}
	return f.Peers[actor].append(*id, node)
}

func (f *Feed) AllHeads() []NodeID {
	return headsFromPeerStates(f.Peers)
}

func (f *Feed) Members() []PeerID {
	return maps.Keys(f.Peers)
}

// func (f *Feed) Verify(ctx context.Context, s cadata.Getter, trust []PeerID, target cadata.ID) error {
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
func (f *Feed) HasPeerVerified(ctx context.Context, s cadata.Getter, peer PeerID, target cadata.ID) (bool, error) {
	ps := f.Peers[peer]
	targetNode, err := GetNode(ctx, s, target)
	if err != nil {
		return false, err
	}
	return reachableFrom(ctx, s, ps.Heads, target, targetNode.N)
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

// CheckNode determines if the entry can be applied to the feed in its current state.
func (f *Feed) CheckNode(ctx context.Context, s cadata.Getter, from PeerID, node Node) error {
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
	// Try evolving the state and check that it is valid.
	if _, err := f.Peers[from].State.Evolve(node); err != nil {
		return err
	}
	return err
}

func (f *Feed) isHead(id NodeID) bool {
	for _, ps := range f.Peers {
		if ps.Heads.Contains(id) {
			return true
		}
	}
	return false
}

func (f *Feed) CanRead(from PeerID) bool {
	_, exists := f.Peers[from]
	return exists
}

type peerState struct {
	Heads IDSet[NodeID] `json:"heads"`
	State State         `json:"state"`
}

func (ps *peerState) append(id NodeID, x Node) error {
	next, err := ps.State.Evolve(x)
	if err != nil {
		return err
	}
	ps.setHeads([]NodeID{id})
	ps.State = *next
	return nil
}

func (ps *peerState) setHeads(heads []NodeID) {
	ps.Heads = append(ps.Heads[:0], heads...)
}

func headsFromPeerStates(x map[PeerID]*peerState) IDSet[NodeID] {
	ret := NewIDSet[NodeID]()
	for _, ps := range x {
		ret = Union(ret, ps.Heads)
	}
	return ret
}

func (f *Feed) SyncHeads(ctx context.Context, dst cadata.Store, src cadata.Getter, heads []NodeID) error {
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
