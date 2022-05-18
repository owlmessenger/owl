package feeds

import (
	"context"
	"errors"

	"github.com/brendoncarroll/go-state/cadata"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/maps"
)

// State is the current state of the feed
type State struct {
	// N is the maximum N encountered.
	N uint64 `json:"n"`

	AddedPeers   map[PeerID]IDSet[NodeID] `json:"added_peers"`
	RemovedPeers map[PeerID]IDSet[NodeID] `json:"removed_peers"`
}

func NewState(node Node) *State {
	if node.Init == nil {
		panic("NewState called on non-init node")
	}
	addedAt := IDSet[NodeID]{NewNodeID(node)}
	addedPeers := map[PeerID]IDSet[NodeID]{}
	for _, peer := range node.Init.Peers {
		addedPeers[peer] = addedAt
	}
	return &State{
		N:            0,
		AddedPeers:   addedPeers,
		RemovedPeers: map[PeerID]IDSet[NodeID]{},
	}
}

func BuildState(ctx context.Context, s cadata.Getter, ids []NodeID) (*State, error) {
	if len(ids) == 0 {
		return nil, errors.New("BuildState called with 0 IDs")
	}
	nodes, err := getAllNodes(ctx, s, ids)
	if err != nil {
		return nil, err
	}
	states := make([]*State, len(nodes))
	for i, node := range nodes {
		prev, err := BuildState(ctx, s, node.Previous)
		if err != nil {
			return nil, err
		}
		state, err := prev.Evolve(node)
		if err != nil {
			return nil, err
		}
		states[i] = state
	}
	return MergeStates(states...), nil
}

func (s *State) Evolve(x Node) (*State, error) {
	if x.N <= s.N {
		return nil, ErrReplayedN{Last: s.N, N: x.N, Peer: x.Author}
	}
	if x.N > s.N+1 {
		return nil, ErrBadN{Have: x.N, Want: s.N + 1}
	}
	switch {
	case x.Init != nil:
		return nil, errors.New("second init node")
	case x.RemovePeer != nil:
		target := x.RemovePeer.Peer
		if !s.HasPeer(target) {
			return nil, errors.New("remove peer on non-existant peer")
		}
		removed := maps.Clone(s.RemovedPeers)
		removed[target] = Union(removed[target], s.AddedPeers[target])
		added := maps.Clone(s.AddedPeers)
		added[target] = IDSet[NodeID]{}
		return &State{
			N:            x.N,
			AddedPeers:   added,
			RemovedPeers: removed,
		}, nil
	case x.AddPeer != nil:
		target := x.AddPeer.Peer
		if s.HasPeer(target) {
			return nil, errors.New("add peer with existing peer")
		}
		added := maps.Clone(s.AddedPeers)
		added[target] = added[target].Add(NewNodeID(x))
		return &State{
			N:            x.N,
			AddedPeers:   added,
			RemovedPeers: s.RemovedPeers,
		}, nil
	case x.Data != nil:
		return &State{
			N:            x.N,
			AddedPeers:   s.AddedPeers,
			RemovedPeers: s.RemovedPeers,
		}, nil
	default:
		return nil, errors.New("empty node")
	}
}

func (s *State) HasPeer(x PeerID) bool {
	addedAt := s.AddedPeers[x]
	removedAt := s.RemovedPeers[x]
	for _, nodeID := range addedAt {
		if !removedAt.Contains(nodeID) {
			return true
		}
	}
	return false
}

func (s *State) NumPeers() int {
	var count int
	for peer := range s.AddedPeers {
		if s.HasPeer(peer) {
			count++
		}
	}
	return count
}

func (s *State) ListPeers() (ret []PeerID) {
	for peer := range s.AddedPeers {
		if s.HasPeer(peer) {
			ret = append(ret, peer)
		}
	}
	return ret
}

func MergeStates(xs ...*State) *State {
	switch len(xs) {
	case 0:
		panic("cannot merge 0 states")
	case 1:
		return xs[0]
	case 2:
		return &State{
			N:            Max(xs[0].N, xs[1].N),
			AddedPeers:   mergeMaps(xs[0].AddedPeers, xs[1].AddedPeers, Union[NodeID]),
			RemovedPeers: mergeMaps(xs[0].RemovedPeers, xs[1].RemovedPeers, Union[NodeID]),
		}
	default:
		l := len(xs)
		left := MergeStates(xs[:l/2]...)
		right := MergeStates(xs[l/2:]...)
		return MergeStates(left, right)
	}
}

func Max[T constraints.Ordered](xs ...T) (max T) {
	for _, x := range xs {
		if x >= max {
			max = x
		}
	}
	return max
}

func mergeMaps[K comparable, V any, M ~map[K]V](a, b M, fn func(a, b V) V) M {
	out := make(M)
	for k, v := range a {
		if v2, exists := b[k]; exists {
			v = fn(v, v2)
		}
		out[k] = v
	}
	for k, v := range b {
		if _, exists := a[k]; exists {
			continue
		}
		out[k] = v
	}
	return out
}
