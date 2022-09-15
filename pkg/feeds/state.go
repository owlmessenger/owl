package feeds

import (
	"context"
	"crypto/rand"
	"encoding/json"

	"github.com/brendoncarroll/go-state/cadata"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/maps"
)

// State is the state of the feed according to an individual peer
type State[T any] struct {
	ID    ID         `json:"id"`
	Heads IDSet[Ref] `json:"heads"`

	Max   uint64            `json:"max"`
	Min   uint64            `json:"min"`
	State T                 `json:"state"`
	PeerN map[PeerID]uint64 `json:"peer_n"`
}

func (s *State[T]) Marshal() []byte {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return data
}

func ParseState[T any](data []byte) (*State[T], error) {
	var ret State[T]
	if err := json.Unmarshal(data, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func InitialState[T any](ctx context.Context, s cadata.Store, x T, salt *[32]byte) (*State[T], error) {
	if salt == nil {
		salt = new([32]byte)
		if _, err := rand.Read(salt[:]); err != nil {
			return nil, err
		}
	}
	node := Node[T]{
		Salt:  salt[:],
		State: x,
	}
	id, err := PostNode(ctx, s, node)
	if err != nil {
		return nil, err
	}
	return &State[T]{
		ID:    *id,
		Heads: NewIDSet(*id),

		State: x,
	}, nil
}

func modify[T any](ctx context.Context, s cadata.Store, prev State[T], author PeerID, desired T) (*State[T], error) {
	prevNodes, err := getAllNodes[T](ctx, s, prev.Heads)
	if err != nil {
		return nil, err
	}
	var minN uint64
	for _, node := range prevNodes {
		minN = min(node.N)
	}
	node := Node[T]{
		N:        prev.Max + 1,
		Previous: prev.Heads,
		Author:   author,
	}
	peern := maps.Clone(prev.PeerN)
	peern[author] = node.N
	id, err := PostNode(ctx, s, node)
	if err != nil {
		return nil, err
	}
	return &State[T]{
		ID:    prev.ID,
		Heads: NewIDSet(*id),

		Max:   node.N,
		Min:   minN,
		State: desired,
		PeerN: peern,
	}, nil
}

// func Merge[T any](ctx context.Context, protocol Protocol[T], s cadata.Store, ids []Ref) (*State[T], error) {
// 	xs, err := getAllNodes[T](ctx, s, ids)
// 	if err != nil {
// 		return nil, err
// 	}
// 	var min, max uint64
// 	for _, x := range xs {
// 		if x.N < min {
// 			min = x.N
// 		}
// 		if x.N > max {
// 			max = x.N
// 		}
// 	}
// 	y, err := protocol.Merge(ctx, slices2.)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &State[T]{
// 		ID:    xs[0],
// 		Heads: xs,
// 		Min:   min,
// 		Max:   max,
// 		State: y,
// 	}, nil
// }

func min[T constraints.Ordered](xs ...T) (ret T) {
	if len(xs) > 0 {
		ret = xs[0]
	}
	for i := 1; i < len(xs); i++ {
		if xs[i-1] < xs[i] {
			ret = xs[i]
		}
	}
	return ret
}
