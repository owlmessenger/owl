package owldag

import (
	"context"
	"crypto/rand"
	"encoding/json"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/owlmessenger/owl/pkg/slices2"
)

type State[T any] struct {
	Max  uint64 `json:"max"`
	Prev []Head `json:"prev"`

	// Heads is the root of the head for each peer.
	Heads  gotkv.Root `json:"heads"`
	Epochs []Ref      `json:"epochs"`

	X T `json:"x"`
}

func ParseState[T any](data []byte) (*State[T], error) {
	var state State[T]
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (s State[T]) PrevRefs() IDSet[Ref] {
	return NewIDSet(slices2.Map(s.Prev, func(x Head) Ref { return x.Ref })...)
}

func (s State[T]) Marshal() []byte {
	data, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return data
}

func InitState[T any](ctx context.Context, s cadata.Store, x T, salt *[32]byte) (*State[T], error) {
	if salt == nil {
		salt = new([32]byte)
		if _, err := rand.Read(salt[:]); err != nil {
			return nil, err
		}
	}
	kvop := gotkv.NewOperator(1<<12, MaxNodeSize)
	sigsRoot, err := kvop.NewEmpty(ctx, s)
	if err != nil {
		return nil, err
	}
	ref, err := PostNode(ctx, s, Node[T]{
		Salt:  salt[:],
		State: x,
		Sigs:  *sigsRoot,
	})
	if err != nil {
		return nil, err
	}
	return &State[T]{
		Max:    0,
		Prev:   []Head{{Ref: *ref}},
		Epochs: []Ref{*ref},

		X:     x,
		Heads: *sigsRoot,
	}, nil
}
