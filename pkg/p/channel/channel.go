package channel

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/memberset"
	"github.com/owlmessenger/owl/pkg/slices2"
)

type State struct {
	Members memberset.State `json:"members"`
	// TODO: use copy on write data structure with efficient appends
	Messages []Message `json:"messages"`
}

type Feed feeds.Feed[State]

type Operator struct {
	members memberset.Operator
}

func New() *Operator {
	kvop := gotkv.NewOperator(1<<12, 1<<16)
	return &Operator{
		members: memberset.New(&kvop),
	}
}

func (o *Operator) NewEmpty(ctx context.Context, s cadata.Store, peers []feeds.PeerID) (*State, error) {
	memberRoot, err := o.members.NewEmpty(ctx, s, peers)
	if err != nil {
		return nil, err
	}
	return &State{Members: *memberRoot, Messages: nil}, nil
}

func (o *Operator) membApply(ctx context.Context, s cadata.Store, x State, fn func(memberset.State) (*memberset.State, error)) (*State, error) {
	y, err := fn(x.Members)
	if err != nil {
		return nil, err
	}
	return &State{Members: *y, Messages: x.Messages}, nil
}

func (o *Operator) msgApply(ctx context.Context, s cadata.Store, x State, fn func([]Message) ([]Message, error)) (*State, error) {
	y, err := fn(x.Messages)
	if err != nil {
		return nil, err
	}
	return &State{Members: x.Members, Messages: y}, nil
}

func (o *Operator) AddPeer(ctx context.Context, s cadata.Store, x State, peers []memberset.Peer, nonce feeds.NodeID) (*State, error) {
	return o.membApply(ctx, s, x, func(x memberset.State) (*memberset.State, error) {
		return o.members.AddPeers(ctx, s, x, peers, nonce)
	})
}

func (o *Operator) RemovePeer(ctx context.Context, s cadata.Store, x State, peers []memberset.PeerID) (*State, error) {
	return o.membApply(ctx, s, x, func(x memberset.State) (*memberset.State, error) {
		return o.members.RemovePeers(ctx, s, x, peers)
	})
}

func (o *Operator) HasMember(ctx context.Context, s cadata.Store, x State, peer feeds.PeerID) (bool, error) {
	return o.members.Exists(ctx, s, x.Members, peer)
}

// Append adds a message from author to the conversation.
func (o *Operator) Append(ctx context.Context, s cadata.Store, x State, author feeds.PeerID, msg Message) (*State, error) {
	return &State{
		Members:  x.Members,
		Messages: append(x.Messages, msg),
	}, nil
}

func (o *Operator) List(ctx context.Context, s cadata.Store, span state.Span[[]uint64]) ([][]uint64, error) {
	return nil, nil
}

// Validate determines if the transision is valid
func (o *Operator) Validate(ctx context.Context, s cadata.Store, author feeds.PeerID, prev, next State) error {
	return nil
}

func (o *Operator) Merge(ctx context.Context, s cadata.Store, xs []State) (*State, error) {
	mem, err := o.members.Merge(ctx, s, slices2.Map(xs, func(x State) gotkv.Root { return x.Members }))
	if err != nil {
		return nil, err
	}
	msgs, err := o.mergeMsg(ctx, s, slices2.Map(xs, func(x State) []Message { return x.Messages }))
	if err != nil {
		return nil, err
	}
	return &State{Members: *mem, Messages: msgs}, nil
}

func (o *Operator) mergeMsg(ctx context.Context, s cadata.Store, xs [][]Message) ([]Message, error) {
	switch len(xs) {
	case 0:
		return nil, nil
	case 1:
		return xs[0], nil
	case 2:
		// Diff the two roots, find the last common message.
		panic("")
	default:
		l := len(xs)
		left, err := o.mergeMsg(ctx, s, xs[:l/2])
		if err != nil {
			return nil, err
		}
		right, err := o.mergeMsg(ctx, s, xs[l/2:])
		if err != nil {
			return nil, err
		}
		return o.mergeMsg(ctx, s, [][]Message{left, right})
	}
}

func parseMsgID(x []byte) (ret []uint64, _ error) {
	if len(x)%8 != 0 {
		return nil, fmt.Errorf("invalid size for message")
	}
	for i := 0; i < len(x)/8; i++ {
		n := binary.BigEndian.Uint64(x[i*8:])
		ret = append(ret, n)
	}
	return ret, nil
}

func uint64Bytes(x uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], x)
	return buf[:]
}
