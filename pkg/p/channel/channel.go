package channel

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-tai64"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/memberset"
	"github.com/owlmessenger/owl/pkg/slices2"
	"golang.org/x/exp/slices"
)

type State struct {
	Members memberset.State `json:"members"`
	// TODO: use copy on write data structure with efficient appends
	Events []Pair `json:"events"`
}

type Feed feeds.Feed[State]

type Operator struct {
	members memberset.Operator
}

func New() Operator {
	kvop := gotkv.NewOperator(1<<12, 1<<16)
	return Operator{
		members: memberset.New(&kvop),
	}
}

func (o *Operator) NewEmpty(ctx context.Context, s cadata.Store, peers []feeds.PeerID) (*State, error) {
	memberRoot, err := o.members.NewEmpty(ctx, s, peers)
	if err != nil {
		return nil, err
	}
	events := []Pair{
		{
			ID: EventID{0},
			Event: Event{
				Timestamp: tai64.Now(),
				Origin:    &struct{}{},
			},
		},
	}
	return &State{Members: *memberRoot, Events: events}, nil
}

func (o *Operator) membApply(ctx context.Context, s cadata.Store, x State, fn func(memberset.State) (*memberset.State, error)) (*State, error) {
	y, err := fn(x.Members)
	if err != nil {
		return nil, err
	}
	return &State{Members: *y, Events: x.Events}, nil
}

func (o *Operator) AddPeer(ctx context.Context, s cadata.Store, x State, peers []memberset.Peer, nonce feeds.ID) (*State, error) {
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
func (o *Operator) Append(ctx context.Context, s cadata.Store, x State, ev Event) (*State, error) {
	key := EventID{0}
	if len(x.Events) > 0 {
		key = x.Events[len(x.Events)-1].ID
	}
	return &State{
		Members: x.Members,
		Events: append(x.Events, Pair{
			ID:    key,
			Event: ev,
		}),
	}, nil
}

// Read reads messages into buf.
func (o *Operator) Read(ctx context.Context, s cadata.Store, x State, span state.Span[EventID], buf []Pair) (int, error) {
	var n int
	for i := range x.Events {
		if n >= len(buf) {
			break
		}
		if span.Contains(x.Events[i].ID, IDCompare) {
			buf[n] = x.Events[i]
			n++
		}
	}
	return n, nil
}

// List lists messgae
func (o *Operator) List(ctx context.Context, s cadata.Store, x State, span state.Span[EventID]) (ret []EventID, _ error) {
	var n int
	for i := range x.Events {
		if n >= len(ret) {
			break
		}
		if span.Contains(x.Events[i].ID, IDCompare) {
			ret[n] = x.Events[i].ID
			n++
		}
	}
	return ret, nil
}

// Get gets a single event by id.
func (o *Operator) Get(ctx context.Context, s cadata.Store, x State, id EventID) (*Event, error) {
	var buf [1]Pair
	span := state.PointSpan(id)
	n, err := o.Read(ctx, s, x, span, buf[:])
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, errors.New("message does not exist")
	}
	return &buf[0].Event, nil
}

// Validate determines if the transision is valid
func (o *Operator) Validate(ctx context.Context, s cadata.Store, author feeds.PeerID, prev, next State) error {
	if err := o.members.Validate(ctx, s, author, prev.Members, next.Members); err != nil {
		return err
	}
	return nil
}

func (o *Operator) Merge(ctx context.Context, s cadata.Store, xs []State) (*State, error) {
	mem, err := o.members.Merge(ctx, s, slices2.Map(xs, func(x State) gotkv.Root { return x.Members }))
	if err != nil {
		return nil, err
	}
	msgs, err := o.mergeMsg(ctx, s, slices2.Map(xs, func(x State) []Pair { return x.Events }))
	if err != nil {
		return nil, err
	}
	return &State{Members: *mem, Events: msgs}, nil
}

func (o *Operator) mergeMsg(ctx context.Context, s cadata.Store, xs [][]Pair) ([]Pair, error) {
	switch len(xs) {
	case 0:
		return nil, nil
	case 1:
		return xs[0], nil
	case 2:
		left, right := xs[0], xs[1]
		merged := make([]Pair, 0, len(left)+len(right))
		merged = append(merged, left...)
		merged = append(merged, right...)
		slices.SortFunc(merged, func(a, b Pair) bool {
			if a.Event.Timestamp != b.Event.Timestamp {
				return a.Event.Timestamp.Before(b.Event.Timestamp)
			}
			return false
		})
		for i := range merged {
			merged[i].ID = EventID{uint64(i)}
		}
		return merged, nil

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
		return o.mergeMsg(ctx, s, [][]Pair{left, right})
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
