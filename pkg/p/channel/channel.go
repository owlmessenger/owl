package channel

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/owlmessenger/owl/pkg/cflog"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/memberset"
	"github.com/owlmessenger/owl/pkg/slices2"
	"golang.org/x/sync/errgroup"
)

type State struct {
	Members memberset.State `json:"members"`
	Events  gotkv.Root      `json:"events"`
}

type (
	PeerID = feeds.PeerID
	Path   = cflog.Path
	Event  = cflog.Event
	Pair   = cflog.Pair
)

type Operator struct {
	gotkv   *gotkv.Operator
	members memberset.Operator
	cflog   cflog.Operator
}

func New() Operator {
	kvop := gotkv.NewOperator(1<<12, 1<<16)
	return Operator{
		gotkv:   &kvop,
		members: memberset.New(&kvop),
		cflog:   cflog.New(&kvop),
	}
}

func (o *Operator) NewEmpty(ctx context.Context, s cadata.Store, peers []feeds.PeerID) (*State, error) {
	memberRoot, err := o.members.NewEmpty(ctx, s, peers)
	if err != nil {
		return nil, err
	}
	logRoot, err := o.cflog.NewEmpty(ctx, s)
	if err != nil {
		return nil, err
	}
	logRoot, err = o.cflog.Append(ctx, s, *logRoot, nil, []cflog.Event{
		{Data: []byte(`{"origin": {}}`)},
	})
	if err != nil {
		return nil, err
	}
	return &State{Members: *memberRoot, Events: *logRoot}, nil
}

func (o *Operator) membApply(ctx context.Context, s cadata.Store, x State, fn func(memberset.State) (*memberset.State, error)) (*State, error) {
	y, err := fn(x.Members)
	if err != nil {
		return nil, err
	}
	return &State{Members: *y, Events: x.Events}, nil
}

func (o *Operator) logApply(ctx context.Context, s cadata.Store, x State, fn func(cflog.Root) (*cflog.Root, error)) (*State, error) {
	y, err := fn(x.Events)
	if err != nil {
		return nil, err
	}
	return &State{Members: x.Members, Events: *y}, nil
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

// Append adds a message to the end of the conversation.
func (o *Operator) Append(ctx context.Context, s cadata.Store, x State, ev cflog.Event) (*State, error) {
	return o.logApply(ctx, s, x, func(x cflog.Root) (*cflog.Root, error) {
		return o.cflog.Append(ctx, s, x, nil, []cflog.Event{ev})
	})
}

// Read reads messages into buf.
func (o *Operator) Read(ctx context.Context, s cadata.Store, x State, begin cflog.Path, buf []cflog.Pair) (int, error) {
	return o.cflog.ReadThread(ctx, s, x.Events, begin, buf)
}

// // List lists messgae
// func (o *Operator) List(ctx context.Context, s cadata.Store, x State, span state.Span[EventID]) (ret []EventID, _ error) {
// 	var n int
// 	for i := range x.Events {
// 		if n >= len(ret) {
// 			break
// 		}
// 		if span.Contains(x.Events[i].ID, IDCompare) {
// 			ret[n] = x.Events[i].ID
// 			n++
// 		}
// 	}
// 	return ret, nil
// }

// // Get gets a single event by id.
// func (o *Operator) Get(ctx context.Context, s cadata.Store, x State, p Path) (*Event, error) {
// 	var buf [1]Pair
// 	span := state.PointSpan(id)
// 	n, err := o.Read(ctx, s, x, span, buf[:])
// 	if err != nil {
// 		return nil, err
// 	}
// 	if n == 0 {
// 		return nil, errors.New("message does not exist")
// 	}
// 	return &buf[0].Event, nil
// }

// Validate determines if the transision is valid
func (o *Operator) Validate(ctx context.Context, s cadata.Store, author feeds.PeerID, prev, next State) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return o.members.Validate(ctx, s, author, prev.Members, next.Members)
	})
	eg.Go(func() error {
		return o.cflog.Validate(ctx, s, author, prev.Events, next.Events)
	})
	return eg.Wait()
}

func (o *Operator) Merge(ctx context.Context, s cadata.Store, xs []State) (*State, error) {
	mem, err := o.members.Merge(ctx, s, slices2.Map(xs, func(x State) memberset.State { return x.Members }))
	if err != nil {
		return nil, err
	}
	lroot, err := o.cflog.Merge(ctx, s, slices2.Map(xs, func(x State) cflog.Root { return x.Events }))
	if err != nil {
		return nil, err
	}
	return &State{Members: *mem, Events: lroot}, nil
}
