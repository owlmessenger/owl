package directmsg

import (
	"context"
	"encoding/json"

	"github.com/owlmessenger/owl/pkg/cflog"
	"github.com/owlmessenger/owl/pkg/owldag"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/tai64"
)

type (
	State = cflog.Root
	Path  = cflog.Path
)

type Operator struct {
	cflog cflog.Operator
}

func New() Operator {
	return Operator{cflog: cflog.New()}
}

func (o *Operator) NewEmpty(ctx context.Context, s cadata.Store) (*State, error) {
	return o.cflog.NewEmpty(ctx, s)
}

type MessageParams struct {
	Author    owldag.PeerID
	Timestamp tai64.TAI64N

	Type string
	Body json.RawMessage
}

func (o *Operator) Append(ctx context.Context, s cadata.Store, x State, mp MessageParams) (*State, error) {
	return o.cflog.Append(ctx, s, x, nil, cflog.EntryParams{
		Author: mp.Author,
		Data: jsonMarshal(entryPayload{
			Type: mp.Type,
			Body: mp.Body,
		}),
		Timestamp: mp.Timestamp,
	})
}

func (o *Operator) Read(ctx context.Context, s cadata.Store, x State, begin Path, buf []Message) (int, error) {
	buf2 := make([]cflog.Entry, len(buf))
	n, err := o.cflog.Read(ctx, s, x, begin, buf2)
	if err != nil {
		return 0, err
	}
	for i := range buf2[:n] {
		e := buf2[i]
		var payload entryPayload
		if err := json.Unmarshal(e.Data, &payload); err != nil {
			return 0, err
		}
		buf[i] = Message{
			Path: e.Path,
			ID:   e.ID,

			Author:    e.Author,
			Timestamp: e.Timestamp,

			Type: payload.Type,
			Body: payload.Body,
		}
	}
	return n, nil
}

func (o *Operator) Merge(ctx context.Context, s cadata.Store, xs []State) (*State, error) {
	return o.cflog.Merge(ctx, s, xs)
}

func (o *Operator) Validate(ctx context.Context, s cadata.Getter, consult owldag.ConsultFunc, x State) error {
	return o.cflog.Validate(ctx, s, consult, x)
}

func (o *Operator) ValidateStep(ctx context.Context, s cadata.Getter, consult owldag.ConsultFunc, prev, next State) error {
	return o.cflog.ValidateStep(ctx, s, consult, prev, next)
}

func (o *Operator) Sync(ctx context.Context, src cadata.Getter, dst cadata.Store, x State) error {
	return o.cflog.Sync(ctx, src, dst, x)
}

type Message struct {
	Path Path
	ID   owldag.Ref

	Author    owldag.PeerID
	Timestamp tai64.TAI64N

	Type string
	Body json.RawMessage
}

type entryPayload struct {
	Type string          `json:"type"`
	Body json.RawMessage `json:"body"`
}

func jsonMarshal(x interface{}) []byte {
	data, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	return data
}
