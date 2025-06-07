package cflog

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/owlmessenger/owl/src/cflog/rope"
	"github.com/owlmessenger/owl/src/owldag"
	"go.brendoncarroll.net/exp/heaps"
	"go.brendoncarroll.net/exp/streams"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/kv"
	"go.brendoncarroll.net/tai64"
)

const MaxEntryLen = 4096

type (
	Ref  = cadata.ID
	Root = rope.Root[Ref]
	Span = rope.Span
	Path = rope.Path

	PeerID = owldag.PeerID
)

type Operator struct {
	meanSize, maxSize int
	ropeSeed          *[16]byte
}

func New() Operator {
	return Operator{
		meanSize: 1 << 12,
		maxSize:  1 << 16,
		ropeSeed: new([16]byte),
	}
}

func (o *Operator) newBuilder(s cadata.Store) *rope.Builder[Ref] {
	return rope.NewBuilder(rope.NewWriteStorage(s), o.meanSize, o.maxSize, o.ropeSeed)
}

func (o *Operator) newIterator(s cadata.Getter, root rope.Root[Ref], span rope.Span) *rope.Iterator[Ref] {
	return rope.NewIterator(rope.NewStorage(s), root, span)
}

func (o *Operator) NewEmpty(ctx context.Context, s cadata.Store) (*Root, error) {
	return o.newBuilder(s).Finish(ctx)
}

// EntryParams create an entry in the log.
type EntryParams struct {
	Author    owldag.PeerID
	Timestamp tai64.TAI64N
	Parent    *Ref
	Data      json.RawMessage
}

func (o *Operator) Append(ctx context.Context, s cadata.Store, x Root, thread Path, ep EntryParams) (*Root, error) {
	return o.AppendBatch(ctx, s, x, thread, []EntryParams{ep})
}

func (o *Operator) AppendBatch(ctx context.Context, s cadata.Store, x Root, thread Path, eps []EntryParams) (*Root, error) {
	if len(thread) > 0 {
		panic("threads not supported")
	}
	// TODO: check that timestamp is increasing
	b := o.newBuilder(s)
	if err := rope.Copy(ctx, b, o.newIterator(s, x, rope.TotalSpan())); err != nil {
		return nil, err
	}
	for _, ep := range eps {
		data, err := json.Marshal(wireEntry{
			Author:    ep.Author,
			Data:      ep.Data,
			Timestamp: ep.Timestamp,
			Parent:    ep.Parent,
		})
		if err != nil {
			return nil, err
		}
		if err := b.Append(ctx, uint8(len(thread)), data); err != nil {
			return nil, err
		}
	}
	return b.Finish(ctx)
}

// Entries make up the log.
type Entry struct {
	Path      Path
	ID        Ref
	Author    owldag.PeerID
	Timestamp tai64.TAI64N
	Data      json.RawMessage
	IsThread  bool
}

func (o *Operator) Read(ctx context.Context, s cadata.Store, x Root, begin Path, buf []Entry) (int, error) {
	span := rope.TotalSpan()
	span.WithLowerIncl(rope.Path(begin))
	it := o.newIterator(s, x, rope.TotalSpan())

	var n int
	for n < len(buf) {
		if err := readEntry(ctx, it, &buf[n]); err != nil {
			if errors.Is(err, streams.EOS()) {
				break
			}
			return 0, err
		}
		n++
	}
	return n, nil
}

func (o *Operator) Merge(ctx context.Context, s cadata.Store, xs []Root) (*Root, error) {
	// TODO: determine first key in diff and use that to avoid remerging the beginning.
	span := rope.TotalSpan()
	its := make([]*rope.Iterator[Ref], len(xs))
	for i := range xs {
		its[i] = o.newIterator(s, xs[i], span)
	}
	b := o.newBuilder(s)
	if err := o.interleave(ctx, b, its); err != nil {
		return nil, err
	}
	return b.Finish(ctx)
}

func (o *Operator) interleave(ctx context.Context, b *rope.Builder[Ref], its []*rope.Iterator[Ref]) error {
	set := make(map[cadata.ID]struct{})
	var h []*wireEntry
	for {
		var ent rope.Entry
		for i := range its {
			if err := its[i].Next(ctx, &ent); err != nil {
				if streams.IsEOS(err) {
					continue
				}
				return err
			}
			we, err := parseWireEntry(ent.Value)
			if err != nil {
				return err
			}
			id := we.ID()
			if _, exists := set[id]; !exists {
				h = heaps.PushFunc(h, we, ltWireEntry)
				set[id] = struct{}{}
			}
		}
		if len(h) == 0 {
			break
		}
		var next *wireEntry
		next, h = heaps.PopFunc(h, ltWireEntry)
		delete(set, next.ID())
		if err := writeEntry(ctx, b, 0, *next); err != nil {
			return err
		}
	}
	return nil
}

func (o *Operator) Validate(ctx context.Context, s cadata.Getter, consult owldag.ConsultFunc, x Root) error {
	return nil
}

func (o *Operator) ValidateStep(ctx context.Context, s cadata.Getter, consult owldag.ConsultFunc, prev, next Root) error {
	return nil
}

func (o *Operator) Sync(ctx context.Context, src cadata.Getter, dst cadata.Store, x Root) error {
	return rope.Walk(ctx, rope.NewStorage(src), x, rope.Walker[Ref]{
		Before: func(r Ref) bool {
			exists, err := kv.ExistsUsingList(ctx, dst, r)
			return err != nil || !exists
		},
		ForEach: func(ent rope.Entry) error {
			return nil
		},
		After: func(r Ref) error {
			return cadata.Copy(ctx, dst, src, r)
		},
	})
}

func readEntry(ctx context.Context, it *rope.Iterator[Ref], out *Entry) error {
	var ent rope.Entry
	if err := it.Next(ctx, &ent); err != nil {
		return err
	}
	we, err := parseWireEntry(ent.Value)
	if err != nil {
		return err
	}
	out.Path = append(out.Path[:0], ent.Path...)
	out.Author = we.Author
	out.Timestamp = we.Timestamp
	out.Data = we.Data
	out.ID = we.ID()
	return nil
}

func writeEntry(ctx context.Context, b *rope.Builder[Ref], level uint8, x wireEntry) error {
	data, err := json.Marshal(x)
	if err != nil {
		return err
	}
	if len(data) > MaxEntryLen {
		return ErrEntryLen{Data: data}
	}
	return b.Append(ctx, level, data)
}
