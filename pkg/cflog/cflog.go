package cflog

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-tai64"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/owlmessenger/owl/pkg/cflog/rope"
	"github.com/owlmessenger/owl/pkg/heap"
	"github.com/owlmessenger/owl/pkg/owldag"
)

const MaxEntryLen = 4096

type (
	Ref  = cadata.ID
	Root = rope.Root[Ref]
	Span = rope.Span
	Path = rope.Path

	PeerID = owldag.PeerID
)

const (
	prefixRevision = 0x00
	prefixChildren = 0xff
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
	Data      json.RawMessage
}

func (o *Operator) Append(ctx context.Context, s cadata.Store, x Root, thread Path, eps []EntryParams) (*Root, error) {
	if len(thread) > 0 {
		panic("threads not supported")
	}
	b := o.newBuilder(s)
	if err := rope.Copy(ctx, b, o.newIterator(s, x, rope.TotalSpan())); err != nil {
		return nil, err
	}
	for _, ep := range eps {
		data, err := json.Marshal(Elem{
			Author: ep.Author,
			Data:   ep.Data,
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

func (o *Operator) Revise(ctx context.Context, s cadata.Store, x Root, p Path, desired Elem) (*Root, error) {
	panic("revisions not yet supported")
	return nil, nil
}

// Entries make up the log.
type Entry struct {
	Path
	Elem
}

func (o *Operator) Read(ctx context.Context, s cadata.Store, x Root, begin Path, buf []Entry) (int, error) {
	span := rope.TotalSpan()
	span.WithLowerIncl(rope.Path(begin))
	it := o.newIterator(s, x, rope.TotalSpan())

	var n int
	var ent rope.Entry
	for n < len(buf) {
		if err := it.Next(ctx, &ent); err != nil {
			if errors.Is(err, gotkv.EOS) {
				break
			}
			return n, err
		}
		buf[n].Path = append(buf[n].Path[:0], ent.Path...)
		if err := json.Unmarshal(ent.Value, &buf[n].Elem); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func (o *Operator) Merge(ctx context.Context, s cadata.Store, xs []Root) (*Root, error) {
	// TODO: determine first key in diff and use that to avoid remerging the beginning.
	span := gotkv.TotalSpan()
	its := make([]*gotkv.Iterator, len(xs))
	for i := range xs {
		its[i] = o.gotkv.NewIterator(s, xs[i], span)
	}
	b := o.gotkv.NewBuilder(s)
	if err := o.interleave(ctx, b, nil, its); err != nil {
		return nil, err
	}
	return b.Finish(ctx)
}

func (o *Operator) interleave(ctx context.Context, b *gotkv.Builder, base Path, its []*gotkv.Iterator) error {
	if len(base) > 1 {
		panic("threads not yet supported")
	}
	set := make(map[cadata.ID]struct{})
	var h []*Elem
	for {
		var ent gotkv.Entry
		for i := range its {
			if err := its[i].Next(ctx, &ent); err != nil {
				if errors.Is(err, gotkv.EOS) {
					continue
				}
				return err
			}
			ev, err := parseElem(ent.Value)
			if err != nil {
				return err
			}
			id := ev.ID()
			if _, exists := set[id]; !exists {
				h = heap.Push(h, ev, ltElem)
				set[id] = struct{}{}
			}
		}
		if len(h) == 0 {
			break
		}
		var next *Elem
		next, h = heap.Pop(h, ltElem)
		delete(set, next.ID())
		p := base.Successor()
		if err := putElem(ctx, b, Path{}, 0, *next); err != nil {
			return err
		}
		base = p
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
	ents, err := rope.ListEntries()
	if err != nil {
		return err
	}
	for _, ent := range ents {
		if x.Depth > 0 {

		}
	}
	cadata.Copy()
	return o.gotkv.Sync(ctx, src, dst, x, func(gotkv.Entry) error { return nil })
}

func putElem(ctx context.Context, b *gotkv.Builder, p Path, rev uint32, ev Elem) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if len(data) > MaxEntryLen {
		return ErrEntryLen{Data: data}
	}
	key := appendElemKey(nil, p, rev)
	return b.Put(ctx, key, data)
}

func appendElemKey(out []byte, p Path, rev uint32) []byte {
	for i, n := range p {
		if i > 0 {
			out = append(out, prefixChildren)
		}
		out = appendUint64(out, n)
	}
	if rev > 0 {
		out = append(out, prefixRevision)
		out = appendUint32(out, rev)
	}
	return out
}

func parseElemKey(x []byte) (Path, uint32, error) {
	var ret Path
	if len(x) < 8 {
		return nil, 0, fmt.Errorf("paths are >= 8 bytes")
	}
	ret = append(ret, binary.BigEndian.Uint64(x[:8]))
	x = x[8:]
	for len(x) > 0 {
		switch x[0] {
		case prefixChildren:
			x = x[1:]
			if len(x) < 8 {
				return nil, 0, fmt.Errorf("paths are >= 8 bytes")
			}
			ret = append(ret, binary.BigEndian.Uint64(x[:8]))
			x = x[8:]
		default:
			return nil, 0, fmt.Errorf("unknown sub-key %v", x[0])
		}
	}
	return ret, 0, nil
}

func ltElem(a, b *Elem) bool {
	return a.Lt(b)
}

func appendUint32(out []byte, x uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], x)
	return append(out, buf[:]...)
}

func appendUint64(out []byte, x uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], x)
	return append(out, buf[:]...)
}
