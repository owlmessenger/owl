package cflog

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-tai64"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/owlmessenger/owl/pkg/heap"
	"github.com/owlmessenger/owl/pkg/owldag"
)

const MaxEntryLen = 4096

type (
	Root   = gotkv.Root
	Span   = state.Span[Path]
	PeerID = owldag.PeerID
)

const (
	prefixRevision = 0x00
	prefixChildren = 0xff
)

type Operator struct {
	gotkv *gotkv.Operator
}

func New(kvop *gotkv.Operator) Operator {
	return Operator{gotkv: kvop}
}

func (o *Operator) NewEmpty(ctx context.Context, s cadata.Store) (*Root, error) {
	return o.gotkv.NewEmpty(ctx, s)
}

// EntryParams create an entry in the log.
type EntryParams struct {
	Author    owldag.PeerID
	Data      json.RawMessage
	Timestamp tai64.TAI64N
}

func (o *Operator) Append(ctx context.Context, s cadata.Store, x Root, thread Path, es []EntryParams) (*Root, error) {
	if len(thread) > 0 {
		panic("threads not supported")
	}
	ent, err := o.gotkv.MaxEntry(ctx, s, x, gotkv.TotalSpan())
	if err != nil {
		return nil, err
	}
	var p Path
	var rev uint32
	var after []cadata.ID
	if ent != nil {
		p, rev, err = parseElemKey(ent.Key)
		if err != nil {
			return nil, err
		}
		e, err := parseElem(ent.Value)
		if err != nil {
			return nil, err
		}
		after = append(after, e.ID())
	}
	b := o.gotkv.NewBuilder(s)
	if err := gotkv.CopyAll(ctx, b, o.gotkv.NewIterator(s, x, gotkv.TotalSpan())); err != nil {
		return nil, err
	}
	for _, e := range es {
		p = p.Successor()
		if err := putElem(ctx, b, p, rev, Elem{
			After:     after,
			Author:    e.Author,
			Data:      e.Data,
			Timestamp: e.Timestamp,
		}); err != nil {
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
	span2 := gotkv.TotalSpan()
	it := o.gotkv.NewIterator(s, x, span2)

	var n int
	var ent gotkv.Entry
	for n < len(buf) {
		if err := it.Next(ctx, &ent); err != nil {
			if errors.Is(err, gotkv.EOS) {
				break
			}
			return n, err
		}
		p, _, err := parseElemKey(ent.Key)
		if err != nil {
			return 0, err
		}
		buf[n].Path = p
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
