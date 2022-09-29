package rope

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state"
)

type Span = state.Span[Path]

func PointSpan(x Path) Span {
	return state.PointSpan(x)
}

func TotalSpan() Span {
	return state.TotalSpan[Path]()
}

type Iterator[Ref any] struct {
	s    Storage[Ref]
	root Root[Ref]
	span Span

	readRoot bool
	levels   []*StreamReader[Ref]
	ctx      context.Context
}

func NewIterator[Ref any](s Storage[Ref], root Root[Ref], span Span) *Iterator[Ref] {
	it := &Iterator[Ref]{
		s:    s,
		root: root,

		levels: make([]*StreamReader[Ref], root.Depth+1),
	}
	return it
}

func (it *Iterator[Ref]) Peek(ctx context.Context, ent *Entry) error {
	defer it.setUnsetCtx(ctx)()
	for {
		sr := it.getReader(0)
		if err := sr.Peek(ctx, ent); err != nil {
			return err
		}
		if cmp := it.span.Compare(ent.Path, PathCompare); cmp < 0 {
			return EOS
		} else if cmp == 0 {
			return nil
		} else {
			if err := sr.Next(ctx, ent); err != nil {
				return err
			}
		}
	}
}

func (it *Iterator[Ref]) Next(ctx context.Context, ent *Entry) error {
	defer it.setUnsetCtx(ctx)()
	for {
		if err := it.getReader(0).Next(ctx, ent); err != nil {
			return err
		}
		if cmp := it.span.Compare(ent.Path, PathCompare); cmp < 0 {
			return EOS
		} else if cmp == 0 {
			return nil
		}
	}
}

func (it *Iterator[Ref]) Seek(ctx context.Context, gteq Path) error {
	defer it.setUnsetCtx(ctx)()
	if cmp := PathCompare(gteq, it.levels[0].Last()); cmp <= 0 {
		return fmt.Errorf("cannot seek backwards")
	}
	var ent Entry
	sr := it.getReader(0)
	for {
		if err := sr.Peek(ctx, &ent); err != nil {
			return err
		}
		if PathCompare(ent.Path, gteq) >= 0 {
			return nil
		}
		if err := sr.Next(ctx, &ent); err != nil {
			return err
		}
	}
}

func (it *Iterator[Ref]) getReader(level int) *StreamReader[Ref] {
	if level >= len(it.levels) {
		panic(level)
	}
	if it.levels[level] == nil {
		it.levels[level] = it.newReader(level)
	}
	return it.levels[level]
}

func (it *Iterator[Ref]) newReader(level int) *StreamReader[Ref] {
	return NewStreamReader(it.s, nil, func(context.Context) (*Ref, error) {
		if level+1 >= len(it.levels) {
			if it.readRoot {
				return nil, nil
			} else {
				it.readRoot = true
				return &it.root.Ref, nil
			}
		}
		sr := it.getReader(level + 1)
		if sr == nil {
			return nil, nil
		}
		idx, err := readIndex(it.ctx, sr)
		if err != nil {
			return nil, err
		}
		return &idx.Ref, nil
	})
}

func (it *Iterator[Ref]) setUnsetCtx(ctx context.Context) func() {
	it.ctx = ctx
	return func() { it.ctx = nil }
}

func (it *Iterator[Ref]) syncedBelow() int {
	for i := range it.levels {
		if it.levels[i].Buffered() == 0 {
			return i
		}
	}
	return len(it.levels)
}

func (it *Iterator[Ref]) readAt(ctx context.Context, level int, ent *Entry) error {
	if level >= it.syncedBelow() {
		panic("rope.Iterator[Ref]: read from wrong level")
	}
	return it.levels[level].Next(ctx, ent)
}
