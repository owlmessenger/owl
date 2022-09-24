package rope

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
)

type Span = state.Span[Path]

func PointSpan(x Path) Span {
	return state.PointSpan(x)
}

func TotalSpan() Span {
	return state.TotalSpan[Path]()
}

type Iterator struct {
	s    cadata.Store
	root Root
	span Span

	readRoot bool
	levels   []*StreamReader
	ctx      context.Context
}

func NewIterator(s cadata.Store, root Root, span Span) *Iterator {
	it := &Iterator{
		s:    s,
		root: root,

		levels: make([]*StreamReader, root.Depth+1),
	}
	return it
}

func (it *Iterator) Peek(ctx context.Context, ent *Entry) error {
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

func (it *Iterator) Next(ctx context.Context, ent *Entry) error {
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

func (it *Iterator) Seek(ctx context.Context, gteq Path) error {
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

func (it *Iterator) getReader(level int) *StreamReader {
	if level >= len(it.levels) {
		panic(level)
	}
	if it.levels[level] == nil {
		it.levels[level] = it.newReader(level)
	}
	return it.levels[level]
}

func (it *Iterator) newReader(level int) *StreamReader {
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

func (it *Iterator) setUnsetCtx(ctx context.Context) func() {
	it.ctx = ctx
	return func() { it.ctx = nil }
}

func (it *Iterator) syncedBelow() int {
	for i := range it.levels {
		if it.levels[i].Buffered() == 0 {
			return i
		}
	}
	return len(it.levels)
}

func (it *Iterator) readAt(ctx context.Context, level int, ent *Entry) error {
	if level >= it.syncedBelow() {
		panic("rope.Iterator: read from wrong level")
	}
	return it.levels[level].Next(ctx, ent)
}
