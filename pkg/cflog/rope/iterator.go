package rope

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

type Iterator struct {
	s    cadata.Store
	root Root

	readRoot bool
	levels   []*StreamReader
	ctx      context.Context
}

func NewIterator(s cadata.Store, root Root) *Iterator {
	it := &Iterator{
		s:    s,
		root: root,

		levels: make([]*StreamReader, root.Depth+1),
	}
	return it
}

func (it *Iterator) Peek(ctx context.Context, ent *Entry) error {
	defer it.setUnsetCtx(ctx)()
	return it.getReader(0).Next(ctx, ent)
}

func (it *Iterator) Next(ctx context.Context, ent *Entry) error {
	defer it.setUnsetCtx(ctx)()
	return it.getReader(0).Next(ctx, ent)
}

func (it *Iterator) Seek(ctx context.Context, gteq Path) error {
	defer it.setUnsetCtx(ctx)()
	return nil
}

func (it *Iterator) getReader(level int) *StreamReader {
	if level >= len(it.levels) {
		it.levels[level] = NewStreamReader(it.s, nil, func(context.Context) (*Ref, error) {
			if it.readRoot {
				// EOS for the iterator
				return nil, nil
			}
			it.readRoot = true
			return &it.root.Ref, nil
		})
	} else if it.levels[level] == nil {
		it.levels[level] = NewStreamReader(it.s, nil, func(context.Context) (*Ref, error) {
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
	return it.levels[level]
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
