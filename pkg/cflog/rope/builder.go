package rope

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

type Builder struct {
	s                 cadata.Store
	meanSize, maxSize int

	levels []*StreamWriter
	isDone bool
	root   *Root
}

func NewBuilder(s cadata.Store, meanSize, maxSize int) *Builder {
	b := &Builder{
		s:        s,
		meanSize: meanSize,
		maxSize:  maxSize,
	}
	b.levels = []*StreamWriter{b.getWriter(0)}
	return b
}

func (b *Builder) Append(ctx context.Context, indent uint8, data []byte) error {
	return nil
}

func (b *Builder) Finish(ctx context.Context) (*Root, error) {
	b.isDone = true
	err := b.levels[0].Flush(ctx)
	return b.root, err
}

func (b *Builder) getWriter(level int) *StreamWriter {
	if len(b.levels) <= level {
		sw := NewStreamWriter(b.s, b.meanSize, b.maxSize, func(ctx context.Context, idx Index) error {
			if b.isDone && level == len(b.levels)-1 {
				b.root = &Root{
					Ref:   idx.Ref,
					Last:  idx.Last,
					Depth: uint8(level),
				}
				return nil
			}
			sw2 := b.getWriter(level + 1)
			if err := sw2.Append(ctx, idx.Last, nil); err != nil {
				return err
			}
			return nil
		})
		b.levels = append(b.levels, sw)
	}
	return b.levels[level]
}
