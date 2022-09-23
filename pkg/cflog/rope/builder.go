package rope

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
)

type Builder struct {
	s                 cadata.Store
	meanSize, maxSize int
	seed              *[16]byte

	levels []*StreamWriter
	isDone bool
	root   *Root
}

func NewBuilder(s cadata.Store, meanSize, maxSize int, seed *[16]byte) *Builder {
	return &Builder{
		s:        s,
		meanSize: meanSize,
		maxSize:  maxSize,
		seed:     new([16]byte),
	}
}

func (b *Builder) Append(ctx context.Context, indent uint8, data []byte) error {
	if b.isDone {
		return fmt.Errorf("builder is finished")
	}
	if indent > 0 {
		panic(indent) // TODO: support paths
	}
	w := b.getWriter(0)
	last := w.Last()
	next := last.Next(indent)
	return w.Append(ctx, next, data)
}

func (b *Builder) Finish(ctx context.Context) (*Root, error) {
	b.isDone = true
	for i := range b.levels {
		if err := b.levels[i].Flush(ctx); err != nil {
			return nil, err
		}
	}
	return b.root, nil
}

func (b *Builder) getWriter(level int) *StreamWriter {
	for len(b.levels) <= level {
		b.levels = append(b.levels, b.newWriter(len(b.levels)))
	}
	return b.levels[level]
}

func (b *Builder) newWriter(level int) *StreamWriter {
	return NewStreamWriter(b.s, b.meanSize, b.maxSize, b.seed, func(ctx context.Context, idx Index) error {
		if b.isDone && level == len(b.levels)-1 {
			b.root = &Root{
				Ref:   idx.Ref,
				Sum:   idx.Sum,
				Depth: uint8(level),
			}
			return nil
		}
		sw2 := b.getWriter(level + 1)
		return sw2.Append(ctx, idx.Sum, idx.Ref[:])
	})
}

func (b *Builder) syncedBelow() int {
	for i := range b.levels {
		if b.levels[i].Buffered() != 0 {
			return i
		}
	}
	return len(b.levels)
}

func (b *Builder) writeAt(ctx context.Context, level int, idx Index) error {
	if b.syncedBelow() <= level {
		panic("write to builder at wrong level")
	}
	return b.getWriter(level).Append(ctx, idx.Sum, idx.Ref[:])
}
