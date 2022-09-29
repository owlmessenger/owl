package rope

import (
	"context"
	"fmt"
)

type Builder[Ref any] struct {
	s                 WriteStorage[Ref]
	meanSize, maxSize int
	seed              *[16]byte

	levels []*StreamWriter[Ref]
	isDone bool
	root   *Root[Ref]
}

func NewBuilder[Ref any](s WriteStorage[Ref], meanSize, maxSize int, seed *[16]byte) *Builder[Ref] {
	return &Builder[Ref]{
		s:        s,
		meanSize: meanSize,
		maxSize:  maxSize,
		seed:     new([16]byte),
	}
}

func (b *Builder[R]) Append(ctx context.Context, indent uint8, data []byte) error {
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

func (b *Builder[Ref]) Finish(ctx context.Context) (*Root[Ref], error) {
	b.isDone = true
	for i := range b.levels {
		if err := b.levels[i].Flush(ctx); err != nil {
			return nil, err
		}
	}
	return b.root, nil
}

func (b *Builder[Ref]) getWriter(level int) *StreamWriter[Ref] {
	for len(b.levels) <= level {
		b.levels = append(b.levels, b.newWriter(len(b.levels)))
	}
	return b.levels[level]
}

func (b *Builder[Ref]) newWriter(level int) *StreamWriter[Ref] {
	return NewStreamWriter(b.s, b.meanSize, b.maxSize, b.seed, func(ctx context.Context, idx Index[Ref]) error {
		if b.isDone && level == len(b.levels)-1 {
			b.root = &Root[Ref]{
				Ref:   idx.Ref,
				Sum:   idx.Sum,
				Depth: uint8(level),
			}
			return nil
		}
		sw2 := b.getWriter(level + 1)
		return sw2.Append(ctx, idx.Sum, b.s.MarshalRef(idx.Ref))
	})
}

func (b *Builder[Ref]) syncedBelow() int {
	for i := range b.levels {
		if b.levels[i].Buffered() != 0 {
			return i
		}
	}
	return len(b.levels)
}

func (b *Builder[Ref]) writeAt(ctx context.Context, level int, ent Entry) error {
	if b.syncedBelow() <= level {
		panic("write to builder at wrong level")
	}
	return b.getWriter(level).Append(ctx, ent.Path, ent.Value)
}
