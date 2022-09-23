package rope

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

// EOS is the End-Of-Stream
var EOS = kvstreams.EOS

// Entry is a single entry in the Rope
type Entry struct {
	Path  Path
	Value []byte
}

// Ref is a reference to a node
type Ref = cadata.ID

// Root is a root of a Rope
type Root struct {
	Ref   cadata.ID `json:"ref"`
	Depth uint8     `json:"depth"`
	Sum   Path      `json:"sum"`
}

// Index is a reference to a node and the sum of the change in path that would
// occur from concatenation the index.
type Index struct {
	Ref Ref
	Sum Path
}

func Copy(ctx context.Context, b *Builder, it *Iterator) error {
	var ent Entry
	for {
		level := min(b.syncedBelow(), it.syncedBelow())
		if err := it.readAt(ctx, level, &ent); err != nil {
			return err
		}
		if err := b.writeAt(ctx, level, indexFromEnt(ent)); err != nil {
			return err
		}
	}
}

func Interleave(ctx context.Context, b *Builder, its []*Iterator, lt func(a, b *Entry) bool) error {
	panic("not implemented")
}

func indexFromEnt(ent Entry) Index {
	return Index{
		Sum: ent.Path,
		Ref: cadata.IDFromBytes(ent.Value),
	}
}

func readIndex(ctx context.Context, sr *StreamReader) (*Index, error) {
	var ent Entry
	if err := sr.Next(ctx, &ent); err != nil {
		return nil, err
	}
	ref := cadata.IDFromBytes(ent.Value)
	return &Index{
		Sum: ent.Path,
		Ref: ref,
	}, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
