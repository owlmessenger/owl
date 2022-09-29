package rope

import (
	"context"
	"errors"

	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

// EOS is the End-Of-Stream
var EOS = kvstreams.EOS

// Entry is a single entry in the Rope
type Entry struct {
	Path  Path
	Value []byte
}

// Root is a root of a Rope
type Root[Ref any] struct {
	Ref   Ref   `json:"ref"`
	Depth uint8 `json:"depth"`
	Sum   Path  `json:"sum"`
}

// Index is a reference to a node and the sum of the change in path that would
// occur from concatenation the index.
type Index[Ref any] struct {
	Ref Ref
	// Adding Sum to an offset is the same as adding everything reachable from Ref to the offset.
	Sum Path
}

func Copy[Ref any](ctx context.Context, b *Builder[Ref], it *Iterator[Ref]) error {
	var ent Entry
	for {
		level := min(b.syncedBelow(), it.syncedBelow())
		if err := it.readAt(ctx, level, &ent); err != nil {
			if errors.Is(err, EOS) {
				break
			}
			return err
		}
		if err := b.writeAt(ctx, level, ent); err != nil {
			return err
		}
	}
	return nil
}

// ListEntries lists the entries in the node referenced by idx.
func ListEntries[Ref any](ctx context.Context, s Storage[Ref], offset Path, ref Ref) (ret []Entry, _ error) {
	sr := NewStreamReader(s, offset, singleRef(ref))
	for {
		var ent Entry
		if err := sr.Next(ctx, &ent); err != nil {
			if errors.Is(err, EOS) {
				break
			}
			return nil, err
		}
		ret = append(ret, ent)
	}
	return ret, nil
}

// ListIndexes lists the indexes in the node referenced by idx.
func ListIndexes[Ref any](ctx context.Context, s Storage[Ref], offset Path, ref Ref) (ret []Index[Ref], _ error) {
	sr := NewStreamReader(s, offset, singleRef(ref))
	for {
		idx, err := readIndex(ctx, sr)
		if err != nil {
			if errors.Is(err, EOS) {
				break
			}
			return nil, err
		}
		ret = append(ret, *idx)
	}
	return ret, nil
}

func readIndex[Ref any](ctx context.Context, sr *StreamReader[Ref]) (*Index[Ref], error) {
	var ent Entry
	if err := sr.Next(ctx, &ent); err != nil {
		return nil, err
	}
	ref, err := sr.s.ParseRef(ent.Value)
	if err != nil {
		return nil, err
	}
	return &Index[Ref]{
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
