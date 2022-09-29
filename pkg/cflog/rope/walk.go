package rope

import "context"

type Walker[Ref any] struct {
	// If Before returns false, the descendents are skipped.
	Before func(Ref) bool
	// ForEach is called for each entry
	ForEach func(Entry) error
	// After is called on a node after it's descendants have been completed.
	After func(Ref) error
}

func Walk[Ref any](ctx context.Context, s Storage[Ref], root Root[Ref], w Walker[Ref]) error {
	return walk(ctx, s, root, w, nil)
}

func walk[Ref any](ctx context.Context, s Storage[Ref], root Root[Ref], w Walker[Ref], offset Path) error {
	if root.Depth > 0 {
		if !w.Before(root.Ref) {
			return nil
		}
		idxs, err := ListIndexes(ctx, s, offset, root.Ref)
		if err != nil {
			return err
		}
		for i, idx := range idxs {
			var offset Path
			if i > 0 {
				offset = idxs[i-1].Sum
			}
			root2 := Root[Ref]{
				Depth: root.Depth - 1,
				Ref:   idx.Ref,
				Sum:   idx.Sum,
			}
			if err := walk(ctx, s, root2, w, offset); err != nil {
				return err
			}
		}
	} else {
		ents, err := ListEntries(ctx, s, offset, root.Ref)
		if err != nil {
			return err
		}
		for _, ent := range ents {
			if err := w.ForEach(ent); err != nil {
				return err
			}
		}
	}
	return w.After(root.Ref)
}
