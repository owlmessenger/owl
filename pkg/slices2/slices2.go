package slices2

import "golang.org/x/sync/errgroup"

// DedupSorted removes duplicate items according to eq
// It doesn't actually matter how the items are sorted as long as items which could be the same are adjacent.
func DedupSorted[T comparable, S ~[]T](xs S) S {
	var deleted int
	for i := range xs {
		if i > 0 && xs[i] == xs[i-1] {
			deleted++
		} else {
			xs[i-deleted] = xs[i]
		}
	}
	return xs[:len(xs)-deleted]
}

// DedupSortedFunc removes duplicate items according to eq
// It doesn't actually matter how the items are sorted as long as items which could be the same are adjacent.
func DedupSortedFunc[T any, S ~[]T](xs S, eq func(a, b T) bool) S {
	var deleted int
	for i := range xs {
		if i > 0 && eq(xs[i], xs[i-1]) {
			deleted++
		} else {
			xs[i-deleted] = xs[i]
		}
	}
	return xs[:len(xs)-deleted]
}

func Merge[T any, S ~[]T](a, b S, lt func(a, b T) bool) (out S) {
	var i, j int
	for i < len(a) && j < len(b) {
		switch {
		case lt(a[i], b[j]):
			out = append(out, a[i])
			i++
		case lt(b[j], a[i]):
			out = append(out, b[j])
			j++
		default:
			out = append(out, b[j])
			i++
			j++
		}
	}
	for ; i < len(a); i++ {
		out = append(out, a[i])
	}
	for ; j < len(b); j++ {
		out = append(out, b[j])
	}
	return out
}

func Map[A, B any, SA ~[]A, SB []B](xs SA, fn func(A) B) (ys SB) {
	ys = make(SB, len(xs))
	for i := range xs {
		ys[i] = fn(xs[i])
	}
	return ys
}

// ParMap calls fn on each item in xs in parallel.
func ParMap[X, Y any, SX ~[]X](xs SX, fn func(X) (Y, error)) ([]Y, error) {
	ys := make([]Y, len(xs))
	eg := errgroup.Group{}
	for i := range xs {
		i := i
		eg.Go(func() error {
			y, err := fn(xs[i])
			if err != nil {
				return err
			}
			ys[i] = y
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return ys, nil
}

func FoldLeft[X, Acc any, S ~[]X](xs S, init Acc, fn func(Acc, X) Acc) Acc {
	a := init
	for i := range xs {
		a = fn(a, xs[i])
	}
	return a
}

func FoldRight[X, Acc any, S ~[]X](xs S, init Acc, fn func(Acc, X) Acc) Acc {
	a := init
	for i := len(xs) - 1; i >= 0; i-- {
		a = fn(a, xs[i])
	}
	return a
}
