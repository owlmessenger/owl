package slices2

// DedupSorted removes duplicate items according to eq
func DedupSorted[T any, S ~[]T](xs S, eq func(a, b T) bool) S {
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
