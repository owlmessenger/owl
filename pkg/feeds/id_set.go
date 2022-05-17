package feeds

import (
	"bytes"
	"encoding/json"
	"fmt"

	"golang.org/x/exp/slices"
)

type IDSet[T ~[32]byte] []T

func NewIDSet[T ~[32]byte](elems ...T) IDSet[T] {
	ret := IDSet[T](elems)
	ret = slices.Clone(ret)
	slices.SortFunc(ret, lessThan[T])
	return dedupSorted(ret)
}

func (s IDSet[T]) Add(x T) IDSet[T] {
	i, exists := slices.BinarySearchFunc(s, x, compare[T])
	if exists {
		return s
	}
	ret := slices.Clone(s)
	return slices.Insert(ret, i, x)
}

func (s IDSet[T]) Remove(x T) IDSet[T] {
	i, exists := slices.BinarySearchFunc(s, x, compare[T])
	if !exists {
		return s
	}
	ret := slices.Clone(s)
	return slices.Delete(ret, i, i+1)
}

func (s IDSet[T]) Contains(x T) bool {
	i, found := slices.BinarySearchFunc(s, x, compare[T])
	return found && s[i] == x
}

func Union[T ~[32]byte](a, b IDSet[T]) IDSet[T] {
	return mergeSlices(a, b, lessThan[T])
}

func (s IDSet[T]) IsEmpty() bool {
	return len(s) > 0
}

func (s IDSet[T]) MarshalJSON() ([]byte, error) {
	if !slices.IsSortedFunc(s, lessThan[T]) {
		return nil, fmt.Errorf("invalid IDSet %v", s)
	}
	x := []T(s)
	return json.Marshal(x)
}

func (s *IDSet[T]) UnmarshalJSON(data []byte) error {
	var x []T
	if !slices.IsSortedFunc(x, lessThan[T]) {
		return fmt.Errorf("invalid IDSet %v", s)
	}
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	*s = IDSet[T](x)
	return nil
}

func lessThan[T ~[32]byte](a, b T) bool {
	return compare(a, b) < 0
}

func compare[T ~[32]byte](a, b T) int {
	return bytes.Compare(a[:], b[:])
}

func dedupSorted[T comparable, S ~[]T](x S) S {
	ret := slices.Clone(x)
	var deleted int
	for i := range ret {
		if i > 0 && ret[i] == ret[i-1] {
			deleted++
		} else {
			ret[i-deleted] = ret[i]
		}
	}
	return ret[:len(ret)-deleted]
}

func mergeSlices[T any, S ~[]T](a, b S, fn func(a, b T) bool) (out S) {
	var i, j int
	for i < len(a) && j < len(b) {
		switch {
		case fn(a[i], b[j]):
			out = append(out, a[i])
			i++
		case fn(b[j], a[i]):
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
