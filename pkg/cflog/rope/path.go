package rope

import (
	"fmt"

	"golang.org/x/exp/slices"
)

type Path []uint64

func (p Path) Next(indent uint8) Path {
	delta := make(Path, indent+1)
	delta[indent] = 1
	return PathAdd(p, delta)
}

func PathCompare(a, b Path) int {
	return slices.Compare(a, b)
}

// PathSub performs a - b
func PathSub(a, b Path) (ret Path) {
	switch {
	case len(a) == 0 && len(b) == 0:
		return Path{0}
	case len(a) == 1 && len(b) == 1:
		return Path{a[0] - b[0]}
	case len(a) == 1 && len(b) == 0:
		return a
	default:
		panic(fmt.Sprint(a, b))
	}
}

func PathAdd(a, b Path) (ret Path) {
	if len(a) > 1 || len(b) > 1 {
		panic(a)
	}
	if len(a) == 0 {
		return append(ret, b...)
	}
	if len(b) == 0 {
		return append(ret, a...)
	}
	ret = Path{a[0] + b[0]}
	return ret
}
