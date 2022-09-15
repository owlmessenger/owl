package cflog

import (
	"fmt"
	"strings"

	"golang.org/x/exp/slices"
)

type Path []uint64

func (p Path) Successor() (ret Path) {
	if len(p) == 0 {
		return Path{0}
	}
	ret = append(ret, p...)
	ret[len(ret)-1]++
	return ret
}

func (p Path) String() string {
	sb := strings.Builder{}
	for i := range p {
		if i > 0 {
			sb.WriteString(".")
		}
		fmt.Fprintf(&sb, "%x", p[i])
	}
	return sb.String()
}

func PathCompare(a, b Path) int {
	return slices.Compare(a, b)
}
