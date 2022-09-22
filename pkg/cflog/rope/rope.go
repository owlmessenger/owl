package rope

import (
	"context"
	"encoding/binary"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"golang.org/x/exp/slices"
)

var EOS = kvstreams.EOS

type Entry struct {
	Path  Path
	Value []byte
}

type Ref = cadata.ID

type Path []uint64

func (p Path) Marshal() (ret []byte) {
	for i := range p {
		buf := [8]byte{}
		binary.BigEndian.PutUint64(buf[:], p[i])
		ret = append(ret, buf[:]...)
	}
	return ret
}

func (p Path) Next(indent uint8) Path {
	if len(p) == 0 {
		return Path{0}
	}
	ret := append(Path{}, p...)
	ret[indent]++
	return ret
}

func PathCompare(a, b Path) int {
	return slices.Compare(a, b)
}

type Root struct {
	Ref   cadata.ID `json:"ref"`
	Depth uint8     `json:"depth"`
	Last  Path      `json:"last"`
}

type Index struct {
	Ref  Ref
	Last Path
}

func Copy(ctx context.Context, b *Builder, it *Iterator) error {
	panic("not implemented")
}

func Interleave(ctx context.Context, b *Builder, its []*Iterator, lt func(a, b *Entry) bool) error {
	panic("not implemented")
}
