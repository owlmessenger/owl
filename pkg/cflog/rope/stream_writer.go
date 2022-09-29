package rope

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/dchest/siphash"
)

type IndexCallback[Ref any] func(context.Context, Index[Ref]) error

type StreamWriter[Ref any] struct {
	s        WriteStorage[Ref]
	meanSize int
	maxSize  int
	cb       IndexCallback[Ref]
	seed     *[16]byte

	last Path
	buf  []byte
}

func NewStreamWriter[Ref any](s WriteStorage[Ref], meanSize, maxSize int, seed *[16]byte, cb IndexCallback[Ref]) *StreamWriter[Ref] {
	if meanSize > maxSize {
		panic(fmt.Sprintf("%d > %d", meanSize, maxSize))
	}
	if s.MaxSize() < maxSize {
		maxSize = s.MaxSize()
	}
	if seed == nil {
		seed = new([16]byte)
	}
	return &StreamWriter[Ref]{
		s:        s,
		meanSize: meanSize,
		maxSize:  maxSize,
		cb:       cb,
		seed:     seed,

		buf: make([]byte, 0, maxSize),
	}
}

func (sw *StreamWriter[Ref]) Append(ctx context.Context, p Path, data []byte) error {
	if len(sw.last) != 0 && PathCompare(p, sw.last) <= 0 {
		return fmt.Errorf("%v <= %v", p, sw.last)
	}
	l := entryEncodedLen(sw.last, p, data)
	if l > sw.maxSize {
		return fmt.Errorf("data exceeds max node size. %d > %d", l, sw.maxSize)
	}
	if len(sw.buf)+l > sw.maxSize {
		if err := sw.Flush(ctx); err != nil {
			return err
		}
	}
	sw.buf = appendEntry(sw.buf, sw.last, p, data)
	entryData := sw.buf[len(sw.buf)-l:]
	sw.setLast(p)
	if sw.isSplitPoint(entryData) {
		return sw.Flush(ctx)
	}
	return nil
}

func (sw *StreamWriter[Ref]) Flush(ctx context.Context) error {
	ref, err := sw.s.Post(ctx, sw.buf)
	if err != nil {
		return err
	}
	sw.buf = sw.buf[:0]
	return sw.cb(ctx, Index[Ref]{
		Ref: ref,
		Sum: sw.last,
	})
}

func (sw *StreamWriter[Ref]) Buffered() int {
	return len(sw.buf)
}

func (sw *StreamWriter[Ref]) Last() Path {
	return sw.last
}

func (sw *StreamWriter[Ref]) isSplitPoint(entryData []byte) bool {
	r := hash64(entryData, sw.seed)
	prob := math.MaxUint64 / uint64(sw.meanSize) * uint64(len(entryData))
	return r < prob
}

func (sw *StreamWriter[Ref]) setLast(p Path) {
	sw.last = append(sw.last[:0], p...)
}

// appendEntry appends an entry to out
// varint | 1 byte indent | variable length data |
func appendEntry(out []byte, prev, p Path, data []byte) []byte {
	out = appendVarint(out,
		uint64(pathEncodedLen(prev, p))+
			uint64(lpEncodedLen(len(data))),
	)
	out = appendPath(out, prev, p)
	out = appendLP(out, data)
	return out
}

// entryEncodedLen is the number of bytes appendEntry will append.
func entryEncodedLen(last, p Path, data []byte) int {
	return lpEncodedLen(pathEncodedLen(last, p) + lpEncodedLen(len(data)))
}

func pathEncodedLen(prev, next Path) (ret int) {
	delta := PathSub(next, prev)
	var total int
	for i := range delta {
		total += varintLen(delta[i])
	}
	return lpEncodedLen(total)
}

func appendPath(out []byte, prev, next Path) []byte {
	delta := PathSub(next, prev)
	var total int
	for i := range delta {
		total += varintLen(delta[i])
	}

	out = appendVarint(out, uint64(total))
	for i := range delta {
		out = appendVarint(out, delta[i])
	}
	return out
}

// appendLP appends a length prefixed x to out and returns the result
func appendLP(out []byte, x []byte) []byte {
	out = appendVarint(out, uint64(len(x)))
	out = append(out, x...)
	return out
}

// lpEncodedLen is the total length of a length-prefixed string of length dataLen
func lpEncodedLen(dataLen int) int {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(dataLen))
	return n + dataLen
}

// appendVarint appends x varint-encoded to out and returns the result.
func appendVarint(out []byte, x uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], x)
	return append(out, buf[:n]...)
}

// varintLen returns the number of bytes it would take to encode x as a varint
func varintLen(x uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], x)
}

func hash64(data []byte, key *[16]byte) uint64 {
	en := binary.LittleEndian
	k0 := en.Uint64(key[:8])
	k1 := en.Uint64(key[8:])
	return siphash.Hash(k0, k1, data)
}
