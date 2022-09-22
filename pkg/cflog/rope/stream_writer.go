package rope

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/dchest/siphash"
)

type IndexCallback = func(context.Context, Index) error

type StreamWriter struct {
	s        cadata.Store
	meanSize int
	maxSize  int
	cb       IndexCallback
	seed     *[16]byte

	last Path
	buf  []byte
}

func NewStreamWriter(s cadata.Store, meanSize, maxSize int, cb IndexCallback) *StreamWriter {
	if meanSize > maxSize {
		panic(fmt.Sprintf("%d > %d", meanSize, maxSize))
	}
	if s.MaxSize() < maxSize {
		maxSize = s.MaxSize()
	}
	return &StreamWriter{
		s:        s,
		meanSize: meanSize,
		maxSize:  maxSize,
		cb:       cb,
		seed:     new([16]byte),

		buf: make([]byte, 0, maxSize),
	}
}

func (sw *StreamWriter) Append(ctx context.Context, path Path, data []byte) error {
	if path == nil {
		path = Path{0}
	}
	if PathCompare(path, sw.last) <= 0 {
		return fmt.Errorf("%v <= %v", path, sw.last)
	}
	l := entryEncodedLen(len(data))
	if l > sw.maxSize {
		return fmt.Errorf("data exceeds max node size. %d > %d", l, sw.maxSize)
	}
	if len(sw.buf)+l > sw.maxSize {
		if err := sw.Flush(ctx); err != nil {
			return err
		}
	}
	indent := uint8(len(path) - 1)
	sw.buf = appendEntry(sw.buf, indent, data)
	entryData := sw.buf[len(sw.buf)-l:]
	sw.last = append(sw.last, path...)
	if sw.isSplitPoint(entryData) {
		return sw.Flush(ctx)
	}
	return nil
}

func (sw *StreamWriter) Flush(ctx context.Context) error {
	ref, err := sw.s.Post(ctx, sw.buf)
	if err != nil {
		return err
	}
	sw.buf = sw.buf[:0]
	return sw.cb(ctx, Index{
		Ref:  ref,
		Last: sw.last,
	})
}

func (sw *StreamWriter) Last() Path {
	return sw.last
}

func (sw *StreamWriter) isSplitPoint(entryData []byte) bool {
	r := hash64(entryData, sw.seed)
	prob := math.MaxUint64 / uint64(sw.meanSize) * uint64(len(entryData))
	return r < prob
}

// entryEncodedLen is the number of bytes appendEntry will append.
func entryEncodedLen(dataLen int) int {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(1+dataLen))
	return n + 1 + dataLen
}

// appendEntry appends an entry to out
// varint | 1 byte indent | variable length data |
func appendEntry(out []byte, indent uint8, data []byte) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(1+len(data)))
	out = append(out, buf[:n]...)
	out = append(out, indent)
	out = append(out, data...)
	return out
}

func hash64(data []byte, key *[16]byte) uint64 {
	en := binary.LittleEndian
	k1 := en.Uint64(key[:8])
	k2 := en.Uint64(key[8:])
	return siphash.Hash(k1, k2, data)
}
