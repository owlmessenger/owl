package rope

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/brendoncarroll/go-state/cadata"
)

type StreamReader struct {
	s          cadata.Store
	getNext    func(context.Context) (*Ref, error)
	offset     Path
	buf        []byte
	begin, end int
}

func NewStreamReader(s cadata.Store, offset Path, getNext func(context.Context) (*Ref, error)) *StreamReader {
	return &StreamReader{
		s:       s,
		offset:  offset,
		getNext: getNext,
		buf:     make([]byte, s.MaxSize()),
	}
}

func (sr *StreamReader) Next(ctx context.Context, ent *Entry) error {
	if sr.end-sr.begin <= 0 {
		ref, err := sr.getNext(ctx)
		if err != nil {
			return err
		}
		if ref == nil {
			return EOS
		}
		sr.end, err = sr.s.Get(ctx, *ref, sr.buf)
		if err != nil {
			return err
		}
		sr.begin = 0
	}
	n, err := parseEntry(ent, sr.offset, sr.buf[sr.begin:sr.end])
	if err != nil {
		return err
	}
	sr.offset = append(sr.offset[:0], ent.Path...)
	sr.begin += n
	return nil
}

func parseEntry(e *Entry, last Path, in []byte) (int, error) {
	l, n := binary.Uvarint(in)
	if n <= 0 {
		return 0, errors.New("problem parsing varint")
	}
	in = in[n:]
	if uint64(len(in)) < l {
		return 0, errors.New("short entry")
	}
	indent := in[0]
	data := in[1:l]
	e.Path = append(e.Path[:0], last...)
	if len(e.Path) > int(indent) {
		e.Path = e.Path[:indent+1]
	}
	if len(e.Path) == 0 {
		e.Path = Path{0}
	} else {
		e.Path[indent]++
	}
	e.Value = append(e.Value[:0], data...)
	return n + int(l), nil
}
