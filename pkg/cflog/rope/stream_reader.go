package rope

import (
	"context"
	"encoding/binary"
	"errors"
	"log"
)

func singleRef[Ref any](ref Ref) func(context.Context) (*Ref, error) {
	var emitted bool
	return func(ctx context.Context) (*Ref, error) {
		if emitted {
			return nil, nil
		}
		emitted = true
		return &ref, nil
	}
}

type StreamReader[Ref any] struct {
	s          Storage[Ref]
	getNext    func(context.Context) (*Ref, error)
	offset     Path
	buf        []byte
	begin, end int
}

func NewStreamReader[Ref any](s Storage[Ref], offset Path, getNext func(context.Context) (*Ref, error)) *StreamReader[Ref] {
	return &StreamReader[Ref]{
		s:       s,
		offset:  offset,
		getNext: getNext,
		buf:     make([]byte, s.MaxSize()),
	}
}

func (sr *StreamReader[Ref]) Peek(ctx context.Context, ent *Entry) error {
	_, err := sr.parseNext(ctx, ent)
	if err != nil {
		return err
	}
	return nil
}

func (sr *StreamReader[Ref]) Next(ctx context.Context, ent *Entry) error {
	n, err := sr.parseNext(ctx, ent)
	if err != nil {
		return err
	}
	sr.begin += n
	sr.setOffset(ent.Path)
	return nil
}

func (sr *StreamReader[Ref]) Buffered() int {
	return sr.end - sr.begin
}

func (sr *StreamReader[Ref]) Last() Path {
	return sr.offset
}

func (sr *StreamReader[Ref]) parseNext(ctx context.Context, ent *Entry) (int, error) {
	if sr.end-sr.begin <= 0 {
		ref, err := sr.getNext(ctx)
		if err != nil {
			return 0, err
		}
		if ref == nil {
			return 0, EOS
		}
		sr.end, err = sr.s.Get(ctx, *ref, sr.buf)
		if err != nil {
			return 0, err
		}
		sr.begin = 0
	}
	if sr.end-sr.begin <= 0 {
		return 0, EOS
	}
	return parseEntry(ent, sr.offset, sr.buf[sr.begin:sr.end])
}

func (sr *StreamReader[Ref]) setOffset(p Path) {
	sr.offset = append(sr.offset[:0], p...)
}

func parseEntry(e *Entry, last Path, in []byte) (int, error) {
	n, data, err := parseLP(in)
	if err != nil {
		return 0, err
	}
	retN := n

	// key
	n, err = parsePath(e, last, data)
	if err != nil {
		return 0, err
	}
	data = data[n:]

	// value
	_, value, err := parseLP(data)
	if err != nil {
		return 0, err
	}
	e.Value = append(e.Value[:0], value...)

	return retN, nil
}

func parsePath(ent *Entry, last Path, in []byte) (int, error) {
	n, data, err := parseLP(in)
	if err != nil {
		return 0, err
	}
	var delta Path
	for len(data) > 0 {
		n, y, err := parseVarint(data)
		if err != nil {
			return 0, err
		}
		delta = append(delta, y)
		data = data[n:]
	}
	ent.Path = PathAdd(last, delta)
	return n, nil
}

func parseLP(in []byte) (int, []byte, error) {
	l, n := binary.Uvarint(in)
	if n <= 0 {
		log.Printf("%q", in)
		return 0, nil, errors.New("problem parsing varint")
	}
	out := in[n:]
	if len(out) < int(l) {
		return 0, nil, errors.New("short entry")
	}
	return int(l) + n, out[:l], nil
}

func parseVarint(x []byte) (int, uint64, error) {
	y, n := binary.Uvarint(x)
	if n <= 0 {
		return 0, 0, errors.New("problem parsing varint")
	}
	return n, y, nil
}
