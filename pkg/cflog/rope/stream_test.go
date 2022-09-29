package rope

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestWriteRead(t *testing.T) {
	const N = 10000
	s := newStore(t)
	var refs []Ref
	sw := NewStreamWriter(s, defaultMeanSize, defaultMaxSize, new([16]byte), func(ctx context.Context, idx Index[Ref]) error {
		refs = append(refs, idx.Ref)
		return nil
	})
	var v []byte
	for i := 0; i < N; i++ {
		v = fmt.Appendf(v[:0], "hello world %d", i)
		err := sw.Append(ctx, Path{uint64(i)}, v)
		require.NoError(t, err)
	}
	require.NoError(t, sw.Flush(ctx))
	require.Greater(t, s.(writeStore).s.(*cadata.MemStore).Len(), 2)

	sr := NewStreamReader[Ref](s, nil, func(context.Context) (*cadata.ID, error) {
		if len(refs) == 0 {
			return nil, nil
		}
		r := refs[0]
		refs = refs[1:]
		return &r, nil
	})

	var ent Entry
	for i := 0; i < N; i++ {
		expectV := []byte("hello world " + strconv.Itoa(i))
		require.NoError(t, sr.Next(ctx, &ent))
		require.Equal(t, Path{uint64(i)}, ent.Path)
		require.Equal(t, expectV, ent.Value)
	}
	require.ErrorIs(t, sr.Next(ctx, &ent), EOS)
}

func TestEntryWrite(t *testing.T) {
	var out []byte

	prev := Path{1}
	next := Path{2}
	data := []byte("hello world")
	l := entryEncodedLen(prev, next, data)
	out = appendEntry(out, prev, next, data)
	require.Len(t, out, l)

	var ent Entry
	l2, err := parseEntry(&ent, prev, out)
	require.NoError(t, err)
	require.Equal(t, l, l2)
	require.Equal(t, next, ent.Path)
}
