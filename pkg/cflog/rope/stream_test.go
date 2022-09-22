package rope

import (
	"context"
	"strconv"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/owlmessenger/owl/pkg/owldag"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestWriteRead(t *testing.T) {
	const N = 10000
	s := newStore(t)
	var refs []Ref
	sw := NewStreamWriter(s, 1<<12, 1<<16, func(ctx context.Context, idx Index) error {
		refs = append(refs, idx.Ref)
		return nil
	})
	for i := 0; i < N; i++ {
		v := []byte("hello world " + strconv.Itoa(i))
		err := sw.Append(ctx, Path{uint64(i)}, v)
		require.NoError(t, err)
	}
	require.NoError(t, sw.Flush(ctx))
	require.Greater(t, s.Len(), 2)

	sr := NewStreamReader(s, nil, func(context.Context) (*Ref, error) {
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

func newStore(t testing.TB) *cadata.MemStore {
	return cadata.NewMem(owldag.Hash, 1<<20)
}
