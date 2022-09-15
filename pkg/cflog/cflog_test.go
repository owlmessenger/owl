package cflog

import (
	"context"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"
)

var ctx = context.Background()

func TestAppendRead(t *testing.T) {
	s := newStore(t)
	op := newLogOp(t)

	root, err := op.NewEmpty(ctx, s)
	require.NoError(t, err)

	buf := make([]Pair, 10)
	n, err := op.Read(ctx, s, *root, Path{}, buf)
	require.NoError(t, err)
	require.Equal(t, 0, n)

	root, err = op.Append(ctx, s, *root, nil, []EntryParams{
		newText("one"),
		newText("two"),
		newText("three"),
	})
	require.NoError(t, err)

	n, err = op.Read(ctx, s, *root, nil, buf)
	require.NoError(t, err)
	require.Equal(t, 3, n)
}

func newLogOp(t testing.TB) Operator {
	kvop := gotkv.NewOperator(1<<12, 1<<16)
	return New(&kvop)
}

func newStore(t testing.TB) cadata.Store {
	return cadata.NewMem(func(x []byte) cadata.ID { return sha3.Sum256(x) }, 1<<20)
}

func newText(x string) EntryParams {
	return EntryParams{
		Data: jsonMarshal(x),
	}
}
