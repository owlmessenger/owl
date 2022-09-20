package cflog

import (
	"context"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/owlmessenger/owl/pkg/owldag"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestAppendRead(t *testing.T) {
	s := newStore(t)
	op := newLogOp(t)

	root, err := op.NewEmpty(ctx, s)
	require.NoError(t, err)

	buf := make([]Entry, 10)
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
	return cadata.NewMem(owldag.Hash, 1<<20)
}

func newText(x string) EntryParams {
	return EntryParams{
		Data: jsonMarshal(x),
	}
}
