package cflog

import (
	"context"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
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

	root, err = op.AppendBatch(ctx, s, *root, nil, []EntryParams{
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
	return New()
}

func newStore(t testing.TB) cadata.Store {
	return cadata.NewMem(owldag.Hash, 1<<20)
}

func newText(x string) EntryParams {
	return EntryParams{
		Data: jsonMarshal(x),
	}
}
