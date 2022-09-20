package owldag

import (
	"context"
	"testing"

	"github.com/brendoncarroll/go-p2p/p2ptest"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestInit(t *testing.T) {
	store := newStore(t)
	sch := testScheme{}
	state, err := InitState[uint64](ctx, store, 1, nil)
	require.NoError(t, err)
	dag := New[uint64](sch, store, nil, *state)
	x := dag.View()
	t.Log(x)
}

func TestModify(t *testing.T) {
	dag := newTestDAG(t)
	privKey := p2ptest.NewTestKey(t, 0)
	for _, n := range []uint64{2, 3, 5, 7} {
		err := dag.Modify(ctx, privKey, func(s cadata.Store, x uint64) (*uint64, error) {
			y := x * n
			return &y, nil
		})
		require.NoError(t, err)
	}
	x := dag.View()
	require.Equal(t, uint64(210), x)
}

func newStore(t testing.TB) cadata.Store {
	return cadata.NewMem(Hash, MaxNodeSize)
}

type testScheme struct{}

func (testScheme) Merge(ctx context.Context, s cadata.Store, xs []uint64) (*uint64, error) {
	var x uint64
	for i := range xs {
		x *= xs[i]
	}
	return &x, nil
}

func (testScheme) Validate(ctx context.Context, s cadata.Store, consult ConsultFunc, x uint64) error {
	return nil
}

// ValidateStep checks that next is valid, given that prev is known to be valid.
func (ts testScheme) ValidateStep(ctx context.Context, s cadata.Store, consult ConsultFunc, prev, next uint64) error {
	return ts.Validate(ctx, s, consult, next)
}

// Sync ensures that all of the data reachable by x is in dst, using src
// to get missing data.
func (s testScheme) Sync(ctx context.Context, src cadata.Getter, dst cadata.Store, x uint64) error {
	panic("not implemented") // TODO: Implement
}

func newTestDAG(t testing.TB) *DAG[uint64] {
	store := newStore(t)
	sch := testScheme{}
	state, err := InitState[uint64](ctx, store, 1, nil)
	require.NoError(t, err)
	dag := New[uint64](sch, store, nil, *state)
	return dag
}
