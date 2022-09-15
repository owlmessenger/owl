package memberset

import (
	"context"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestNew(t *testing.T) {
	kvop := gotkv.NewOperator(1<<12, 1<<16)
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	o := New(&kvop)
	x, err := o.NewEmpty(ctx, s, nil)
	require.NoError(t, err)
	require.NotNil(t, x)
}
