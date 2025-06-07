package memberset

import (
	"context"
	"testing"

	"github.com/gotvc/got/pkg/gotkv"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
)

var ctx = context.Background()

func TestNew(t *testing.T) {
	kvop := gotkv.NewAgent(1<<12, 1<<16)
	s := cadata.NewMem(cadata.DefaultHash, cadata.DefaultMaxSize)
	o := New(&kvop)
	x, err := o.NewEmpty(ctx, s, nil)
	require.NoError(t, err)
	require.NotNil(t, x)
}
