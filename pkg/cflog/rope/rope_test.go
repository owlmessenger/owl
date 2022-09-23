package rope

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	defaultMeanSize = 1 << 12
	defaultMaxSize  = 1 << 16
)

func TestBuilder(t *testing.T) {
	s := newStore(t)
	b := NewBuilder(s, defaultMeanSize, defaultMaxSize, nil)

	const N = 10000
	var v []byte
	for i := 0; i < N; i++ {
		v = fmt.Appendf(v[:0], "hello world %d", i)
		require.NoError(t, b.Append(ctx, 0, v))
	}

	root, err := b.Finish(ctx)
	require.NoError(t, err)
	require.NotNil(t, root)
}
