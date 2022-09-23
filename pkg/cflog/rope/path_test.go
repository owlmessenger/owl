package rope

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPathAdd(t *testing.T) {
	tcs := []struct {
		A, B, Out Path
	}{
		{A: Path{0}, B: Path{1}, Out: Path{1}},
		{A: Path{3}, B: Path{4}, Out: Path{7}},
	}
	for _, tc := range tcs {
		out := PathAdd(tc.A, tc.B)
		require.Equal(t, tc.Out, out)
	}
}
