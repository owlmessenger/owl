package feeds

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/stretchr/testify/require"
)

func TestNewEmpty(t *testing.T) {
	ctx := context.Background()
	s := cadata.NewMem(Hash, 1<<16)
	f, err := NewEmpty(ctx, s, []PeerID{newPeerID(0), newPeerID(1)})
	require.NoError(t, err)
	require.NotNil(t, f)
}

func newPeerID(x int) (ret PeerID) {
	binary.BigEndian.PutUint64(ret[:], uint64(x))
	return ret
}
