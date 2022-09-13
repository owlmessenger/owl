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
	p := testProtocol{}
	f, err := NewInit[struct{}](ctx, p, s, struct{}{})
	require.NoError(t, err)
	require.NotNil(t, f)
}

func newPeerID(x int) (ret PeerID) {
	binary.BigEndian.PutUint64(ret[:], uint64(x))
	return ret
}

type testProtocol struct {
}

func (p testProtocol) Merge(ctx context.Context, xs []struct{}) (struct{}, error) {
	return struct{}{}, nil
}

func (p testProtocol) Validate(ctx context.Context, author PeerID, prev, next struct{}) error {
	return nil
}

func (p testProtocol) CanRead(ctx context.Context, xs struct{}, peer PeerID) (bool, error) {
	return true, nil
}

func (p testProtocol) ListPeers(ctx context.Context, x struct{}) ([]PeerID, error) {
	return nil, nil
}
