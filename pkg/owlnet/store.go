package owlnet

import (
	"context"

	"github.com/owlmessenger/owl/pkg/owldag"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/cadata"
)

var (
	_ cadata.Getter = &Store{}
)

type Store struct {
	c    BlobPullClient
	addr PeerID
	h    Handle
}

func NewStore(c BlobPullClient, addr PeerID, h Handle) *Store {
	return &Store{c: c, addr: addr}
}

func (s *Store) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	return s.c.Pull(ctx, s.addr, s.h, id, buf)
}

func (s *Store) Exists(ctx context.Context, id cadata.ID) (bool, error) {
	buf := make([]byte, s.MaxSize())
	_, err := s.Get(ctx, id, buf)
	if err != nil {
		if state.IsErrNotFound[cadata.ID](err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Store) Hash(x []byte) cadata.ID {
	return owldag.Hash(x)
}

func (s *Store) MaxSize() int {
	return owldag.MaxNodeSize
}
