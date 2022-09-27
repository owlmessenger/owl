package owlnet

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/owlmessenger/owl/pkg/owldag"
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

func (s *Store) Hash(x []byte) cadata.ID {
	return owldag.Hash(x)
}

func (s *Store) MaxSize() int {
	return owldag.MaxNodeSize
}
