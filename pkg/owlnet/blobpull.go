package owlnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/owlmessenger/owl/pkg/owldag"
)

type BlobPullReq struct {
	DAG owldag.Ref `json:"dag"`
	ID  cadata.ID  `json:"id"`
}

type BlobPullServer struct {
	Open func(peerID PeerID, epoch owldag.Ref) cadata.Getter
}

func (s *BlobPullServer) HandleAsk(ctx context.Context, resp []byte, msg p2p.Message[inet256.ID]) int {
	var req BlobPullReq
	if err := json.Unmarshal(resp, &req); err != nil {
		return -1
	}
	id := req.ID
	store := s.Open(msg.Src, req.DAG)
	if store == nil {
		return -1
	}
	n, err := store.Get(ctx, id, resp)
	if errors.Is(err, cadata.ErrNotFound) {
		return copy(resp, id[:])
	}
	if err != nil {
		return copy(resp, []byte(err.Error()))
	}
	return n
}

type BlobPullClient struct {
	swarm p2p.SecureAskSwarm[PeerID]
}

func (c BlobPullClient) Pull(ctx context.Context, dst PeerID, id cadata.ID, buf []byte) (int, error) {
	n, err := c.swarm.Ask(ctx, buf, dst, p2p.IOVec{id[:]})
	if err != nil {
		return 0, err
	}
	if bytes.Equal(buf, id[:]) {
		return 0, cadata.ErrNotFound
	}
	actual := owldag.Hash(buf[:n])
	if actual != id {
		return 0, cadata.ErrBadData
	}
	return n, nil
}
