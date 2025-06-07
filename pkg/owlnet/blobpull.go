package owlnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/owlmessenger/owl/pkg/owldag"
	"go.brendoncarroll.net/p2p"
	"go.brendoncarroll.net/state/cadata"
	"go.inet256.org/inet256/pkg/inet256"
)

type BlobPullReq struct {
	Store Handle    `json:"store"`
	ID    cadata.ID `json:"id"`
}

type BlobPullServer struct {
	Open func(peerID PeerID, volID uint32, sid uint8) cadata.Getter

	openHandle func(PeerID, Handle) (uint32, uint8, error)
}

func (s *BlobPullServer) HandleAsk(ctx context.Context, resp []byte, msg p2p.Message[inet256.ID]) int {
	var req BlobPullReq
	if err := json.Unmarshal(resp, &req); err != nil {
		return -1
	}
	volID, storeID, err := s.openHandle(msg.Src, req.Store)
	if err != nil {
		return -1
	}
	id := req.ID
	store := s.Open(msg.Src, volID, storeID)
	if store == nil {
		return -1
	}
	n, err := store.Get(ctx, id, resp)
	if errors.Is(err, cadata.ErrNotFound{Key: id}) {
		return copy(resp, id[:])
	}
	if err != nil {
		return copy(resp, []byte(err.Error()))
	}
	return n
}

type BlobPullClient struct {
	swarm p2p.SecureAskSwarm[PeerID, inet256.PublicKey]
}

func (c BlobPullClient) Pull(ctx context.Context, dst PeerID, h Handle, id cadata.ID, buf []byte) (int, error) {
	reqData, err := json.Marshal(BlobPullReq{
		Store: h,
		ID:    id,
	})
	if err != nil {
		panic(err)
	}
	n, err := c.swarm.Ask(ctx, buf, dst, p2p.IOVec{reqData})
	if err != nil {
		return 0, err
	}
	if bytes.Equal(buf, id[:]) {
		return 0, cadata.ErrNotFound{Key: id}
	}
	actual := owldag.Hash(buf[:n])
	if actual != id {
		return 0, cadata.ErrBadData
	}
	return n, nil
}
