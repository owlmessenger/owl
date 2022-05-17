package owlnet

import (
	"context"

	"github.com/brendoncarroll/go-p2p"
)

type PersonaReq struct {
}

type PersonaRes struct {
	Error *WireError

	GetFeed *FeedID
}

type PersonaClient struct {
	swarm Swarm
}

// ListPeers returns other peers, which are part of the same persona
func (c PersonaClient) GetFeed(ctx context.Context, dst PeerID) (*FeedID, error) {
	res, err := c.ask(ctx, dst, &PersonaReq{})
	if err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, res.Error
	}
	return res.GetFeed, nil
}

func (c PersonaClient) ask(ctx context.Context, dst PeerID, req *PersonaReq) (*PersonaRes, error) {
	return nil, nil
}

type PersonaServer struct {
	GetFeed func(ctx context.Context, src PeerID) (*FeedID, error)
}

func (s PersonaServer) HandleAsk(ctx context.Context, resp []byte, msg p2p.Message[PeerID]) int {
	return 0
}
