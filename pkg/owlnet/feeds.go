package owlnet

import (
	"context"
	"encoding/json"

	"github.com/brendoncarroll/go-p2p"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/sirupsen/logrus"
)

type FeedID = feeds.NodeID

type FeedReq struct {
	GetHeads  *GetHeadsReq  `json:"get_heads,omitempty"`
	PushHeads *PushHeadsReq `json:"push_heads,omitempty"`
}

type GetHeadsReq struct {
	FeedID FeedID `json:"feed_id"`
}

type PushHeadsReq struct {
	FeedID FeedID         `json:"feed_id"`
	Heads  []feeds.NodeID `json:"heads"`
	Blobs  []byte         `json:"blobs"`
}

type FeedRes struct {
	Error *WireError

	GetHeads  []feeds.NodeID
	PushHeads *struct{}
}

type FeedsClient struct {
	swarm p2p.SecureAskSwarm[PeerID]
}

func (fc FeedsClient) GetHeads(ctx context.Context, dst PeerID) ([]feeds.NodeID, error) {
	res, err := fc.ask(ctx, dst, FeedReq{})
	if err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, res.Error
	}
	return res.GetHeads, nil
}

func (fc FeedsClient) PushHeads(ctx context.Context, dst PeerID, heads []feeds.NodeID) error {
	res, err := fc.ask(ctx, dst, FeedReq{})
	if err != nil {
		return err
	}
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (fc FeedsClient) ask(ctx context.Context, dst PeerID, req FeedReq) (*FeedRes, error) {
	var res FeedRes
	if err := askJSON(ctx, fc.swarm, dst, req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

type FeedsServer struct {
	Logger *logrus.Logger
	OnPush func(from PeerID, feedID [32]byte, heads []feeds.NodeID) error
	OnGet  func(from PeerID, feedID [32]byte) ([]feeds.NodeID, error)
}

func (fs FeedsServer) HandleAsk(ctx context.Context, resp []byte, msg p2p.Message[PeerID]) int {
	var req FeedReq
	if err := json.Unmarshal(resp, &req); err != nil {
		return -1
	}
	return 0
}
