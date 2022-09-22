package owlnet

import (
	"context"
	"errors"

	"github.com/brendoncarroll/go-p2p"
	"google.golang.org/grpc/codes"

	"github.com/owlmessenger/owl/pkg/owldag"
)

type DAGReq struct {
	GetHeads  *GetHeadsReq  `json:"get_heads,omitempty"`
	PushHeads *PushHeadsReq `json:"push_heads,omitempty"`
}

type DAGRes struct {
	Error *WireError

	GetHeads  []owldag.Head
	PushHeads *struct{}
}

type GetHeadsReq struct {
	Epoch owldag.Ref `json:"epoch"`
}

type PushHeadsReq struct {
	Epoch owldag.Ref    `json:"epoch"`
	Heads []owldag.Head `json:"heads"`
	Blobs []byte        `json:"blobs"`
}

type ListDAGsReq struct{}

type DAGClient struct {
	swarm p2p.SecureAskSwarm[PeerID]
}

func (fc DAGClient) GetHeads(ctx context.Context, dst PeerID, id owldag.Ref) ([]owldag.Head, error) {
	res, err := fc.ask(ctx, dst, DAGReq{
		GetHeads: &GetHeadsReq{
			Epoch: id,
		},
	})
	if err != nil {
		return nil, err
	}
	return res.GetHeads, nil
}

func (fc DAGClient) PushHeads(ctx context.Context, dst PeerID, fid owldag.Ref, heads []owldag.Head) error {
	res, err := fc.ask(ctx, dst, DAGReq{
		PushHeads: &PushHeadsReq{
			Epoch: fid,
			Heads: heads,
		},
	})
	if err != nil {
		return err
	}
	if res.PushHeads == nil {
		return errors.New("owlnet.DAGsClient: empty response")
	}
	return nil
}

func (fc DAGClient) ask(ctx context.Context, dst PeerID, req DAGReq) (*DAGRes, error) {
	var res DAGRes
	if err := askJSON(ctx, fc.swarm, dst, req, &res); err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, res.Error
	}
	return &res, nil
}

type DAGServer struct {
	OnPush func(from PeerID, dagID [32]byte, heads []owldag.Head) error
	OnGet  func(from PeerID, dagID [32]byte) ([]owldag.Head, error)
}

func (s DAGServer) HandleAsk(ctx context.Context, resp []byte, msg p2p.Message[PeerID]) int {
	return serveJSON(ctx, resp, msg, func(req DAGReq) (*DAGRes, error) {
		return &DAGRes{
			Error: &WireError{Code: codes.Unimplemented, Msg: "Unimplemented"},
		}, nil
	})
}
