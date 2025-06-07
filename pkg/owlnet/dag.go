package owlnet

import (
	"context"
	"errors"

	"go.brendoncarroll.net/p2p"
	"go.inet256.org/inet256/pkg/inet256"
	"google.golang.org/grpc/codes"

	"github.com/owlmessenger/owl/pkg/owldag"
	"github.com/owlmessenger/owl/pkg/slices2"
)

type DAGReq struct {
	GetHeads  *GetHeadsReq  `json:"get_heads,omitempty"`
	PushHeads *PushHeadsReq `json:"push_heads,omitempty"`
	GetList   *GetListReq   `json:"list,omitempty"`
}

type DAGRes struct {
	Error *WireError `json:"error,omitempty"`

	GetHeads  []owldag.Head `json:"get_heads,omitempty"`
	PushHeads *struct{}     `json:"push_heads,omitempty"`
	GetList   []DAGInfo     `json:"get_list,omitempty"`
}

type GetHeadsReq struct {
	DAG Handle `json:"dag"`
}

type PushHeadsReq struct {
	SrcDAG Handle        `json:"src_dag"`
	DstDAG Handle        `json:"dst_dag"`
	Heads  []owldag.Head `json:"heads"`
	Blobs  []byte        `json:"blobs"`
}

type GetListReq struct {
	// Scheme, if set, restricts the results returned to those which match the scheme
	Scheme string `json:"scheme"`
	// Contains when len(_) > 0 restricts the results to those which contain all the Refs.
	Contains []owldag.Ref `json:"contains"`
}

type DAGInfo struct {
	Scheme string       `json:"scheme"`
	Epochs []owldag.Ref `json:"epochs"`
	Handle Handle       `json:"handle"`
	Stores []Handle     `json:"stores"`
}

type LocalDAGInfo struct {
	ID     int
	Scheme string
	Epochs []owldag.Ref
}

type DAGClient struct {
	swarm     p2p.SecureAskSwarm[PeerID, inet256.PublicKey]
	newHandle func(PeerID, uint32) Handle
}

func (dc *DAGClient) GetHeads(ctx context.Context, dst PeerID, h Handle) ([]owldag.Head, error) {
	res, err := dc.ask(ctx, dst, DAGReq{
		GetHeads: &GetHeadsReq{
			DAG: h,
		},
	})
	if err != nil {
		return nil, err
	}
	return res.GetHeads, nil
}

func (dc *DAGClient) PushHeads(ctx context.Context, dst PeerID, srcDAG uint32, h Handle, heads []owldag.Head) error {
	res, err := dc.ask(ctx, dst, DAGReq{
		PushHeads: &PushHeadsReq{
			DstDAG: h,
			SrcDAG: dc.newHandle(dst, srcDAG),
			Heads:  heads,
		},
	})
	if err != nil {
		return err
	}
	if res.PushHeads == nil {
		return errors.New("owlnet.DAGClient.PushHeads: empty response")
	}
	return nil
}

func (dc *DAGClient) List(ctx context.Context, dst PeerID, scheme string, refs []owldag.Ref) ([]DAGInfo, error) {
	res, err := dc.ask(ctx, dst, DAGReq{
		GetList: &GetListReq{
			Scheme:   scheme,
			Contains: refs,
		},
	})
	if err != nil {
		return nil, err
	}
	return res.GetList, nil
}

func (dc *DAGClient) Get(ctx context.Context, dst PeerID, scheme string, refs []owldag.Ref) (*DAGInfo, error) {
	infos, err := dc.List(ctx, dst, scheme, refs)
	if err != nil {
		return nil, err
	}
	if len(infos) < 1 {
		return nil, errors.New("no infos found")
	}
	if len(infos) > 1 {
		return nil, errors.New("ambiguous DAG criteria")
	}
	info := infos[0]
	return &info, nil
}

func (dc *DAGClient) ask(ctx context.Context, dst PeerID, req DAGReq) (*DAGRes, error) {
	var res DAGRes
	if err := askJSON(ctx, dc.swarm, dst, req, &res); err != nil {
		return nil, err
	}
	if res.Error != nil {
		return nil, res.Error
	}
	return &res, nil
}

type DAGServer struct {
	OnPushHeads func(from PeerID, volID int, srcDAG Handle, heads []owldag.Head) error
	OnGetHeads  func(from PeerID, volID int) ([]owldag.Head, error)
	OnList      func(from PeerID, schemeIs string, mustContain []owldag.Ref) ([]LocalDAGInfo, error)

	openHandle func(PeerID, Handle) (uint32, error)
	newHandle  func(PeerID, uint32) Handle
}

func (s DAGServer) HandleAsk(ctx context.Context, resp []byte, msg p2p.Message[PeerID]) int {
	return serveJSON(ctx, resp, msg, func(req DAGReq) (*DAGRes, error) {
		switch {
		case req.GetHeads != nil:
			volID, err := s.openHandle(msg.Src, req.GetHeads.DAG)
			if err != nil {
				return nil, err
			}
			heads, err := s.OnGetHeads(msg.Src, int(volID))
			if err != nil {
				return nil, err
			}
			return &DAGRes{GetHeads: heads}, nil
		case req.PushHeads != nil:
		case req.GetList != nil:
			localInfos, err := s.OnList(msg.Src, req.GetList.Scheme, req.GetList.Contains)
			if err != nil {
				return nil, err
			}
			dagInfos := slices2.Map(localInfos, func(x LocalDAGInfo) DAGInfo {
				return DAGInfo{
					Handle: s.newHandle(msg.Src, uint32(x.ID)),
					Epochs: x.Epochs,
				}
			})
			return &DAGRes{GetList: dagInfos}, nil
		default:
			return nil, errors.New("empty DAG request")
		}
		return &DAGRes{
			Error: &WireError{Code: codes.Unimplemented, Msg: "Unimplemented"},
		}, nil
	})
}
