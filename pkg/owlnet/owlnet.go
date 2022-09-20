package owlnet

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/p2pmux"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/owlmessenger/owl/pkg/owldag"
	"google.golang.org/grpc/codes"
)

type (
	PeerID = owldag.PeerID
	Swarm  = p2p.SecureAskSwarm[PeerID]
)

const (
	channelBlobPull = "owl/blobpull-v0"
	channelFeeds    = "owl/feeds-v0"
)

type Node struct {
	swarm Swarm
	mux   p2pmux.SecureAskMux[PeerID, string]

	blobPullSwarm p2p.SecureAskSwarm[PeerID]
	feedsSwarm    p2p.SecureAskSwarm[PeerID]
}

func New(swarm Swarm) *Node {
	mux := p2pmux.NewStringSecureAskMux(swarm)
	return &Node{
		swarm:         swarm,
		mux:           mux,
		blobPullSwarm: mux.Open(channelBlobPull),
		feedsSwarm:    mux.Open(channelFeeds),
	}
}

func (n *Node) BlobPullServer(ctx context.Context, srv *BlobPullServer) error {
	return ServeAsks(ctx, n.blobPullSwarm, srv)
}

func (n *Node) BlobPullClient() BlobPullClient {
	return BlobPullClient{swarm: n.blobPullSwarm}
}

func (n *Node) FeedsServer(ctx context.Context, srv *FeedsServer) error {
	return ServeAsks(ctx, n.feedsSwarm, srv)
}

func (n *Node) FeedsClient() FeedsClient {
	return FeedsClient{n.feedsSwarm}
}

func (n *Node) Close() error {
	return n.swarm.Close()
}

type AskHandler interface {
	HandleAsk(ctx context.Context, resp []byte, req p2p.Message[inet256.ID]) int
}

func ServeAsks(ctx context.Context, asker p2p.Asker[inet256.ID], h AskHandler) error {
	for {
		if err := asker.ServeAsk(ctx, h.HandleAsk); err != nil {
			return err
		}
	}
}

func askJSON(ctx context.Context, asker p2p.Asker[inet256.ID], dst inet256.ID, req, res interface{}) error {
	respData := make([]byte, 1<<16)
	reqData, _ := json.Marshal(req)
	n, err := asker.Ask(ctx, respData, dst, p2p.IOVec{reqData})
	if err != nil {
		return err
	}
	return json.Unmarshal(respData[:n], res)
}

func serveJSON[In, Out any](ctx context.Context, asker p2p.Asker[inet256.ID], fn func(In) (*Out, error)) error {
	return asker.ServeAsk(ctx, func(ctx context.Context, resp []byte, req p2p.Message[inet256.Addr]) int {
		var in In
		if err := json.Unmarshal(req.Payload, &in); err != nil {
			return -1
		}
		out, err := fn(in)
		if err != nil {
			return -1
		}
		data, err := json.Marshal(out)
		if err != nil {
			return -1
		}
		return copy(resp, data)
	})
}

type WireError struct {
	Code codes.Code
	Msg  string
}

func (we WireError) Error() string {
	return fmt.Sprintf("{code: %v, msg: %v}", we.Code, we.Msg)
}
