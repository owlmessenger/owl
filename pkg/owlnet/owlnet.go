package owlnet

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/p/p2pmux"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/owlmessenger/owl/pkg/owldag"
	"google.golang.org/grpc/codes"
)

type (
	PeerID = owldag.PeerID
	Swarm  = p2p.SecureAskSwarm[PeerID, inet256.PublicKey]
)

const (
	channelBlobPull = "owl/blobpull-v0"
	channelDAG      = "owl/dag-v0"
)

type Node struct {
	swarm Swarm

	secret        *[32]byte
	mux           p2pmux.SecureAskMux[PeerID, string, inet256.PublicKey]
	blobPullSwarm p2p.SecureAskSwarm[PeerID, inet256.PublicKey]
	dagSwarm      p2p.SecureAskSwarm[PeerID, inet256.PublicKey]
}

func New(swarm Swarm) *Node {
	mux := p2pmux.NewStringSecureAskMux(swarm)
	return &Node{
		swarm:         swarm,
		mux:           mux,
		blobPullSwarm: mux.Open(channelBlobPull),
		dagSwarm:      mux.Open(channelDAG),
		secret:        generateSecret(),
	}
}

func (n *Node) BlobPullServer(ctx context.Context, srv *BlobPullServer) error {
	srv.openHandle = func(peerID PeerID, h Handle) (uint32, uint8, error) {
		data, err := OpenHandle(n.secret, peerID, h)
		if err != nil {
			return 0, 0, err
		}
		return binary.BigEndian.Uint32(data[:4]), data[4], nil
	}
	return ServeAsks(ctx, n.blobPullSwarm, srv)
}

func (n *Node) BlobPullClient() BlobPullClient {
	return BlobPullClient{swarm: n.blobPullSwarm}
}

func (n *Node) DAGServer(ctx context.Context, srv *DAGServer) error {
	srv.openHandle = func(peerID PeerID, h Handle) (uint32, error) {
		data, err := OpenHandle(n.secret, peerID, h)
		if err != nil {
			return 0, err
		}
		return binary.BigEndian.Uint32(data[:]), nil
	}
	srv.newHandle = func(peerID PeerID, x uint32) Handle {
		var data [16]byte
		binary.BigEndian.PutUint32(data[:], x)
		return NewHandle(n.secret, peerID, &data)
	}
	return ServeAsks(ctx, n.dagSwarm, srv)
}

func (n *Node) DAGClient() *DAGClient {
	return &DAGClient{
		swarm: n.dagSwarm,
		newHandle: func(peerID PeerID, x uint32) Handle {
			var data [16]byte
			binary.BigEndian.PutUint32(data[:], x)
			return NewHandle(n.secret, peerID, &data)
		},
	}
}

func (n *Node) Close() error {
	return n.swarm.Close()
}

type AskHandler interface {
	HandleAsk(ctx context.Context, resp []byte, req p2p.Message[inet256.ID]) int
}

func ServeAsks(ctx context.Context, asker p2p.AskServer[inet256.ID], h AskHandler) error {
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

func serveJSON[In, Out any](ctx context.Context, resp []byte, req p2p.Message[inet256.ID], fn func(In) (*Out, error)) int {
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
}

type WireError struct {
	Code codes.Code
	Msg  string
}

func (we WireError) Error() string {
	return fmt.Sprintf("{code: %v, msg: %v}", we.Code, we.Msg)
}

func NewError(c codes.Code, msg string) *WireError {
	return &WireError{Code: c, Msg: msg}
}

func generateSecret() *[32]byte {
	out := new([32]byte)
	if _, err := io.ReadFull(rand.Reader, out[:]); err != nil {
		panic(err)
	}
	return out
}
