package owl

import (
	"context"
	"io"
	"sync"

	"github.com/brendoncarroll/go-p2p/p/mbapp"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/inet256/inet256/pkg/p2padapter"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/sha3"
	"golang.org/x/sync/errgroup"

	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/owlnet"
	"github.com/owlmessenger/owl/pkg/p/channel"
	"github.com/owlmessenger/owl/pkg/p/contactset"
	"github.com/owlmessenger/owl/pkg/p/directory"
)

var _ API = &Server{}

type Server struct {
	db      *sqlx.DB
	inet256 inet256.Service

	initOnce sync.Once

	contactSetCtrl *feedController[contactset.State]
	directoryCtrl  *feedController[directory.State]
	channelCtrl    *feedController[channel.State]

	mu    sync.Mutex
	nodes map[inet256.ID]*owlnet.Node

	eg errgroup.Group
}

func NewServer(db *sqlx.DB, inet256srv inet256.Service) *Server {
	ctx := logctx.WithFmtLogger(context.Background(), logrus.StandardLogger())
	s := &Server{
		db:      db,
		inet256: inet256srv,

		nodes: make(map[inet256.ID]*owlnet.Node),
	}
	s.contactSetCtrl = newFeedController(fcParams[contactset.State]{
		Context:      ctx,
		DB:           db,
		GetNode:      s.getNode,
		ProtocolName: "contactset@v0",
		Protocol:     func(store cadata.Store) feeds.Protocol[contactset.State] {
			s.lis
			return contactset.NewProtocol(store, ),
		},
	})
	s.directoryCtrl = newFeedController(fcParams[directory.State]{
		Context:      ctx,
		DB:           db,
		GetNode:      s.getNode,
		ProtocolName: "directory@v0",
	})
	s.channelCtrl = newFeedController(fcParams[channel.State]{
		Context:      ctx,
		DB:           db,
		GetNode:      s.getNode,
		ProtocolName: "channel@v0",
	})

	return s
}

func (s *Server) Init(ctx context.Context) (err error) {
	s.initOnce.Do(func() {
		err = func() error {
			if err := setupDB(ctx, s.db); err != nil {
				return err
			}
			return nil
		}()
	})
	return err
}

func (s *Server) Close() error {
	s.mu.Lock()
	for _, node := range s.nodes {
		node.Close()
	}
	s.mu.Unlock()

	s.eg.Wait()
	s.db.Close()
	return nil
}

func (s *Server) readLoop(ctx context.Context, localID PeerID, n *owlnet.Node) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return n.BlobPullServer(ctx, &owlnet.BlobPullServer{
			Open: func(peerID PeerID, rootID feeds.ID) cadata.Getter {
				feedID, err := lookupFeed(s.db, localID, rootID)
				if err != nil {
					logctx.Errorf(ctx, "%v", err)
					return nil
				}
				allow, err := s.channelCtrl.CanRead(ctx, feedID)
				if err != nil {
					logctx.Errorf(ctx, "opening store: %v", err)
					return nil
				}
				if !allow {
					return nil
				}
				storeID, err := lookupFeedStore(s.db, feedID)
				if err != nil {
					logctx.Errorf(ctx, "%v", err)
					return nil
				}
				return newStore(s.db, storeID)
			},
		})
	})
	eg.Go(func() error {
		return n.FeedsServer(ctx, &owlnet.FeedsServer{})
	})
	return eg.Wait()
}

func (s *Server) getNode(ctx context.Context, id PeerID) (*owlnet.Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if node, exists := s.nodes[id]; exists {
		return node, nil
	}
	privateKey, err := s.getPrivateKey(s.db, id)
	if err != nil {
		return nil, err
	}
	node, err := s.inet256.Open(ctx, privateKey)
	if err != nil {
		return nil, err
	}
	node2 := owlnet.New(mbapp.New(p2padapter.SwarmFromNode(node), 1<<16))
	s.nodes[id] = node2
	s.eg.Go(func() error {
		defer func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.nodes, id)
		}()
		return s.readLoop(ctx, node.LocalAddr(), node2)
	})
	return s.nodes[id], nil
}

func deriveSeed(seed *[32]byte, other string) *[32]byte {
	var ret [32]byte
	h := sha3.NewShake256()
	h.Write(seed[:])
	h.Write([]byte(other))
	if _, err := io.ReadFull(h, ret[:]); err != nil {
		panic(err)
	}
	return &ret
}
