package owl

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/brendoncarroll/go-p2p/p/mbapp"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/inet256/inet256/pkg/p2padapter"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/owlnet"
)

var _ API = &Server{}

type Server struct {
	db      *sqlx.DB
	inet256 inet256.Service
	log     *logrus.Logger

	initOnce       sync.Once
	feedController *feedController
	mu             sync.Mutex
	nodes          map[inet256.ID]*owlnet.Node

	eg errgroup.Group
}

func NewServer(db *sqlx.DB, inet256srv inet256.Service) *Server {
	log := logrus.StandardLogger()
	s := &Server{
		db:      db,
		inet256: inet256srv,
		log:     log,

		nodes: make(map[inet256.ID]*owlnet.Node),
	}
	s.feedController = newFeedController(fcParams{
		log:          log,
		db:           db,
		getNode:      s.getNode,
		handleUpdate: s.handleFeed,
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

func (s *Server) readLoop(ctx context.Context, n *owlnet.Node) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return n.BlobPullServer(ctx, &owlnet.BlobPullServer{
			Open: func(peerID PeerID, feedID owlnet.FeedID) cadata.Getter {
				storeID, feed, err := s.getFeed(ctx, feedID)
				if err != nil {
					s.log.Error(err)
					return nil
				}
				if !feed.CanRead(peerID) {
					return nil
				}
				return newStore(s.db, storeID)
			},
		})
	})
	eg.Go(func() error {
		return n.FeedsServer(ctx, &owlnet.FeedsServer{
			Logger: s.log,
		})
	})
	return eg.Wait()
}

func (s *Server) getFeed(ctx context.Context, feedID [32]byte) (int, *feeds.Feed, error) {
	return dbutil.Do2Tx(ctx, s.db, func(tx *sqlx.Tx) (int, *feeds.Feed, error) {
		storeID, err := lookupFeedStore(tx, feedID)
		if err != nil {
			return 0, nil, err
		}
		f, err := loadFeed(tx, feedID)
		if err != nil {
			return 0, nil, err
		}
		return storeID, f, nil
	})
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
	node2 := owlnet.New(mbapp.New(p2padapter.P2PSwarmFromNode(node), 1<<16))
	s.nodes[id] = node2
	s.eg.Go(func() error {
		defer func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.nodes, id)
		}()
		return s.readLoop(ctx, node2)
	})
	return s.nodes[id], nil
}

func (s *Server) handleFeed(tx *sqlx.Tx, feed *feeds.Feed, peerID PeerID, store cadata.Store) error {
	// TODO: check that we have the peerID
	var chanInt int
	if err := tx.Get(&chanInt, `SELECT id FROM channels WHERE feed_id = ?`, feed.ID[:]); !errors.Is(err, sql.ErrNoRows) {
		if err != nil {
			return err
		}
		heads := feed.GetHeads(peerID)
		if len(heads) != 1 {
			return nil
		}
		head := heads[0]
		node, err := feeds.GetNode(nil, store, head)
		if err != nil {
			return err
		}
		return indexChannelNode(tx, chanInt, head, *node)
	}
	var x int
	if err := tx.Get(&x, `SELECT 1 FROM persona_contacts WHERE feed_id = ?`, feed.ID[:]); !errors.Is(err, sql.ErrNoRows) {
		if err != nil {
			return err
		}
		// TODO: do something with the feed, update persona_contacts.last_peer to peerID
		return nil
	}
	return fmt.Errorf("unknown feed %v", feed.ID)
}
