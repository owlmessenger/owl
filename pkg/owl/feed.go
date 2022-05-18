package owl

import (
	"context"
	"errors"
	"log"
	"sync"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/jmoiron/sqlx"
	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/owlnet"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

type fcParams struct {
	db           *sqlx.DB
	log          *logrus.Logger
	getNode      func(context.Context, PeerID) (*owlnet.Node, error)
	handleUpdate func(tx *sqlx.Tx, feed *feeds.Feed, localID PeerID, s cadata.Store) error
}

type feedController struct {
	db        *sqlx.DB
	log       *logrus.Logger
	getNode   func(context.Context, PeerID) (*owlnet.Node, error)
	indexFunc func(tx *sqlx.Tx, feed *feeds.Feed, localID PeerID, s cadata.Store) error

	cf   context.CancelFunc
	eg   errgroup.Group
	mu   sync.Mutex
	subs map[chan struct{}][]feeds.NodeID
}

func newFeedController(params fcParams) *feedController {
	ctx, cf := context.WithCancel(context.Background())
	fc := &feedController{
		log:       params.log,
		db:        params.db,
		getNode:   params.getNode,
		indexFunc: params.handleUpdate,

		cf:   cf,
		subs: make(map[chan struct{}][]cadata.ID),
	}
	fc.eg.Go(func() error {
		return fc.run(ctx)
	})
	return fc
}

func (fc *feedController) run(ctx context.Context) error {
	return nil
}

func (fc *feedController) appendData(ctx context.Context, feedID feeds.NodeID, localID PeerID, data []byte) error {
	var feed *feeds.Feed
	if err := dbutil.DoTx(ctx, fc.db, func(tx *sqlx.Tx) error {
		var err error
		feed, err = loadFeed(tx, feedID)
		if err != nil {
			return err
		}
		storeID, err := lookupFeedStore(tx, feedID)
		if err != nil {
			return err
		}
		s := newTxStore(tx, storeID)
		if err := feed.AppendData(nil, s, localID, data); err != nil {
			return err
		}
		if err := fc.indexFunc(tx, feed, localID, s); err != nil {
			return err
		}
		return saveFeed(tx, feedID, feed)
	}); err != nil {
		return err
	}
	node, err := fc.getNode(ctx, localID)
	if err != nil {
		return err
	}
	client := node.FeedsClient()
	heads := feed.GetHeads(localID)
	for _, dst := range feed.Members() {
		if dst == localID {
			continue
		}
		log.Println("pushing heads", heads, dst, client)
		// if err := client.PushHeads(ctx, dst, heads); err != nil {
		// 	fc.log.Warn(err)
		// }
	}
	return nil
}

// func (fc *feedController) viewFeedTx(tx *sqlx.Tx, feedID [32]byte, as PeerID) ([]feeds.NodeID, cadata.Store, error) {
// 	return dbutil.Do1Tx(ctx, fc.db, func(tx *sqlx.Tx) (*feeds.Feed, error) {
// 		return loadFeed(tx, feedID)
// 	})
// 	store := newStore(fc.db, storeID)
// }

func (fc *feedController) handlePush(ctx context.Context, src, dst owlnet.PeerID, feedID owlnet.FeedID, heads []feeds.NodeID) error {
	return dbutil.DoTx(ctx, fc.db, func(tx *sqlx.Tx) error {
		feed, err := loadFeed(tx, feedID)
		if err != nil {
			return err
		}
		storeID, err := lookupFeedStore(tx, feedID)
		if err != nil {
			return err
		}
		store := newTxStore(tx, storeID)
		if err := feed.SetHeads(ctx, store, src, heads); err != nil {
			return err
		}
		if err := feed.AdoptHeads(ctx, store, src, dst); err != nil {
			return err
		}
		return saveFeed(tx, feedID, feed)
	})
}

func (fc *feedController) handleGet(ctx context.Context, src, dst owlnet.PeerID, feedID owlnet.FeedID) ([]feeds.NodeID, error) {
	return dbutil.Do1Tx(ctx, fc.db, func(tx *sqlx.Tx) ([]feeds.NodeID, error) {
		feed, err := loadFeed(tx, feedID)
		if err != nil {
			return nil, err
		}
		if !feed.CanRead(src) {
			return nil, errors.New("peer is not a member of feed")
		}
		return feed.GetHeads(dst), nil
	})
}

func (fc *feedController) listFeeds(ctx context.Context) (ret []owlnet.FeedID, err error) {
	err = fc.db.Select(&ret, `SELECT id FROM feeds`)
	return ret, err
}

func (fc *feedController) close() {
	fc.eg.Wait()
}

// Subscribe will close ch if the feed heads change from head
func (fc *feedController) Subscribe(feedID owlnet.FeedID, heads []feeds.NodeID, ch chan struct{}) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.subs[ch] = heads
}

func (fc *feedController) Unsubscribe(ch chan struct{}) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	delete(fc.subs, ch)
}

func (fc *feedController) notify(heads []feeds.NodeID) {
	for ch, heads2 := range fc.subs {
		if !slices.Equal(heads, heads2) {
			close(ch)
			delete(fc.subs, ch)
		}
	}
}

// createFeed creates a feed and the store for the feed
func createFeed(tx *sqlx.Tx, peers []feeds.PeerID) (*owlnet.FeedID, error) {
	storeID, err := createStore(tx)
	if err != nil {
		return nil, err
	}
	store := newTxStore(tx, storeID)
	f, err := feeds.NewEmpty(nil, store, peers)
	if err != nil {
		return nil, err
	}
	feedID := f.GetID()
	if _, err := tx.Exec(`INSERT INTO feeds (id, store_id, state) VALUES (?, ?, ?)`, feedID[:], storeID, f.Marshal()); err != nil {
		return nil, err
	}
	return &feedID, nil
}

// deleteFeed deletes from the feed table
// it also deletes the store for the feed.
func deleteFeed(tx *sqlx.Tx, feedID owlnet.FeedID) error {
	storeID, err := lookupFeedStore(tx, feedID)
	if err != nil {
		return err
	}
	if err := deleteStore(tx, storeID); err != nil {
		return err
	}
	_, err = tx.Exec(`DELETE FROM feeds WHERE id = ?`, feedID[:])
	return err
}

func lookupFeedStore(tx *sqlx.Tx, feedID [32]byte) (int, error) {
	var x int
	err := tx.Get(&x, `SELECT store_id FROM feeds WHERE id = ?`, feedID[:])
	return x, err
}

// loadFeed retrieves the feed state for feedID and returns it to the database.
func loadFeed(tx *sqlx.Tx, feedID [32]byte) (*feeds.Feed, error) {
	var state []byte
	if err := tx.Get(&state, `SELECT state FROM feeds WHERE id = ? `, feedID[:]); err != nil {
		return nil, err
	}
	return feeds.ParseFeed(state)
}

func saveFeed(tx *sqlx.Tx, feedID owlnet.FeedID, f *feeds.Feed) error {
	_, err := tx.Exec(`UPDATE feeds SET state = ? WHERE id = ?`, f.Marshal(), feedID[:])
	if err != nil {
		return err
	}
	return nil
}
