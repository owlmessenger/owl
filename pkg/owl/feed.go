package owl

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/jmoiron/sqlx"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/owlnet"
)

type fcParams[T any] struct {
	Context     context.Context
	DB          *sqlx.DB
	GetNode     func(context.Context, PeerID) (*owlnet.Node, error)
	GetProtocol func(ctx context.Context, persona string) (feeds.Protocol[T], error)

	ProtocolName string
	NewProtocol  func(s cadata.Store) feeds.Protocol[T]
}

type feedController[T any] struct {
	db           *sqlx.DB
	getNode      func(context.Context, PeerID) (*owlnet.Node, error)
	protocolName string
	getProtocol  func(ctx context.Context, persona string) (feeds.Protocol[T], error)

	cf   context.CancelFunc
	eg   errgroup.Group
	mu   sync.Mutex
	subs map[chan struct{}][]feeds.ID
}

func newFeedController[T any](params fcParams[T]) *feedController[T] {
	ctx, cf := context.WithCancel(params.Context)
	fc := &feedController[T]{
		db:           params.DB,
		getNode:      params.GetNode,
		protocolName: params.ProtocolName,
		getProtocol:  params.GetProtocol,

		cf:   cf,
		subs: make(map[chan struct{}][]cadata.ID),
	}
	fc.eg.Go(func() error {
		return fc.run(ctx)
	})
	return fc
}

func (fc *feedController[T]) run(ctx context.Context) error {
	return nil
}

func (fc *feedController[T]) Create(ctx context.Context, fn func(s cadata.Store) (*T, error)) (int, error) {
	seed := new([32]byte)
	if _, err := io.ReadFull(rand.Reader, seed[:]); err != nil {
		return 0, err
	}
	return dbutil.DoTx1(ctx, fc.db, func(tx *sqlx.Tx) (int, error) {
		storeID, err := createStore(tx)
		if err != nil {
			return 0, err
		}
		s := newTxStore(tx, storeID)
		state, err := fn(s)
		if err != nil {
			return 0, err
		}
		protocol, err := fc.getProtocol(ctx)
		feed, err := feeds.NewInit(ctx, fc.newProtocol(s), s, *state)
		if err != nil {
			return 0, err
		}
		rootID := feed.GetRoot()
		fstate := feed.GetState()
		return createFeed(tx, fc.protocolName, rootID, fstate, storeID)
	})
}

func (fc *feedController[T]) Delete(ctx context.Context, feedID int) error {
	return dbutil.DoTx(ctx, fc.db, func(tx *sqlx.Tx) error {
		return deleteFeed(tx, feedID)
	})
}

func (fc *feedController[T]) Join(ctx context.Context, root feeds.ID) (int, error) {
	return 0, nil
}

func (fc *feedController[T]) Modify(ctx context.Context, feedID int, author PeerID, fn func(feed *feeds.Feed[T], s cadata.Store) error) error {
	if err := dbutil.DoTx(ctx, fc.db, func(tx *sqlx.Tx) error {
		state, err := loadFeed[T](tx, feedID)
		if err != nil {
			return err
		}
		storeID, err := lookupFeedStore(tx, feedID)
		if err != nil {
			return err
		}
		store := newTxStore(tx, storeID)
		feed := feeds.New(fc.newProtocol(store), *state, store)
		if err := fn(feed, store); err != nil {
			return err
		}
		return saveFeed(tx, feedID, feed.GetState())
	}); err != nil {
		return err
	}
	return fc.Broadcast(ctx, author, feedID)
}

func (fc *feedController[T]) View(ctx context.Context, feedID int) (*T, cadata.Store, error) {
	return dbutil.DoTx2(ctx, fc.db, func(tx *sqlx.Tx) (*T, cadata.Store, error) {
		fstate, err := loadFeed[T](tx, feedID)
		if err != nil {
			return nil, nil, err
		}
		sid, err := lookupFeedStore(tx, feedID)
		if err != nil {
			return nil, nil, err
		}
		return &fstate.State, newStore(fc.db, sid), nil
	})
}

func (fc *feedController[T]) Broadcast(ctx context.Context, sender PeerID, feedID int) error {
	// node, err := fc.getNode(ctx, localID)
	// if err != nil {
	// 	return err
	// }
	// client := node.FeedsClient()
	// peers, err := fc.protocol.ListPeers()
	// for _, dst := range fc.protocol.ListPeers {
	// 	if dst == localID {
	// 		continue
	// 	}
	// 	log.Println("pushing heads", heads, dst, client)
	// 	// if err := client.PushHeads(ctx, dst, heads); err != nil {
	// 	// 	fc.log.Warn(err)
	// 	// }
	// }
	return nil
}

func (fc *feedController[T]) CanRead(ctx context.Context, feedID int) (bool, error) {
	return false, nil
}

func (fc *feedController[T]) handlePush(ctx context.Context, src, dst owlnet.PeerID, feedID int, heads []feeds.Ref) error {
	return dbutil.DoTx(ctx, fc.db, func(tx *sqlx.Tx) error {
		feed, err := loadFeed[T](tx, feedID)
		if err != nil {
			return err
		}
		storeID, err := lookupFeedStore(tx, feedID)
		if err != nil {
			return err
		}
		store := newTxStore(tx, storeID)
		panic(fmt.Sprint(feed, store))
		return nil
		// if err := feed.SetHeads(ctx, store, src, heads); err != nil {
		// 	return err
		// }
		// if err := feed.AdoptHeads(ctx, store, src, dst); err != nil {
		// 	return err
		// }
		// return saveFeed(tx, feedID, feed)
	})
}

func (fc *feedController[T]) handleGet(ctx context.Context, src, dst owlnet.PeerID, rootID owlnet.FeedID) ([]feeds.Ref, error) {
	return dbutil.DoTx1(ctx, fc.db, func(tx *sqlx.Tx) ([]feeds.Ref, error) {
		feedID, err := lookupFeed(tx, dst, rootID)
		if err != nil {
			return nil, err
		}
		storeID, err := lookupFeedStore(tx, feedID)
		if err != nil {
			return nil, err
		}
		state, err := loadFeed[T](tx, feedID)
		if err != nil {
			return nil, err
		}
		store := newTxStore(tx, storeID)
		feed := feeds.New(fc.newProtocol(store), *state, store)
		yes, err := feed.CanRead(ctx, src)
		if err != nil {
			return nil, err
		}
		if !yes {
			return nil, errors.New("peer is not a member of feed")
		}
		return feed.GetHeads(), nil
	})
}

func (fc *feedController[T]) listFeeds(ctx context.Context) (ret []owlnet.FeedID, err error) {
	err = fc.db.Select(&ret, `SELECT id FROM feeds`)
	return ret, err
}

func (fc *feedController[T]) close() {
	fc.eg.Wait()
}

// Subscribe will close ch if the feed heads change from head
func (fc *feedController[T]) Subscribe(feedID owlnet.FeedID, heads []feeds.Ref) chan struct{} {
	ch := make(chan struct{})
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.subs[ch] = heads
	return ch
}

func (fc *feedController[T]) Unsubscribe(ch chan struct{}) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	delete(fc.subs, ch)
}

func (fc *feedController[T]) notify(heads []feeds.Ref) {
	for ch, heads2 := range fc.subs {
		if !slices.Equal(heads, heads2) {
			close(ch)
			delete(fc.subs, ch)
		}
	}
}

// createFeed creates a feed and the store for the feed
func createFeed[T any](tx *sqlx.Tx, protocol string, rootID feeds.ID, state feeds.State[T], storeID int) (int, error) {
	stateData := state.Marshal()
	var feedID int
	err := tx.Get(&feedID, `INSERT INTO feeds (protocol, root, state, store_id) VALUES (?, ?, ?, ?) RETURNING id`, protocol, rootID, stateData, storeID)
	return feedID, err
}

// deleteFeed deletes from the feed table
// it also deletes the store for the feed.
func deleteFeed(tx *sqlx.Tx, feedID int) error {
	storeID, err := lookupFeedStore(tx, feedID)
	if err != nil {
		return err
	}
	if err := deleteStore(tx, storeID); err != nil {
		return err
	}
	_, err = tx.Exec(`DELETE FROM feeds WHERE id = ?`, feedID)
	return err
}

func lookupFeedStore(tx dbutil.Reader, feedID int) (int, error) {
	var x int
	err := tx.Get(&x, `SELECT store_id FROM feeds WHERE id = ?`, feedID)
	return x, err
}

// loadFeed retrieves the feed state for feedID and returns it to the database.
func loadFeed[T any](tx *sqlx.Tx, feedID int) (*feeds.State[T], error) {
	var data []byte
	if err := tx.Get(&data, `SELECT state FROM feeds WHERE id = ? `, feedID); err != nil {
		return nil, err
	}
	return feeds.ParseState[T](data)
}

func saveFeed[T any](tx *sqlx.Tx, feedID int, x feeds.State[T]) error {
	_, err := tx.Exec(`UPDATE feeds SET state = ? WHERE id = ?`, x.Marshal(), feedID)
	if err != nil {
		return err
	}
	return nil
}

func lookupFeed(tx dbutil.Reader, dst PeerID, rootID feeds.ID) (int, error) {
	panic("not implemented")
}
