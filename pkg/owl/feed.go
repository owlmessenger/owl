package owl

import (
	"context"
	"crypto/rand"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/jmoiron/sqlx"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/feeds"
)

// createFeed creates a new feed by calling fn with a new store.
// fn should return the initial state for the feed, possibly referencing objects in s.
func createFeed[T any](tx *sqlx.Tx, protocol string, fn func(cadata.Store) (*T, error)) (int, error) {
	ctx := context.Background()
	seed := new([32]byte)
	if _, err := io.ReadFull(rand.Reader, seed[:]); err != nil {
		return 0, err
	}
	storeID, err := createStore(tx)
	if err != nil {
		return 0, err
	}
	s := newTxStore(tx, storeID)
	x, err := fn(s)
	if err != nil {
		return 0, err
	}
	fstate, err := feeds.InitialState(ctx, s, *x, seed)
	if err != nil {
		return 0, err
	}
	return insertFeed(tx, protocol, fstate.ID, *fstate, storeID)
}

// insertFeed inserts a single row in the feeds table
func insertFeed[T any](tx *sqlx.Tx, protocol string, rootID feeds.ID, state feeds.State[T], storeID int) (int, error) {
	stateData := state.Marshal()
	var feedID int
	err := tx.Get(&feedID, `INSERT INTO feeds (protocol, root, state, store_id) VALUES (?, ?, ?, ?) RETURNING id`, protocol, rootID, stateData, storeID)
	return feedID, err
}

// dropFeed deletes from the feed table
// it also deletes the store for the feed.
func dropFeed(tx *sqlx.Tx, feedID int) error {
	storeID, err := lookupFeedStore(tx, feedID)
	if err != nil {
		return err
	}
	if err := dropStore(tx, storeID); err != nil {
		return err
	}
	_, err = tx.Exec(`DELETE FROM feeds WHERE id = ?`, feedID)
	return err
}

// modifyFeed updates a feed's state by calling fn.
func modifyFeed[T any](tx *sqlx.Tx, feedID int, author PeerID, fn func(x feeds.State[T], sf, s0 cadata.Store) (*feeds.State[T], error)) error {
	x, err := loadFeed[T](tx, feedID)
	if err != nil {
		return err
	}
	storeID, err := lookupFeedStore(tx, feedID)
	if err != nil {
		return err
	}
	store := newTxStore(tx, storeID)
	y, err := fn(*x, store, store)
	if err != nil {
		return err
	}
	return saveFeed(tx, feedID, *y)
}

func viewFeed[T any](db *sqlx.DB, feedID int) (*T, cadata.Store, error) {
	fstate, err := loadFeed[T](db, feedID)
	if err != nil {
		return nil, nil, err
	}
	sid, err := lookupFeedStore(db, feedID)
	if err != nil {
		return nil, nil, err
	}
	return &fstate.State, newStore(db, sid), nil
}

// loadFeed retrieves the feed state for feedID and returns it to the database.
func loadFeed[T any](tx dbutil.Reader, feedID int) (*feeds.State[T], error) {
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

func lookupFeedStore(tx dbutil.Reader, feedID int) (int, error) {
	var x int
	err := tx.Get(&x, `SELECT store_id FROM feeds WHERE id = ?`, feedID)
	return x, err
}

// lookupFeed returns the feed for a persona
func lookupFeed(tx dbutil.Reader, personaID int, rootID feeds.ID) (ret int, _ error) {
	err := tx.Get(&ret, `SELECT id FROM feeds
		WHERE root = ?
		AND id IN (
			SELECT feed_id FROM persona_channels WHERE persona_id = ?
			UNION
			SELECT contactset_feed FROM personas WHERE id = ?
			UNION
			SELECT directory_feed FROM personas WHERE id = ?
		)
	`, rootID, personaID, personaID, personaID)
	return ret, err
}
