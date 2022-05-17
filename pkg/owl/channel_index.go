package owl

import (
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"

	"github.com/owlmessenger/owl/pkg/feeds"
)

// func buildChannelIndexTx(tx *sqlx.Tx, chanInt int, feedID owlnet.FeedID) error {
// 	return nil
// }

// TODO: don't build the whole index every time
func (s *Server) buildChannelIndex(tx *sqlx.Tx, cid ChannelID) error {
	localID, err := s.getAuthor(tx, cid)
	if err != nil {
		return err
	}
	feedID, storeID, err := s.lookupChannelFeed(tx, cid)
	if err != nil {
		return err
	}
	feed, err := loadFeed(tx, feedID)
	if err != nil {
		return err
	}
	chanInt, err := s.lookupChannel(tx, cid)
	if err != nil {
		return err
	}
	heads := feed.GetHeads(localID)
	store := newTxStore(tx, storeID)
	todo := map[uint64][]feeds.Pair{}
	if err := feeds.ForEachDescGroup(nil, store, heads, func(n uint64, pairs []feeds.Pair) error {
		todo[n] = append([]feeds.Pair{}, pairs...)
		return nil
	}); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM channel_events WHERE channel_id = ?`, chanInt); err != nil {
		return err
	}
	var n uint64
	var index uint64
	for len(todo) > 0 {
		for _, pair := range todo[n] {
			mi := EventPath{index}
			if err := s.putChannelMsg(tx, chanInt, mi, pair.ID); err != nil {
				return err
			}
			index++
		}
		delete(todo, n)
		n++
	}
	return nil
}

func (s *Server) putChannelMsg(tx *sqlx.Tx, intID int, p EventPath, id feeds.NodeID) error {
	_, err := tx.Exec(`DELETE FROM channel_events WHERE channel_id = ? AND path = ?`, intID, p.Marshal())
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO channel_events (channel_id, path, blob_id) VALUES (?, ?, ?)`, intID, p.Marshal(), id[:])
	return err
}

func (s *Server) isBlobIndexed(tx *sqlx.Tx, intID int, id feeds.NodeID) (bool, error) {
	var x int
	if err := tx.Get(&x, `SELECT 1 FROM channel_events WHERE channel_id = ? AND blob_id = ?`, intID, id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}
		return false, err
	}
	return true, nil
}
