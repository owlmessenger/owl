package owl

import (
	"bytes"
	"errors"
	"math"

	"github.com/jmoiron/sqlx"
	"golang.org/x/exp/slices"

	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/slices2"
)

// indexChannelNode indexes a feeds.Node into the channel_events table
func indexChannelNode(tx *sqlx.Tx, chanInt int, id feeds.NodeID, node feeds.Node) error {
	ep := EventPath{}
	if node.Data != nil {
		msg, err := parseMessage(node.Data)
		if err != nil {
			return err
		}
		if msg.Parent != nil {
			// TODO: support nested messages
			// append parent N
			panic(msg.Parent)
		}
	}
	i := (node.N << 8)
	ep = append(ep, i)
	return putChannelEvent(tx, chanInt, ep, id)
}

// putChannelEvent indexes a channel event and reindexes if necessary
func putChannelEvent(tx *sqlx.Tx, intID int, p EventPath, id feeds.NodeID) error {
	pEnd := slices.Clone(p)
	pEnd[len(pEnd)-1] = math.MaxUint64
	type Row struct {
		Path EventPath `db:"path"`
		ID   []byte    `db:"blob_id"`
	}
	// select
	var xs []Row
	if err := tx.Select(&xs, `SELECT path, blob_id FROM channel_events
		WHERE channel_id = ? AND path >= ?
	`, intID, p, pEnd); err != nil {
		return err
	}
	// delete
	_, err := tx.Exec(`DELETE FROM channel_events
		WHERE channel_id = ? AND path >= ? AND path < ?
	`, intID, p, pEnd)
	if err != nil {
		return err
	}
	// add
	xs = append(xs, Row{Path: p, ID: id[:]})
	if len(xs) > MaxChannelPeers {
		return errors.New("feed wider than number of peers")
	}
	// sort
	slices.SortFunc(xs, func(a, b Row) bool {
		return bytes.Compare(a.ID[:], b.ID[:]) < 0
	})
	// dedup
	xs = slices2.DedupSorted(xs, func(a, b Row) bool {
		return bytes.Equal(a.ID, b.ID)
	})
	// assign paths
	for i, x := range xs {
		l := len(x.Path)
		x.Path[l-1] |= uint64(i)
	}
	// insert
	for _, x := range xs {
		if _, err = tx.Exec(`INSERT INTO channel_events (channel_id, path, blob_id)
			VALUES (?, ?, ?)
		`, intID, x.Path, x.ID[:]); err != nil {
			return err
		}
	}
	return err
}
