package owl

import (
	"context"
	"database/sql"
	"errors"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/jmoiron/sqlx"
	"golang.org/x/exp/maps"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/owlnet"
)

var _ ContactAPI = &Server{}

func (s *Server) AddContact(ctx context.Context, persona string, name string, id PeerID) error {
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		return s.addContact(tx, persona, name, id)
	}); err != nil {
		return err
	}
	// TODO: try to get feed from initial peer
	_, err := s.getLocalPeer(s.db, persona)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) RemoveContact(ctx context.Context, persona, name string) error {
	panic("not implemented")
}

func (s *Server) ListContact(ctx context.Context, persona string) ([]string, error) {
	return s.listContacts(s.db, persona)
}

func (s *Server) addContact(tx *sqlx.Tx, persona, name string, peerID PeerID) error {
	var personaID int
	if err := tx.Get(&personaID, `SELECT id FROM personas WHERE name = ?`, persona); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO persona_contacts (persona_id, name, last_peer) VALUES (?, ?, ?)`, personaID, name, peerID[:]); err != nil {
		return err
	}
	return nil
}

func (s *Server) listContacts(db dbSelector, persona string) (ret []string, err error) {
	err = db.Select(&ret, `SELECT persona_contacts.name FROM personas
		JOIN persona_contacts on personas.id = persona_contacts.persona_id
		WHERE personas.name = ?
	`, persona)
	return ret, err
}

func (s *Server) getPeersFor(tx *sqlx.Tx, persona string, contact string) ([]PeerID, error) {
	lastPeer, err := s.getLastPeer(tx, persona, contact)
	if err != nil {
		return nil, err
	}
	ret := []PeerID{lastPeer}
	feedID, err := s.lookupContactFeed(tx, persona, contact)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ret, nil
		}
		return nil, err
	}
	feed, err := loadFeed(tx, feedID)
	if err != nil {
		return nil, err
	}
	// TODO: don't just use the whole feed for peers
	ret = append(ret, maps.Keys(feed.Peers)...)
	// TODO: lookup peers from feed
	return ret, nil
}

func (s *Server) lookupContactFeed(tx *sqlx.Tx, persona, contact string) (owlnet.FeedID, error) {
	var x []byte
	if err := tx.Get(&x, `SELECT persona_contacts.feed_id FROM personas
		JOIN persona_contacts ON personas.id = persona_contacts.persona_id
		WHERE personas.name = ? AND persona_contacts.name = ? AND persona_contacts.feed_id IS NOT NULL
	`, persona, contact); err != nil {
		return owlnet.FeedID{}, err
	}
	return cadata.IDFromBytes(x), nil
}

func (s *Server) getLastPeer(tx *sqlx.Tx, persona string, contact string) (ret PeerID, err error) {
	var lastBytes []byte
	if err := tx.Get(&lastBytes, `SELECT last_peer FROM personas
		JOIN persona_contacts ON personas.id = persona_contacts.persona_id
		WHERE personas.name = ? AND persona_contacts.name = ?
	`, persona, contact); err != nil {
		return PeerID{}, err
	}
	return inet256.AddrFromBytes(lastBytes), nil
}

// lookupName returns the contact name for a peer
func (s *Server) lookupName(tx dbGetter, persona string, peerID PeerID) (string, error) {
	var name string
	// local personas
	if err := tx.Get(&name, `SELECT personas.name FROM personas
		JOIN persona_keys ON personas.id = persona_keys.persona_id
		WHERE personas.name = ? AND persona_keys.id = ?
	`, persona, peerID[:]); !errors.Is(err, sql.ErrNoRows) {
		return name, err
	}
	// last_peers
	if err := tx.Get(&name, `SELECT persona_contacts.name FROM personas
		JOIN persona_contacts ON personas.id = persona_contacts.persona_id
		WHERE personas.name = ? AND persona_contacts.last_peer = ?
	`, persona, peerID[:]); !errors.Is(err, sql.ErrNoRows) {
		return name, err
	}
	return "", errors.New("no contact/persona found for peer")
}
