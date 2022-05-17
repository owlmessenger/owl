package owl

import (
	"context"
	"database/sql"
	"errors"

	"github.com/inet256/inet256/pkg/inet256"
	"github.com/jmoiron/sqlx"

	"github.com/owlmessenger/owl/pkg/dbutil"
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
	if _, err := tx.Exec(`INSERT INTO persona_contacts (persona_id, name, intro_peer) VALUES (?, ?, ?)`, personaID, name, peerID[:]); err != nil {
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
	intoPeer, err := s.getIntroPeer(tx, persona, contact)
	if err != nil {
		return nil, err
	}
	ret := []PeerID{intoPeer}
	// TODO: lookup peers from feed
	return ret, nil
}

func (s *Server) getIntroPeer(tx *sqlx.Tx, persona string, contact string) (ret PeerID, err error) {
	var introBytes []byte
	if err := tx.Get(&introBytes, `SELECT intro_peer FROM personas
		JOIN persona_contacts ON personas.id = persona_contacts.persona_id
		WHERE personas.name = ? AND persona_contacts.name = ?
	`, persona, contact); err != nil {
		return PeerID{}, err
	}
	return inet256.AddrFromBytes(introBytes), nil
}

// lookupName returns the contact name for a peer
func (s *Server) lookupName(tx dbGetter, persona string, peerID PeerID) (string, error) {
	var name string
	// first try intro_id
	if err := tx.Get(&name, `SELECT persona_contacts.name FROM personas
		JOIN persona_contacts ON personas.id = persona_contacts.persona_id
		WHERE personas.name = ? AND persona_contacts.intro_peer = ?
	`, persona, peerID[:]); !errors.Is(err, sql.ErrNoRows) {
		return name, err
	}
	if err := tx.Get(&name, `SELECT personas.name FROM personas
		JOIN persona_keys ON personas.id = persona_keys.persona_id
		WHERE personas.name = ? AND persona_keys.id = ?
	`, persona, peerID[:]); !errors.Is(err, sql.ErrNoRows) {
		return name, err
	}
	panic("no key for contact")
}
