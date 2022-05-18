package owl

import (
	"context"
	"crypto"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/jmoiron/sqlx"

	"github.com/owlmessenger/owl/pkg/owlnet"
)

type dbGetter interface {
	Get(dst interface{}, query string, args ...interface{}) error
}

type dbSelector interface {
	Select(dst interface{}, query string, args ...interface{}) error
}

func setupDB(ctx context.Context, db *sqlx.DB) error {
	tx, err := db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS blobs (
			id BLOB,
			data BLOB NOT NULL,
			PRIMARY KEY(id)
		);`,
		`CREATE TABLE stores (
			id INTEGER PRIMARY KEY
		);`,
		`CREATE TABLE IF NOT EXISTS store_blobs (
			store_id INTEGER,
			blob_id BLOB,
			FOREIGN KEY(store_id) REFERENCES store_id,
			FOREIGN KEY(blob_id) REFERENCES blobs(id),
			PRIMARY KEY(store_id, blob_id)
		);`,
		`CREATE TABLE IF NOT EXISTS feeds (
			id BLOB,
			store_id INTEGER NOT NULL,
			state BLOB,
			UNIQUE(store_id),
			FOREIGN KEY(store_id) REFERENCES stores(id),
			PRIMARY KEY(id)
		);`,
		`CREATE TABLE IF NOT EXISTS personas (
			id INTEGER NOT NULL,
			name TEXT NOT NULL,
			feed_id BLOB,
			UNIQUE(name),
			PRIMARY KEY(id)
		);`,
		`CREATE TABLE persona_keys (
			persona_id INTEGER NOT NULL,
			id BLOB NOT NULL,
			public_key BLOB,
			private_key BLOB,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY(persona_id) REFERENCES personas(id),
			PRIMARY KEY(persona_id, id)
		);`,
		`CREATE TABLE persona_contacts (
			persona_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			last_peer BLOB NOT NULL,
			feed_id BLOB,
			FOREIGN KEY (persona_id) REFERENCES personas(id),
			PRIMARY KEY (persona_id, name)
		);`,
		`CREATE TABLE IF NOT EXISTS channels (
			id INTEGER NOT NULL,
			persona_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			feed_id BLOB NOT NULL,
			oob_store_id INTEGER NOT NULL,
			FOREIGN KEY(persona_id) REFERENCES personas(id),
			FOREIGN KEY(feed_id) REFERENCES feeds(id),
			FOREIGN KEY(oob_store_id) REFERENCES stores(id),
			PRIMARY KEY(id),
			UNIQUE(persona_id, name)
		);`,
		`CREATE TABLE IF NOT EXISTS channel_events (
			channel_id INTEGER,
			path BLOB NOT NULL,
			blob_id BLOB NOT NULL,
			FOREIGN KEY(channel_id) REFERENCES channels(id),
			FOREIGN KEY(blob_id) REFERENCES blobs(id),
			PRIMARY KEY(channel_id, path)
		);`,
	} {
		if _, err := tx.Exec(q); err != nil {
			return fmt.Errorf("%v while running statement %v", err, q)
		}
	}
	return tx.Commit()
}

func (s *Server) addKey(tx *sqlx.Tx, personaID int, id inet256.ID, pubKey inet256.PublicKey, privKey inet256.PrivateKey) error {
	var pubKeyData []byte
	if pubKey != nil {
		pubKeyData = inet256.MarshalPublicKey(pubKey)
	}
	var privKeyData []byte
	var err error
	if privKey != nil {
		privKeyData, err = x509.MarshalPKCS8PrivateKey(privKey)
	}
	_, err = tx.Exec(`INSERT INTO persona_keys (persona_id, id, public_key, private_key) VALUES (?, ?, ?, ?)`, personaID, id[:], pubKeyData, privKeyData)
	return err
}

func (s *Server) createPersona(tx *sqlx.Tx, name string) (int, error) {
	var intID int
	err := tx.Get(&intID, `INSERT INTO personas (name) VALUES (?) RETURNING id`, name)
	return intID, err
}

func (s *Server) lookupPersona(tx *sqlx.Tx, name string) (int, error) {
	var personaID int
	err := tx.Get(&personaID, `SELECT id FROM personas WHERE name = ?`, name)
	return personaID, err
}

// getAuthor returns a peer in the persona, for which a private key is known
func (s *Server) getAuthor(tx dbGetter, cid ChannelID) (PeerID, error) {
	var x []byte
	err := tx.Get(&x, `SELECT persona_keys.id as id FROM persona_keys
		JOIN personas on personas.id = persona_keys.persona_id
		WHERE personas.name = ?
	`, cid.Persona)
	return inet256.AddrFromBytes(x), err
}

func (s *Server) getLocalPeer(tx dbGetter, persona string) (PeerID, error) {
	var x []byte
	err := tx.Get(&x, `SELECT persona_keys.id as id FROM persona_keys
		JOIN personas on personas.id = persona_keys.persona_id
		WHERE personas.name = ?
	`, persona)
	return inet256.AddrFromBytes(x), err
}

func (s *Server) getPrivateKey(tx dbGetter, localID PeerID) (inet256.PrivateKey, error) {
	var privKeyData []byte
	if err := tx.Get(&privKeyData, `SELECT private_key FROM persona_keys WHERE id = ?`, localID[:]); err != nil {
		return nil, err
	}
	privateKey, err := x509.ParsePKCS8PrivateKey(privKeyData)
	if err != nil {
		return nil, err
	}
	return privateKey.(crypto.Signer), nil
}

func (s *Server) setPersonaFeed(tx *sqlx.Tx, persona string, feedID owlnet.FeedID) error {
	_, err := tx.Exec(`UPDATE personas SET feed_id = ? WHERE name = ?`, feedID[:], persona)
	return err
}

func (s *Server) createChannel(tx *sqlx.Tx, cid ChannelID, feedID owlnet.FeedID) (int, error) {
	personaID, err := s.lookupPersona(tx, cid.Persona)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = fmt.Errorf("persona %q does not exist", cid.Persona)
		}
		return 0, err
	}
	oobStoreID, err := createStore(tx)
	if err != nil {
		return 0, err
	}
	var intID int
	err = tx.Get(&intID, `INSERT INTO channels (persona_id, name, feed_id, oob_store_id) VALUES (?, ?, ?, ?) RETURNING id`, personaID, cid.Name, feedID[:], oobStoreID)
	return intID, err
}

func (s *Server) lookupChannel(tx *sqlx.Tx, cid ChannelID) (id int, err error) {
	err = tx.Get(&id, `SELECT channels.id FROM personas
		JOIN channels ON personas.id = channels.persona_id
		WHERE personas.name = ? AND channels.name = ?
	`, cid.Persona, cid.Name)
	return id, err
}

// deleteChannel deletes from the channel table
func (s *Server) deleteChannel(tx *sqlx.Tx, cid ChannelID) error {
	panic("not implemented")
}

func (s *Server) lookupChannelFeed(tx *sqlx.Tx, cid ChannelID) (owlnet.FeedID, int, error) {
	var x struct {
		ID      []byte `db:"id"`
		StoreID int    `db:"store_id"`
	}
	err := tx.Get(&x, `SELECT feeds.id, feeds.store_id FROM channels
		JOIN feeds ON feeds.id = channels.feed_id
		JOIN personas ON personas.id = channels.persona_id
		WHERE personas.name = ? AND channels.name = ?`, cid.Persona, cid.Name)
	return cadata.IDFromBytes(x.ID), x.StoreID, err
}

// createStore allocates a new store ID which wil not be reused
func createStore(tx *sqlx.Tx) (ret int, err error) {
	err = tx.Get(&ret, `INSERT INTO stores VALUES (NULL) RETURNING id`)
	return ret, err
}

// deleteStore deletes a store and any blobs not included in another store.
func deleteStore(tx *sqlx.Tx, storeID int) error {
	if _, err := tx.Exec(`DELETE FROM stores WHERE id = ?`, storeID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM store_blobs WHERE store_id = ?`, storeID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM blobs WHERE id NOT IN (
		SELECT blob_id FROM store_blobs
	)`); err != nil {
		return err
	}
	return nil
}
