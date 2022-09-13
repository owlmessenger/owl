package owl

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/x509"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/jmoiron/sqlx"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/p/contactset"
	"github.com/owlmessenger/owl/pkg/p/directory"
)

func (s *Server) CreatePersona(ctx context.Context, name string) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	_, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return err
	}
	pubKey := privKey.Public()
	csFeed, err := s.contactSetCtrl.Create(ctx, func(s cadata.Store) (*contactset.State, error) {
		op := contactset.New()
		return op.New(ctx, s)
	})
	if err != nil {
		return err
	}
	dirFeed, err := s.directoryCtrl.Create(ctx, func(s cadata.Store) (*directory.State, error) {
		op := directory.New()
		return op.New(ctx, s)
	})
	if err != nil {
		return err
	}
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		personaID, err := s.createPersona(tx, name, csFeed, dirFeed)
		if err != nil {
			return err
		}
		return s.addPrivateKey(tx, personaID, pubKey, privKey)
	})
}

func (s *Server) JoinPersona(ctx context.Context, name string, ids []inet256.Addr) error {
	panic("not implemented")
	return nil
}

func (s *Server) ListPersonas(ctx context.Context) (ret []string, _ error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	if err := s.db.SelectContext(ctx, &ret, `SELECT id FROM personas ORDER BY name`); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Server) GetPersona(ctx context.Context, name string) (*Persona, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*Persona, error) {
		intID, err := s.lookupPersona(tx, name)
		if err != nil {
			return nil, err
		}
		var p Persona
		rows, err := tx.Query(`SELECT id, private_key IS NOT NULL FROM persona_keys WHERE persona_id = $1 ORDER BY created_at`, intID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var idBytes []byte
			var isLocal bool
			if err := rows.Scan(&idBytes, &isLocal); err != nil {
				return nil, err
			}
			id := inet256.AddrFromBytes(idBytes)
			if isLocal {
				p.LocalIDs = append(p.LocalIDs, id)
			} else {
				p.RemoteIDs = append(p.RemoteIDs, id)
			}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return &p, nil
	})
}

func (s *Server) ExpandPersona(ctx context.Context, name string, id inet256.ID) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	// TODO: use node to lookup public key
	// add public key
	panic("not implemented")
	return nil
}

func (s *Server) ShrinkPersona(ctx context.Context, name string, id inet256.ID) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM persona_keys
		WHERE id = ? AND persona_id IN (
			SELECT id FROM personas WHERE name = ?
		)
	`, id, name)
	return err
}

func (s *Server) AddPrivateKey(ctx context.Context, name string, privateKey inet256.PrivateKey) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	pubKey := privateKey.Public()
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		pid, err := s.lookupPersona(tx, name)
		if err != nil {
			return err
		}
		return s.addPrivateKey(tx, pid, pubKey, privateKey)
	})
}

// GetLocalPeer returns a PeerID to use for communication
func (s *Server) GetLocalPeer(ctx context.Context, persona string) (*PeerID, error) {
	id, err := s.getLocalPeer(s.db, persona)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// createPersona inserts into the personas table
func (s *Server) createPersona(tx *sqlx.Tx, name string, contactSetFeed, dirFeed int) (int, error) {
	var personaID int
	err := tx.Get(&personaID, `INSERT INTO personas (name, contactset_feed, directory_feed)
		VALUES (?, ?, ?) RETURNING id`, name, contactSetFeed, dirFeed)
	return personaID, err
}

func (s *Server) getLocalPeer(tx dbutil.Reader, persona string) (PeerID, error) {
	var x []byte
	err := tx.Get(&x, `SELECT persona_keys.id as id FROM persona_keys
		JOIN personas on personas.id = persona_keys.persona_id
		WHERE personas.name = ?
	`, persona)
	return inet256.AddrFromBytes(x), err
}

func (s *Server) getPrivateKey(tx dbutil.Reader, localID PeerID) (inet256.PrivateKey, error) {
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

func (s *Server) lookupPersona(tx dbutil.Reader, name string) (int, error) {
	var personaID int
	err := tx.Get(&personaID, `SELECT id FROM personas WHERE name = ?`, name)
	return personaID, err
}

func (s *Server) addPrivateKey(tx *sqlx.Tx, personaID int, pubKey inet256.PublicKey, privKey inet256.PrivateKey) error {
	id := inet256.NewAddr(pubKey)
	var pubKeyData []byte
	if pubKey != nil {
		pubKeyData = inet256.MarshalPublicKey(pubKey)
	}
	privKeyData, err := x509.MarshalPKCS8PrivateKey(privKey)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO persona_keys (persona_id, id, public_key, private_key) VALUES (?, ?, ?, ?)`, personaID, id[:], pubKeyData, privKeyData)
	return err
}
