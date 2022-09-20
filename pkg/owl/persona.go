package owl

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/jmoiron/sqlx"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/schemes/contactset"
	"github.com/owlmessenger/owl/pkg/schemes/directory"
)

func (s *Server) CreatePersona(ctx context.Context, req *CreatePersonaReq) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	_, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}

	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		personaID, err := s.createPersona(tx, req.Name)
		if err != nil {
			return err
		}

		csVol, err := createVolume(tx)
		if err != nil {
			return err
		}
		if err := assocVol(tx, personaID, csVol, contactSetScheme); err != nil {
			return err
		}
		if _, err := initDAG(tx, csVol, func(s cadata.Store) (*contactset.State, error) {
			op := contactset.New()
			return op.New(ctx, s)
		}); err != nil {
			return err
		}

		dirVol, err := createVolume(tx)
		if err != nil {
			return err
		}
		if err := assocVol(tx, personaID, dirVol, directoryScheme); err != nil {
			return err
		}
		if _, err := initDAG(tx, dirVol, func(s cadata.Store) (*directory.State, error) {
			op := directory.New()
			return op.New(ctx, s)
		}); err != nil {
			return err
		}

		return s.addPrivateKey(tx, personaID, privKey)
	})
}

func (s *Server) JoinPersona(ctx context.Context, req *JoinPersonaReq) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	_, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}

	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		personaID, err := s.createPersona(tx, req.Name)
		if err != nil {
			return err
		}
		return s.addPrivateKey(tx, personaID, privKey)
	})
}

func (s *Server) DropPersona(ctx context.Context, name string) error {
	panic("not implemented")
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

func (s *Server) GetPersona(ctx context.Context, req *GetPersonaReq) (*Persona, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*Persona, error) {
		intID, err := s.lookupPersona(tx, req.Name)
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

func (s *Server) ExpandPersona(ctx context.Context, req *ExpandPersonaReq) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	// TODO: use node to lookup public key
	// add public key
	panic("not implemented")
	return nil
}

func (s *Server) ShrinkPersona(ctx context.Context, req *ShrinkPersonaReq) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM persona_keys
		WHERE id = ? AND persona_id IN (
			SELECT id FROM personas WHERE name = ?
		)
	`, req.Peer[:], req.Name)
	return err
}

func (s *Server) AddPrivateKey(ctx context.Context, name string, privateKey inet256.PrivateKey) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		pid, err := s.lookupPersona(tx, name)
		if err != nil {
			return err
		}
		return s.addPrivateKey(tx, pid, privateKey)
	})
}

// GetLocalPeer returns a PeerID to use for communication
func (s *Server) GetLocalPeer(ctx context.Context, persona string) (*PeerID, error) {
	ps, err := s.getPersonaServer(ctx, persona)
	if err != nil {
		return nil, err
	}
	id, err := ps.getLocalPeer(ctx)
	return &id, err
}

// createPersona inserts into the personas table
func (s *Server) createPersona(tx *sqlx.Tx, name string) (int, error) {
	var personaID int
	err := tx.Get(&personaID, `INSERT INTO personas (name)
		VALUES (?) RETURNING id`, name)
	return personaID, err
}

func (s *Server) lookupPersona(tx dbutil.Reader, name string) (int, error) {
	var personaID int
	err := tx.Get(&personaID, `SELECT id FROM personas WHERE name = ?`, name)
	return personaID, err
}

func (s *Server) addPrivateKey(tx *sqlx.Tx, personaID int, privKey inet256.PrivateKey) error {
	pubKey := privKey.Public()
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

func getPersonaMembers(tx dbutil.Reader, id int) (ret []PeerID, _ error) {
	var rows [][]byte
	if err := tx.Select(&rows, `SELECT id FROM persona_keys WHERE persona_id = ?`, id); err != nil {
		return nil, err
	}
	for _, row := range rows {
		ret = append(ret, inet256.AddrFromBytes(row))
	}
	return ret, nil
}

// assocVol associates a volume with a persona by id.
func assocVol(tx *sqlx.Tx, personaID, volID int, scheme string) error {
	_, err := tx.Exec(`INSERT INTO persona_volumes (persona_id, volume_id, scheme) VALUES (?, ?, ?)`, personaID, volID, scheme)
	return err
}
