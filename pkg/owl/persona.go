package owl

import (
	"context"
	"crypto/ed25519"

	"github.com/inet256/inet256/pkg/inet256"
	"github.com/jmoiron/sqlx"
	"golang.org/x/exp/maps"

	"github.com/owlmessenger/owl/pkg/dbutil"
)

func (s *Server) CreatePersona(ctx context.Context, name string, ids []inet256.ID) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	_, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return err
	}
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		intID, err := s.createPersona(tx, name)
		if err != nil {
			return err
		}
		added := map[inet256.ID]struct{}{}
		pubKey := privKey.Public()
		id := inet256.NewAddr(pubKey)
		if err := s.addKey(tx, intID, id, pubKey, privKey); err != nil {
			return err
		}
		added[id] = struct{}{}
		for _, id := range ids {
			if _, exists := added[id]; exists {
				continue
			}
			if err := s.addKey(tx, intID, id, nil, nil); err != nil {
				return err
			}
			added[id] = struct{}{}
		}
		// if there are no other peers participating in this persona, then we create the feed.
		if len(ids) < 1 {
			feedID, err := createFeed(tx, maps.Keys(added))
			if err != nil {
				return err
			}
			if err := s.setPersonaFeed(tx, name, *feedID); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (s *Server) ListPersonas(ctx context.Context) (ret []string, _ error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	if err := s.db.SelectContext(ctx, &ret, `SELECT name FROM personas ORDER BY name`); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Server) GetPersona(ctx context.Context, name string) (*Persona, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return dbutil.Do1Tx(ctx, s.db, func(tx *sqlx.Tx) (*Persona, error) {
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
	return nil
}

func (s *Server) ShrinkPersona(ctx context.Context, name string, id inet256.ID) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Server) AddPrivateKey(ctx context.Context, name string, privateKey inet256.PrivateKey) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	pubKey := privateKey.Public()
	id := inet256.NewAddr(pubKey)
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		pid, err := s.lookupPersona(tx, name)
		if err != nil {
			return err
		}
		return s.addKey(tx, pid, id, pubKey, privateKey)
	})
}

func (s *Server) GetPeer(ctx context.Context, persona string) (*PeerID, error) {
	id, err := s.getLocalPeer(s.db, persona)
	if err != nil {
		return nil, err
	}
	return &id, nil
}
