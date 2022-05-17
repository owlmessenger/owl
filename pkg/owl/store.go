package owl

import (
	"context"
	"database/sql"
	"errors"
	"io"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/jmoiron/sqlx"
	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/feeds"
)

type txStore struct {
	tx    *sqlx.Tx
	intID int
}

func newTxStore(tx *sqlx.Tx, intID int) *txStore {
	return &txStore{tx: tx, intID: intID}
}

func (s *txStore) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	if len(data) > s.MaxSize() {
		return cadata.ID{}, cadata.ErrTooLarge
	}
	id := s.Hash(data)
	if _, err := s.tx.Exec(`INSERT INTO blobs (id, data)
		VALUES ($1, $2) ON CONFLICT DO NOTHING`, id[:], data); err != nil {
		return cadata.ID{}, err
	}
	if err := s.add(id); err != nil {
		return cadata.ID{}, err
	}
	return id, nil
}

func (s *txStore) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	var data []byte
	if err := s.tx.Get(&data, `SELECT blobs.data FROM store_blobs JOIN blobs ON blob_id = blobs.id
		WHERE store_id = $1 AND blob_id = $2
	`, s.intID, id[:]); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = cadata.ErrNotFound
		}
		return 0, err
	}
	if len(data) > len(buf) {
		return 0, io.ErrShortBuffer
	}
	return copy(buf, data), nil
}

func (s *txStore) Add(ctx context.Context, id cadata.ID) error {
	count, err := s.count(id)
	if err != nil {
		return err
	}
	if count > 0 {
		return s.add(id)
	} else {
		return cadata.ErrNotFound
	}
}

func (s *txStore) add(id cadata.ID) error {
	_, err := s.tx.Exec(`INSERT INTO store_blobs (store_id, blob_id)
		VALUES ($1, $2) ON CONFLICT DO NOTHING`, s.intID, id[:])
	return err
}

func (s *txStore) Delete(ctx context.Context, id cadata.ID) error {
	if _, err := s.tx.Exec(`DELETE FROM store_blobs WHERE store_id = $1 AND id = $2`, s.intID, id[:]); err != nil {
		return err
	}
	if count, err := s.count(id); err != nil {
		return err
	} else if count < 1 {
		if _, err := s.tx.Exec(`DELETE FROM store_blobs WHERE `); err != nil {
			return err
		}
	}
	return nil
}

func (s *txStore) List(ctx context.Context, first cadata.ID, ids []cadata.ID) (int, error) {
	rows, err := s.tx.Query(`SELECT blob_id FROM store_blobs
		WHERE store_id = $1 AND blob_id >= $2
		LIMIT $3
	`, s.intID, first[:], len(ids))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var n int
	for rows.Next() && n < len(ids) {
		var buf []byte
		if err := rows.Scan(&buf); err != nil {
			return 0, err
		}
		ids[n] = cadata.IDFromBytes(buf)
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if n < len(ids) {
		err = cadata.ErrEndOfList
	}
	return n, err
}

func (s *txStore) MaxSize() int {
	return feeds.MaxNodeSize
}

func (s *txStore) Hash(x []byte) cadata.ID {
	return feeds.Hash(x)
}

func (s *txStore) count(id cadata.ID) (count int, err error) {
	err = s.tx.Get(&count, `SELECT count(distinct store_id) FROM store_blobs WHERE blob_id = $1`, id[:])
	return count, err
}

type store struct {
	db    *sqlx.DB
	intID int
}

func newStore(db *sqlx.DB, intID int) *store {
	return &store{db: db, intID: intID}
}

func (s *store) Post(ctx context.Context, data []byte) (cadata.ID, error) {
	return dbutil.Do1Tx(ctx, s.db, func(tx *sqlx.Tx) (cadata.ID, error) {
		s2 := txStore{tx: tx, intID: s.intID}
		return s2.Post(ctx, data)
	})
}

func (s *store) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	return dbutil.Do1Tx(ctx, s.db, func(tx *sqlx.Tx) (int, error) {
		s2 := txStore{tx: tx, intID: s.intID}
		return s2.Get(ctx, id, buf)
	})
}

func (s *store) Add(ctx context.Context, id cadata.ID) error {
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		s2 := txStore{tx: tx, intID: s.intID}
		return s2.Add(ctx, id)
	})
}

func (s *store) Delete(ctx context.Context, id cadata.ID) error {
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		s2 := txStore{tx: tx, intID: s.intID}
		return s2.Delete(ctx, id)
	})
}

func (s *store) List(ctx context.Context, first cadata.ID, ids []cadata.ID) (int, error) {
	return dbutil.Do1Tx(ctx, s.db, func(tx *sqlx.Tx) (int, error) {
		s2 := txStore{tx: tx, intID: s.intID}
		return s2.List(ctx, first, ids)
	})
}

func (s *store) MaxSize() int {
	s2 := txStore{}
	return s2.MaxSize()
}

func (s *store) Hash(x []byte) cadata.ID {
	s2 := txStore{}
	return s2.Hash(x)
}
