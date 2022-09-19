package owl

import (
	"bytes"
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/owlmessenger/owl/pkg/dbutil"
)

type cell struct {
	db *sqlx.DB
	id int
}

func newCell(db *sqlx.DB, id int) cell {
	return cell{db: db}
}

func (c cell) CAS(ctx context.Context, actual, prev, next []byte) (bool, int, error) {
	return dbutil.DoTx2(ctx, c.db, func(tx *sqlx.Tx) (bool, int, error) {
		var current []byte
		if err := tx.Get(&current, `SELECT cell FROM volumes WHERE id = ?`, c.id); err != nil {
			return false, 0, err
		}
		if bytes.Equal(current, prev) {
			if _, err := tx.Exec(`UPDATE volumes SET cell = ? WHERE id = ?`, next, c.id); err != nil {
				return false, 0, err
			}
			return true, copy(actual, next), nil
		} else {
			return false, copy(actual, current), nil
		}
	})
}

func (c cell) Read(ctx context.Context, buf []byte) (int, error) {
	var current []byte
	if err := c.db.GetContext(ctx, &current, `SELECT cell FROM volumes WHERE id = ?`, c.id); err != nil {
		return 0, err
	}
	return copy(buf, current), nil
}

func (c cell) MaxSize() int {
	return 1 << 20
}
