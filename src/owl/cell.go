package owl

import (
	"bytes"
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/owlmessenger/owl/src/dbutil"
	"go.brendoncarroll.net/state/cells"
)

var _ cells.BytesCell = cell{}

type cell struct {
	db *sqlx.DB
	id int

	cells.BytesCellBase
}

func newCell(db *sqlx.DB, id int) cell {
	return cell{db: db}
}

func (c cell) CAS(ctx context.Context, actual *[]byte, prev, next []byte) (bool, error) {
	return dbutil.DoTx1(ctx, c.db, func(tx *sqlx.Tx) (bool, error) {
		var current []byte
		if err := tx.Get(&current, `SELECT cell FROM volumes WHERE id = ?`, c.id); err != nil {
			return false, err
		}
		if bytes.Equal(current, prev) {
			if _, err := tx.Exec(`UPDATE volumes SET cell = ? WHERE id = ?`, next, c.id); err != nil {
				return false, err
			}
			cells.CopyBytes(actual, next)
			return true, nil
		} else {
			cells.CopyBytes(actual, current)
			return false, nil
		}
	})
}

func (c cell) Load(ctx context.Context, dst *[]byte) error {
	var current []byte
	if err := c.db.GetContext(ctx, &current, `SELECT cell FROM volumes WHERE id = ?`, c.id); err != nil {
		return err
	}
	cells.CopyBytes(dst, current)
	return nil
}

func (c cell) MaxSize() int {
	return 1 << 20
}
