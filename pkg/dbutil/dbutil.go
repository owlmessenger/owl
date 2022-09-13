package dbutil

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type Reader interface {
	Get(dst interface{}, query string, args ...interface{}) error
	Select(dst interface{}, query string, args ...interface{}) error
}

func DoTx(ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) error) error {
	tx, err := db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func DoTx1[T any](ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) (T, error)) (T, error) {
	var ret, zero T
	err := DoTx(ctx, db, func(tx *sqlx.Tx) error {
		ret = zero
		var err error
		ret, err = fn(tx)
		return err
	})
	return ret, err
}

func DoTx2[A, B any](ctx context.Context, db *sqlx.DB, fn func(tx *sqlx.Tx) (A, B, error)) (A, B, error) {
	var a, zeroA A
	var b, zeroB B
	err := DoTx(ctx, db, func(tx *sqlx.Tx) error {
		a, b = zeroA, zeroB
		var err error
		a, b, err = fn(tx)
		return err
	})
	return a, b, err
}
