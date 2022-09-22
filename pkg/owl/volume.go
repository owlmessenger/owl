package owl

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cells"
	"github.com/jmoiron/sqlx"

	"github.com/owlmessenger/owl/pkg/dbutil"
)

// createVolume creates a new volume, with an empty cell.
func createVolume(tx *sqlx.Tx) (int, error) {
	sTopID, err := createStore(tx)
	if err != nil {
		return 0, err
	}
	s0ID, err := createStore(tx)
	if err != nil {
		return 0, err
	}
	var volumeID int
	err = tx.Get(&volumeID, `INSERT INTO volumes (cell, store_0, store_top)
		VALUES (?, ?, ?) RETURNING id
	`, []byte{}, s0ID, sTopID)
	return volumeID, err
}

// dropVolume deletes from the volume table
// it also deletes the store for the volume.
func dropVolume(tx *sqlx.Tx, volumeID int) error {
	s0ID, sTopID, err := lookupVolumeStores(tx, volumeID)
	if err != nil {
		return err
	}
	for _, sid := range []int{s0ID, sTopID} {
		if err := dropStore(tx, sid); err != nil {
			return err
		}
	}
	_, err = tx.Exec(`DELETE FROM volumes WHERE id = ?`, volumeID)
	return err
}

// modifyVolumeTx modifies a volume in a transaction
func modifyVolumeTx(tx *sqlx.Tx, volumeID int, fn func(x []byte, s0, sTop cadata.Store) ([]byte, error)) error {
	var x []byte
	if err := tx.Get(&x, `SELECT cell FROM volumes WHERE id = ? `, volumeID); err != nil {
		return err
	}
	s0ID, sTopID, err := lookupVolumeStores(tx, volumeID)
	if err != nil {
		return err
	}
	y, err := fn(x, newTxStore(tx, s0ID), newTxStore(tx, sTopID))
	if err != nil {
		return err
	}
	if y == nil {
		y = []byte{}
	}
	_, err = tx.Exec(`UPDATE volumes SET cell = ? WHERE id = ?`, y, volumeID)
	return err
}

// modifyVolume does a compare-and-swap outside a single transaction.
// fn may be called more than once
func modifyVolume(ctx context.Context, db *sqlx.DB, volumeID int, fn func(x []byte, s0, sTop cadata.Store) ([]byte, error)) error {
	c := newCell(db, volumeID)
	s0ID, sTopID, err := lookupVolumeStores(db, volumeID)
	if err != nil {
		return err
	}
	s0, sTop := newStore(db, s0ID), newStore(db, sTopID)
	return cells.Apply(ctx, c, func(x []byte) ([]byte, error) {
		y, err := fn(x, s0, sTop)
		if err != nil {
			return nil, err
		}
		if y == nil {
			y = []byte{}
		}
		return y, nil
	})
}

// viewVolumeTx returns the data and store for a volume for use in a transaction.
func viewVolumeTx(tx *sqlx.Tx, volID int) (data []byte, s0, sTop cadata.Store, _ error) {
	var x struct {
		Data []byte `db:"cell"`
		S0   int    `db:"store_0"`
		STop int    `db:"store_top"`
	}
	if err := tx.Get(&x, `SELECT cell, store_0, store_top FROM volumes WHERE id = ?`, volID); err != nil {
		return nil, nil, nil, err
	}
	return x.Data, newTxStore(tx, x.S0), newTxStore(tx, x.STop), nil
}

func viewVolume(ctx context.Context, db *sqlx.DB, volID int) (data []byte, s0 cadata.Store, sTop cadata.Store, _ error) {
	var x struct {
		Data   []byte `db:"cell"`
		S0     int    `db:"store_0"`
		STop   int    `db:"store_top"`
		Scheme string `db:"scheme"`
	}
	if err := db.GetContext(ctx, &x, `SELECT cell, store_0, store_top, scheme FROM volumes WHERE id = ?`, volID); err != nil {
		return nil, nil, nil, err
	}
	return x.Data, newStore(db, x.S0), newStore(db, x.STop), nil
}

// findVolumes finds volumes for persona with stores which contain ID.
func findVolumes(tx dbutil.Reader, personaID int, id cadata.ID) (ret []int, _ error) {
	err := tx.Select(&ret, `SELECT volume_id FROM persona_volumes as pv
		JOIN volumes ON pv.volume_id = volumes.id
		JOIN stores ON volumes.store_top = stores.id
		JOIN store_blobs ON stores.id = store_blobs.store_id
		WHERE pv.persona_id = ? AND store_blobs.blob_id = ?
	`, personaID, id)
	return ret, err
}

func findVolume(tx dbutil.Reader, personaID int, ids ...cadata.ID) (int, string, error) {
	for _, id := range ids {
		vols, err := findVolumes(tx, personaID, id)
		if err != nil {
			return 0, "", err
		}
		if len(vols) > 0 {
			var scheme string
			if err := tx.Get(&scheme, `SELECT scheme FROM persona_volumes WHERE persona_id = ? AND volume_id = ?`, personaID, vols[0]); err != nil {
				return 0, "", err
			}
			return vols[0], scheme, nil
		}
	}
	return 0, "", fmt.Errorf("no feed found for persona=%v ids=%v", personaID, ids)
}

func lookupVolumeStores(tx dbutil.Reader, volumeID int) (s0, sTop int, _ error) {
	var x struct {
		Top int `db:"store_top"`
		S0  int `db:"store_0"`
	}
	err := tx.Get(&x, `SELECT store_top, store_0 FROM volumes WHERE id = ?`, volumeID)
	return x.S0, x.Top, err
}
