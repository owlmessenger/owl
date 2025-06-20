package owl

import (
	"context"

	"github.com/jmoiron/sqlx"

	"github.com/owlmessenger/owl/src/migrations"
)

func OpenDB(dbPath string) (*sqlx.DB, error) {
	db, err := sqlx.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

var desiredState = migrations.InitialState().
	ApplyStmt(`CREATE TABLE blobs (
		id BLOB,
		data BLOB NOT NULL,
		PRIMARY KEY(id)
	);`).
	ApplyStmt(`CREATE TABLE stores (
		id INTEGER PRIMARY KEY
	);`).
	ApplyStmt(`CREATE TABLE store_blobs (
		store_id INTEGER,
		blob_id BLOB,
		FOREIGN KEY(store_id) REFERENCES store_id,
		FOREIGN KEY(blob_id) REFERENCES blobs(id),
		PRIMARY KEY(store_id, blob_id)
	);`).
	ApplyStmt(`CREATE TABLE volumes (
		id INTEGER NOT NULL,
		cell BLOB NOT NULL,
		store_top INTEGER NOT NULL,
		store_0 INTEGER NOT NULL,
		FOREIGN KEY(store_top) REFERENCES stores(id),
		FOREIGN KEY(store_0) REFERENCES stores(id),
		PRIMARY KEY(id)
	);`).
	ApplyStmt(`CREATE TABLE personas (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		UNIQUE(name)
	);`).
	ApplyStmt(`CREATE TABLE persona_keys (
		persona_id INTEGER NOT NULL,
		id BLOB NOT NULL,
		public_key BLOB NOT NULL,
		private_key BLOB NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(persona_id) REFERENCES personas(id),
		PRIMARY KEY(persona_id, id)
	);`).
	ApplyStmt(`CREATE TABLE persona_peers (
		persona_id INTEGER NOT NULL,
		id BLOB NOT NULL,
		public_key BLOB,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(persona_id) REFERENCES personas(id),
		UNIQUE(id),
		PRIMARY KEY(persona_id, id)
	);`).
	ApplyStmt(`CREATE TABLE persona_volumes (
		persona_id INTEGER NOT NULL,
		volume_id INTEGER NOT NULL,
		scheme TEXT NOT NULL,
		FOREIGN KEY(persona_id) REFERENCES personas(id),
		FOREIGN KEY(volume_id) REFERENCES volume(id),
		UNIQUE(volume_id),
		PRIMARY KEY(persona_id, volume_id)
	);`)

func setupDB(ctx context.Context, db *sqlx.DB) error {
	return migrations.Migrate(ctx, db, desiredState)
}
