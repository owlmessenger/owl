package owl

import (
	"context"

	"github.com/jmoiron/sqlx"

	"github.com/owlmessenger/owl/pkg/migrations"
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
	ApplyStmt(`CREATE TABLE feeds (
		id INTEGER NOT NULL,
		root BLOB NOT NULL,
		protocol TEXT NOT NULL,
		state BLOB NOT NULL,
		store_id INTEGER NOT NULL,
		FOREIGN KEY(store_id) REFERENCES stores(id),
		PRIMARY KEY(id)
	);`).
	ApplyStmt(`CREATE TABLE personas (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		contactset_feed INTEGER NOT NULL,
		directory_feed INTEGER NOT NULL,
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
		PRIMARY KEY(persona_id, id)
	);`).
	ApplyStmt(`CREATE TABLE persona_channels (
		persona_id INTEGER NOT NULL,
		feed_id INTEGER NOT NULL,
		FOREIGN KEY(persona_id) REFERENCES personas(id),
		FOREIGN KEY(feed_id) REFERENCES feeds(id),
		PRIMARY KEY(persona_id, feed_id)
	);`)

func setupDB(ctx context.Context, db *sqlx.DB) error {
	return migrations.Migrate(ctx, db, desiredState)
}
