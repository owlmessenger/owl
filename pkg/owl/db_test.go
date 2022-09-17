package owl

import (
	"context"
	"testing"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/stretchr/testify/require"
)

func TestSetupDB(t *testing.T) {
	db := dbutil.NewTestDB(t)
	db.SetMaxOpenConns(1)
	err := setupDB(context.Background(), db)
	require.NoError(t, err)
}
