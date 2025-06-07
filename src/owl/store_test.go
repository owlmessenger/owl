package owl

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/owlmessenger/owl/src/dbutil"
	"github.com/stretchr/testify/require"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/state/cadata/storetest"
)

func TestStore(t *testing.T) {
	db := dbutil.NewTestDB(t)
	require.NoError(t, setupDB(context.TODO(), db))

	var n int32
	storetest.TestStore(t, func(t testing.TB) cadata.Store {
		i := atomic.AddInt32(&n, 1)
		s := newStore(db, int(i))
		return s
	})
}
