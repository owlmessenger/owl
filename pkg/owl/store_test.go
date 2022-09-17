package owl

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-state/cadata/storetest"
	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/stretchr/testify/require"
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
