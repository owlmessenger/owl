package owl_test

import (
	"testing"

	"github.com/owlmessenger/owl/src/dbutil"
	"github.com/owlmessenger/owl/src/owl"
	"github.com/owlmessenger/owl/src/owltest"
	"github.com/stretchr/testify/require"
	"go.inet256.org/inet256/client/go/inet256client"
)

func TestAPI(t *testing.T) {
	owltest.TestAPI(t, func(t testing.TB, xs []owl.API) {
		inetSrv := inet256client.NewTestService(t)
		for i := range xs {
			db := dbutil.NewTestDB(t)
			o := owl.NewServer(db, inetSrv)
			t.Cleanup(func() {
				require.NoError(t, o.Close())
			})
			xs[i] = o
		}
	})
}
