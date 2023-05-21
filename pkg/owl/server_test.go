package owl_test

import (
	"testing"

	"github.com/inet256/inet256/client/go/inet256client"
	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/owl"
	"github.com/owlmessenger/owl/pkg/owltest"
	"github.com/stretchr/testify/require"
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
