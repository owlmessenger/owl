package owljsonrpc

import (
	"context"
	"net"
	"testing"

	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/inet256/inet256/client/go_client/inet256client"
	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/owl"
	"github.com/owlmessenger/owl/pkg/owltest"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var ctx = logctx.WithFmtLogger(context.Background(), logrus.StandardLogger())

func TestJSONRPC(t *testing.T) {
	owltest.TestAPI(t, func(t testing.TB, xs []owl.API) {
		setupBase(t, xs[:])
		setupRPC(t, xs[:])
	})
}

func setupBase(t testing.TB, xs []owl.API) {
	inetSrv := inet256client.NewTestService(t)
	for i := range xs {
		db := dbutil.NewTestDB(t)
		o := owl.NewServer(db, inetSrv)
		t.Cleanup(func() {
			require.NoError(t, o.Close())
		})
		xs[i] = o
	}
}

func setupRPC(t testing.TB, xs []owl.API) {
	for i := range xs {
		a, b := net.Pipe()
		t.Cleanup(func() { a.Close() })
		t.Cleanup(func() { b.Close() })

		go ServeRWC(ctx, b, xs[i])
		xs[i] = NewClient(a)
	}
}
