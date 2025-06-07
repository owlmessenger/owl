package e2etest

import (
	"context"
	"io"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/owlmessenger/owl/src/owl"
	"github.com/owlmessenger/owl/src/owljsonrpc"
	"github.com/owlmessenger/owl/src/owltest"
	"github.com/stretchr/testify/require"
)

func TestRPC(t *testing.T) {
	owltest.TestPersonasCRUD(t, setup)
}

func setup(t testing.TB, xs []owl.API) {
	for i := range xs {
		xs[i] = newOwl(t, i)
	}
}

func newOwl(t testing.TB, i int) *owljsonrpc.Client {
	ctx, cf := context.WithCancel(context.Background())
	t.Cleanup(cf)
	dbPath := t.TempDir() + "testdb_" + strconv.Itoa(i) + ".db"
	cmd := exec.CommandContext(ctx, "go", "run", "../cmd/owl", "--db="+dbPath, "serve", "-")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	rwc := &readWriteCloser{read: stdout.Read, write: stdin.Write, close: func() error {
		stdin.Close()
		stdout.Close()
		return nil
	}}
	go func() {
		io.Copy(os.Stderr, stderr)
	}()
	client := owljsonrpc.NewClient(rwc)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})
	return client
}

type readWriteCloser struct {
	read  func([]byte) (int, error)
	write func([]byte) (int, error)
	close func() error
}

func (rwc *readWriteCloser) Read(p []byte) (int, error) {
	return rwc.read(p)
}

func (rwc *readWriteCloser) Write(p []byte) (int, error) {
	return rwc.write(p)
}

func (rwc *readWriteCloser) Close() error {
	return rwc.close()
}
