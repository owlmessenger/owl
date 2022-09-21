package owlcmd

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/owlmessenger/owl/pkg/owl"
	"github.com/owlmessenger/owl/pkg/owljsonrpc"
)

func newServeCmd(sf func() owl.API) *cobra.Command {
	return &cobra.Command{
		Use:  "serve",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			addr := args[0]

			var rwc io.ReadWriteCloser
			switch addr {
			case "-":
				rwc = &readWriteCloser{
					read:  cmd.InOrStdin().Read,
					write: cmd.OutOrStdout().Write,
					close: func() error { return nil },
				}
			default:
				return fmt.Errorf("unsupported address %v", addr)
			}
			s := sf()
			return owljsonrpc.ServeRWC(ctx, rwc, s)
		},
	}
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
