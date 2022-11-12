package owlcmd

import (
	"context"
	"errors"
	"os"

	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/inet256/diet256"
	"github.com/inet256/inet256/client/go_client/inet256client"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/spf13/cobra"
	"golang.org/x/exp/slog"

	"github.com/owlmessenger/owl/pkg/owl"
)

var ctx = logctx.NewContext(context.Background(), slog.New(slog.NewTextHandler(os.Stderr)))

func NewRootCmd() *cobra.Command {
	var s owl.API
	cmd := &cobra.Command{
		Use:   "owl",
		Short: "Owl Messenger",
	}
	dbPath := cmd.PersistentFlags().String("db", "", "--db=./db-path.db")
	cmd.PersistentPreRunE = func(cmd2 *cobra.Command, args []string) error {
		if *dbPath == "" {
			return errors.New("db flag is required")
		}
		db, err := owl.OpenDB(*dbPath)
		if err != nil {
			return err
		}
		var nwk inet256.Service
		if os.Getenv("INET256_API") == "" {
			logctx.Infof(ctx, "INET256_API not set, falling back to in-process diet256")
			nwk = diet256.New()
		} else {
			nwk, err = inet256client.NewEnvClient()
			if err != nil {
				return err
			}
		}
		s = owl.NewServer(db, nwk)
		return nil
	}
	cmd.PersistentPostRunE = func(cmd2 *cobra.Command, args []string) error {
		if closer, ok := s.(interface{ Close() error }); ok {
			return closer.Close()
		}
		return nil
	}
	for _, c := range []*cobra.Command{
		newChatCmd(func() owl.API { return s }),
		newPersonaCmd(func() owl.PersonaAPI { return s }),
		newChannelCmd(func() owl.ChannelAPI { return s }),
		newContactCmd(func() owl.ContactAPI { return s }),
		newServeCmd(func() owl.API { return s }),
	} {
		cmd.AddCommand(c)
	}
	return cmd
}
