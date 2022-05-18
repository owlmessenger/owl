package owlcmd

import (
	"context"
	"errors"

	"github.com/brendoncarroll/go-p2p/p2ptest"
	"github.com/inet256/inet256/networks/floodnet"
	"github.com/inet256/inet256/pkg/mesh256"
	"github.com/owlmessenger/owl/pkg/owl"
	"github.com/spf13/cobra"
)

type loadFunc = func(dirpath string) (owl.API, error)
type initFunc = func(dirpath string) error

var ctx = context.Background()

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
		nwk := mesh256.NewServer(mesh256.Params{
			NewNetwork: floodnet.Factory,
			PrivateKey: p2ptest.NewTestKey(nil, 0),
			Peers:      mesh256.NewPeerStore(),
		})
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
	} {
		cmd.AddCommand(c)
	}
	return cmd
}
