package owlcmd

import (
	"github.com/owlmessenger/owl/pkg/owl"
	"github.com/spf13/cobra"
)

type loadFunc = func(dirpath string) (owl.API, error)
type initFunc = func(dirpath string) error

func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "owl",
		Short: "Owl Messenger",
	}
	for _, c := range []*cobra.Command{
		newChatCmd(nil),
	} {
		cmd.AddCommand(c)
	}
	return cmd
}
