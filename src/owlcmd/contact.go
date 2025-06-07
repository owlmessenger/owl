package owlcmd

import (
	"bufio"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/owlmessenger/owl/src/owl"
)

func newContactCmd(sf func() owl.ContactAPI) *cobra.Command {
	s := sf()
	c := &cobra.Command{}

	lsCmd := &cobra.Command{
		Use:   "ls",
		Short: "lists contacts for a persona",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			persona := args[0]
			contacts, err := s.ListContact(ctx, &owl.ListContactReq{Persona: persona})
			if err != nil {
				return err
			}
			w := bufio.NewWriter(cmd.OutOrStdout())
			for _, c := range contacts {
				fmt.Fprintf(w, "%s\n", c)
			}
			return w.Flush()
		},
	}
	createCmd := &cobra.Command{
		Use:   "create",
		Short: "creates a new contact for a persona with the given name",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	for _, c2 := range []*cobra.Command{
		lsCmd,
		createCmd,
	} {
		c.AddCommand(c2)
	}
	return c
}
