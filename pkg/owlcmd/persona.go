package owlcmd

import (
	"bufio"

	"github.com/owlmessenger/owl/pkg/owl"
	"github.com/spf13/cobra"
)

func newPersonaCmd(s func() owl.PersonaAPI) *cobra.Command {
	c := &cobra.Command{
		Use:   "persona",
		Short: "manage personas",
	}
	for _, c2 := range []*cobra.Command{
		newPersonaListCmd(s),
		newPersonaCreateCmd(s),
	} {
		c.AddCommand(c2)
	}
	return c
}

func newPersonaListCmd(sf func() owl.PersonaAPI) *cobra.Command {
	return &cobra.Command{
		Use: "list",
		RunE: func(cmd *cobra.Command, args []string) error {
			names, err := sf().ListPersonas(ctx)
			if err != nil {
				return err
			}
			w := bufio.NewWriter(cmd.OutOrStdout())
			for _, name := range names {
				w.WriteString(name + "\n")
			}
			w.WriteString("\n")
			return w.Flush()
		},
	}
}

func newPersonaCreateCmd(sf func() owl.PersonaAPI) *cobra.Command {
	return &cobra.Command{
		Use:  "create",
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			return sf().CreatePersona(ctx, &owl.CreatePersonaReq{Name: name})
		},
	}
}

func newPersonaJoinCmd(sf func() owl.PersonaAPI) *cobra.Command {
	return &cobra.Command{
		Use:  "join",
		Args: cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			idb64s := args[1:]
			var ids []owl.PeerID
			for _, b64s := range idb64s {
				id, err := owl.ParseB64PeerID([]byte(b64s))
				if err != nil {
					return err
				}
				ids = append(ids, id)
			}
			return sf().JoinPersona(ctx, &owl.JoinPersonaReq{Name: name, Peers: ids})
		},
	}
}
