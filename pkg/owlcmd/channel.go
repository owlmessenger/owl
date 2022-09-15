package owlcmd

import (
	"bufio"
	"errors"
	"fmt"

	"github.com/owlmessenger/owl/pkg/owl"
	"github.com/spf13/cobra"
)

func newChannelCmd(sf func() owl.ChannelAPI) *cobra.Command {
	c := &cobra.Command{
		Use: "channel",
	}
	for _, c2 := range []*cobra.Command{
		newChannelCreateCmd(sf),
		newChannelListCmd(sf),
		newChannelReadCmd(sf),
	} {
		c.AddCommand(c2)
	}
	return c
}

func newChannelCreateCmd(sf func() owl.ChannelAPI) *cobra.Command {
	cmd := &cobra.Command{
		Use:  "create <channel-name>",
		Args: cobra.MinimumNArgs(1),
	}
	persona := personaFlag(cmd)
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if *persona == "" {
			return errors.New("must provide persona")
		}
		name := args[0]
		members := args[1:]
		return sf().CreateChannel(ctx, owl.ChannelID{Persona: *persona, Name: name}, owl.ChannelParams{Members: members})
	}
	return cmd
}

func newChannelListCmd(sf func() owl.ChannelAPI) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "lists channels for the persona",
	}
	persona := personaFlag(cmd)
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if *persona == "" {
			return errors.New("must provide persona")
		}
		w := bufio.NewWriter(cmd.OutOrStdout())
		if err := owl.ForEachChannel(ctx, sf(), *persona, func(name string) error {
			_, err := fmt.Fprintf(w, "%s\n", name)
			return err
		}); err != nil {
			return err
		}
		return w.Flush()
	}
	return cmd
}

func newChannelReadCmd(sf func() owl.ChannelAPI) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "read <channel-name>",
		Short: "reads message from the channel",
		Args:  cobra.ExactArgs(1),
	}
	persona := personaFlag(cmd)
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if *persona == "" {
			return errors.New("must provide persona")
		}
		name := args[0]
		cid := owl.ChannelID{Persona: *persona, Name: name}
		w := bufio.NewWriter(cmd.OutOrStdout())
		if err := owl.ForEachEntry(ctx, sf(), cid, func(e owl.Entry) error {
			return printEvent(w, &e)
		}); err != nil {
			return err
		}
		return w.Flush()
	}
	return cmd
}

func personaFlag(cmd *cobra.Command) *string {
	return cmd.Flags().String("persona", "", "--persona <name>")
}
