package owlcmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"

	"github.com/owlmessenger/owl/pkg/owl"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func newChatCmd(sf func() owl.API) *cobra.Command {
	cmd := &cobra.Command{
		Use:  "chat",
		Args: cobra.ExactArgs(2),
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if len(args) < 2 {
			return errors.New("must provide persona and channel")
		}
		persona, channel := args[0], args[1]
		cid := owl.ChannelID{Persona: persona, Name: channel}
		out := cmd.OutOrStdout()
		in := cmd.InOrStdin()

		ctx := context.Background()
		eg, ctx := errgroup.WithContext(ctx)
		eg.Go(func() error {
			return owl.WatchChannel(ctx, sf(), cid, func(i owl.EventPath, e owl.Event) error {
				if e.Message == nil {
					return nil
				}
				m := e.Message
				_, err := fmt.Fprintf(out, "%v %v %x %s:\t%s\n", i, m.SentAt, m.FromPeer[:8], m.FromContact, string(m.Body))
				return err
			})
		})
		eg.Go(func() error {
			scn := bufio.NewScanner(in)
			for scn.Scan() {
				scn.Bytes()
			}
			return scn.Err()
		})
		return eg.Wait()
	}
	return cmd
}
