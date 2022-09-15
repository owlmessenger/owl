package owlcmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/owlmessenger/owl/pkg/owl"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func newChatCmd(sf func() owl.API) *cobra.Command {
	cmd := &cobra.Command{
		Use:  "chat <channel-name>",
		Args: cobra.ExactArgs(1),
	}
	persona := personaFlag(cmd)
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if *persona == "" {
			return errors.New("must provide persona")
		}
		channel := args[0]
		cid := owl.ChannelID{Persona: *persona, Name: channel}
		out := cmd.OutOrStdout()
		in := cmd.InOrStdin()
		s := sf()

		ctx := context.Background()
		eg, ctx := errgroup.WithContext(ctx)
		eg.Go(func() error {
			log.Println(cid, out)
			return nil
			// return owl.WatchChannel(ctx, sf(), cid, func(i owl.EventPath, e owl.Event) error {
			// 	return
			// })
		})
		eg.Go(func() error {
			scn := bufio.NewScanner(in)
			for scn.Scan() {
				if err := s.Send(ctx, cid, owl.NewText(scn.Text())); err != nil {
					log.Println("error:", err)
				}
			}
			return scn.Err()
		})
		return eg.Wait()
	}
	return cmd
}

func printEvent(w *bufio.Writer, p owl.EventPath, e *owl.Event) error {
	var err error
	switch {
	case e.PeerAdded != nil:
		pa := e.PeerAdded
		_, err = fmt.Fprintf(w, " PEER %v ADDED BY %v\n", pa.Peer, pa.AddedBy)
	case e.Message != nil:
		m := e.Message
		sentAt := m.SentAt.Truncate(time.Second).Local().Format(time.Stamp)
		_, err = fmt.Fprintf(w, "%8v %8v %x %s: %s\n", p, sentAt, m.FromPeer[:4], m.FromContact, string(m.Body))
	default:
		log.Println("empty event")
		return nil
	}
	return err
}
