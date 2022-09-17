package owl

import (
	"context"

	"github.com/brendoncarroll/go-state"
	"github.com/inet256/inet256/pkg/inet256"
)

// func WatchChannel(ctx context.Context, s ChannelAPI, cid ChannelID, fn func(i EntryPath, e Entry) error) error {
// 	cinfo, err := s.GetChannel(ctx, cid)
// 	if err != nil {
// 		return err
// 	}
// 	for {
// 		events, err := s.Read(ctx, cid, cinfo.Latest, 100)
// 		if err != nil {
// 			return err
// 		}
// 		index := append(EntryPath{}, cinfo.Latest...)
// 		for _, e := range events {
// 			if err := fn(index, e); err != nil {
// 				return err
// 			}
// 			index[len(index)]++
// 		}
// 	}
// }

func ForEachChannel(ctx context.Context, s ChannelAPI, persona string, fn func(string) error) error {
	span := state.TotalSpan[string]()
	for {
		begin, _ := span.LowerBound()
		names, err := s.ListChannels(ctx, &ListChannelReq{
			Persona: persona,
			Begin:   begin,
			Limit:   128,
		})
		if err != nil {
			return err
		}
		for _, name := range names {
			if err := fn(name); err != nil {
				return err
			}
			span = span.WithLowerExcl(name)
		}
		if len(names) == 0 {
			return nil
		}
	}
}

func ForEachEntry(ctx context.Context, s ChannelAPI, cid ChannelID, fn func(Entry) error) error {
	var begin EntryPath
	for {
		pairs, err := s.Read(ctx, &ReadReq{
			Persona: cid.Persona,
			Name:    cid.Name,

			Begin: begin,
			Limit: 128,
		})
		if err != nil {
			return err
		}
		if len(pairs) == 0 {
			return nil
		}
		for _, pair := range pairs {
			if err := fn(pair); err != nil {
				return err
			}
			begin = pair.Path
			begin[len(begin)-1]++
		}
	}
}

func ParseB64PeerID(x []byte) (PeerID, error) {
	return inet256.ParseAddrBase64(x)
}
