package owl

import (
	"context"

	"github.com/inet256/inet256/pkg/inet256"
)

// func WatchChannel(ctx context.Context, s ChannelAPI, cid ChannelID, fn func(i EventPath, e Event) error) error {
// 	cinfo, err := s.GetChannel(ctx, cid)
// 	if err != nil {
// 		return err
// 	}
// 	for {
// 		events, err := s.Read(ctx, cid, cinfo.Latest, 100)
// 		if err != nil {
// 			return err
// 		}
// 		index := append(EventPath{}, cinfo.Latest...)
// 		for _, e := range events {
// 			if err := fn(index, e); err != nil {
// 				return err
// 			}
// 			index[len(index)]++
// 		}
// 	}
// }

func ForEachChannel(ctx context.Context, s ChannelAPI, persona string, fn func(string) error) error {
	var begin string
	for {
		names, err := s.ListChannels(ctx, persona, begin, 128)
		if err != nil {
			return err
		}
		for _, name := range names {
			if err := fn(name); err != nil {
				return err
			}
			begin = name + "\x00"
		}
		if len(names) == 0 {
			return nil
		}
	}
}

func ForEachEvent(ctx context.Context, s ChannelAPI, cid ChannelID, fn func(p Pair) error) error {
	var begin EventPath
	for {
		pairs, err := s.Read(ctx, cid, begin, 128)
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
	return inet256.ParseAddrB64(x)
}
