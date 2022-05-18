package owl

import (
	"context"

	"github.com/inet256/inet256/pkg/inet256"
)

func WatchChannel(ctx context.Context, s ChannelAPI, cid ChannelID, fn func(i EventPath, e Event) error) error {
	cinfo, err := s.GetChannel(ctx, cid)
	if err != nil {
		return err
	}
	for {
		events, err := s.Read(ctx, cid, cinfo.Latest, 100)
		if err != nil {
			return err
		}
		index := append(EventPath{}, cinfo.Latest...)
		for _, e := range events {
			if err := fn(index, e); err != nil {
				return err
			}
			index[len(index)]++
		}
	}
}

func ParseB64PeerID(x []byte) (PeerID, error) {
	return inet256.ParseAddrB64(x)
}
