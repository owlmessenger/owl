package owl

import "context"

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
