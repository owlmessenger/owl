package owl

import "context"

func WatchChannel(ctx context.Context, s ChannelAPI, cid ChannelID, fn func(i EventPath, e Event) error) error {
	latest, err := s.GetLatest(ctx, cid)
	if err != nil {
		return err
	}
	for {
		events, err := s.Read(ctx, cid, latest, 100)
		if err != nil {
			return err
		}
		index := append(EventPath{}, latest...)
		for _, e := range events {
			if err := fn(index, e); err != nil {
				return err
			}
			index[len(index)]++
		}
	}
}
