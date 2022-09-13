package owl

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-tai64"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/p/channel"
	"github.com/owlmessenger/owl/pkg/p/directory"
)

const MaxChannelPeers = 256

var _ ChannelAPI = &Server{}

// CreateChannel creates a new channel
func (s *Server) CreateChannel(ctx context.Context, cid ChannelID, members []string) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	// collect peer addresses
	var peers []PeerID
	for _, member := range members {
		c, err := s.GetContact(ctx, cid.Persona, member)
		if err != nil {
			return err
		}
		peers = append(peers, c.Addrs...)
	}
	feedID, err := s.channelCtrl.Create(ctx, func(s cadata.Store) (*channel.State, error) {
		op := channel.New()
		return op.NewEmpty(ctx, s, peers)
	})
	if err != nil {
		return err
	}
	var rootID feeds.ID
	if err := s.db.GetContext(ctx, &rootID, `SELECT root FROM feeds WHERE root = ?`, feedID); err != nil {
		return err
	}
	return s.AddChannel(ctx, cid, rootID)
}

// AddChannel adds an existing channel feed identified by cid.
func (s *Server) AddChannel(ctx context.Context, cid ChannelID, fid feeds.ID) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	dirFeed, err := lookupDirectoryFeed(s.db, cid.Persona)
	if err != nil {
		return err
	}
	localID, err := s.GetLocalPeer(ctx, cid.Persona)
	if err != nil {
		return err
	}
	return s.directoryCtrl.Modify(ctx, dirFeed, *localID, func(feed *feeds.Feed[directory.State], s cadata.Store) error {
		op := directory.New()
		return feed.Modify(ctx, *localID, func(prev []feeds.Ref, x directory.State) (*directory.State, error) {
			if exists, err := op.Exists(ctx, s, x, cid.Name); err != nil {
				return nil, err
			} else if exists {
				return nil, errors.New("channel already exists with that name")
			}
			return op.Put(ctx, s, x, cid.Name, directory.Value{Channel: &fid})
		})
	})
}

func (s *Server) DeleteChannel(ctx context.Context, cid ChannelID) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	dirFeed, err := lookupDirectoryFeed(s.db, cid.Persona)
	if err != nil {
		return err
	}
	localID, err := s.GetLocalPeer(ctx, cid.Persona)
	if err != nil {
		return err
	}
	return s.directoryCtrl.Modify(ctx, dirFeed, *localID, func(feed *feeds.Feed[directory.State], s cadata.Store) error {
		op := directory.New()
		return feed.Modify(ctx, *localID, func(prev []feeds.Ref, x directory.State) (*directory.State, error) {
			return op.Delete(ctx, s, x, cid.Name)
		})
	})
}

func (s *Server) ListChannels(ctx context.Context, persona string, span state.Span[string], limit int) ([]string, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	if limit < 1 {
		limit = math.MaxInt
	}
	dirFeed, err := lookupDirectoryFeed(s.db, persona)
	if err != nil {
		return nil, err
	}
	x, store, err := s.directoryCtrl.View(ctx, dirFeed)
	if err != nil {
		return nil, err
	}
	op := directory.New()
	return op.List(ctx, store, *x, span)
}

func (s *Server) GetChannel(ctx context.Context, cid ChannelID) (*ChannelInfo, error) {
	feedID, err := s.resolveChannel(ctx, cid)
	if err != nil {
		return nil, err
	}
	return &ChannelInfo{
		Feed: *feedID,
	}, nil
}

func (s *Server) resolveChannel(ctx context.Context, cid ChannelID) (*feeds.ID, error) {
	dirFeed, err := lookupDirectoryFeed(s.db, cid.Persona)
	if err != nil {
		return nil, err
	}
	state, store, err := s.directoryCtrl.View(ctx, dirFeed)
	if err != nil {
		return nil, err
	}
	op := directory.New()
	v, err := op.Get(ctx, store, *state, cid.Name)
	if err != nil {
		return nil, err
	}
	if v.Channel == nil {
		return nil, errors.New("non-channel directory values not supported")
	}
	return v.Channel, nil
}

func (s *Server) lookupChannelFeed(ctx context.Context, cid ChannelID) (int, error) {
	rootID, err := s.resolveChannel(ctx, cid)
	if err != nil {
		return 0, err
	}
	var feedID int
	if err := s.db.Get(&feedID, `SELECT feeds.id FROM personas
		JOIN persona_channels ON personas.id = persona_channels.persona_id
		JOIN feeds ON feeds.id = persona_channels.feed_id
		WHERE personas.name = ? AND feeds.root = ?
	`, cid.Name, *rootID); err != nil {
		return 0, err
	}
	return feedID, nil
}

func (s *Server) Send(ctx context.Context, cid ChannelID, mp MessageParams) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	feedID, err := s.lookupChannelFeed(ctx, cid)
	if err != nil {
		return err
	}
	localID, err := s.GetLocalPeer(ctx, cid.Persona)
	if err != nil {
		return err
	}
	var msgData []byte // TODO:
	msgData = []byte("hello world " + time.Now().String())

	op := channel.New()
	return s.channelCtrl.Modify(ctx, feedID, *localID, func(feed *feeds.Feed[channel.State], s cadata.Store) error {
		return feed.Modify(ctx, *localID, func(prev []cadata.ID, x channel.State) (*channel.State, error) {
			return op.Append(ctx, s, x, channel.Event{
				From:      *localID,
				Timestamp: tai64.FromGoTime(time.Now()),
				Message:   msgData,
			})
		})
	})
}

func (s *Server) Read(ctx context.Context, cid ChannelID, begin EventPath, limit int) ([]Pair, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 1024
	}
	feedID, err := s.lookupChannelFeed(ctx, cid)
	if err != nil {
		return nil, err
	}
	x, store, err := s.channelCtrl.View(ctx, feedID)
	if err != nil {
		return nil, err
	}
	op := channel.New()
	span := state.TotalSpan[channel.EventID]().WithLowerExcl(channel.EventID(begin))
	buf := make([]channel.Pair, 128)
	n, err := op.Read(ctx, store, *x, span, buf)
	if err != nil {
		return nil, err
	}
	var ret []Pair
	for _, x := range buf[:n] {
		y, err := s.convertEvent(ctx, x.Event)
		if err != nil {
			return nil, err
		}
		ret = append(ret, Pair{
			Path:  EventPath(x.ID),
			Event: y,
		})
	}
	return ret, nil
}

func (s *Server) Wait(ctx context.Context, cid ChannelID, since EventPath) (EventPath, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *Server) Flush(ctx context.Context, cid ChannelID) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	return nil
}

func lookupDirectoryFeed(tx dbutil.Reader, persona string) (int, error) {
	var feedID int
	err := tx.Get(`SELECT directory_feed FROM personas WHERE name = ?`, persona)
	return feedID, err
}

func (s *Server) convertEvent(ctx context.Context, x channel.Event) (*Event, error) {
	var y Event
	switch {
	case x.Message != nil:
	case x.PeerAdded != nil:
	case x.PeerRemoved != nil:
	default:
		return nil, errors.New("empty event")
	}
	return &y, nil
}
