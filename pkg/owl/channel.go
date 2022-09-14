package owl

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-tai64"
	"github.com/jmoiron/sqlx"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/p/channel"
	"github.com/owlmessenger/owl/pkg/p/directory"
)

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
	ps, err := s.getPersonaServer(ctx, cid.Persona)
	if err != nil {
		return err
	}
	// create the new feed and channel state
	rootID, err := dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*feeds.ID, error) {
		feedID, err := createFeed(tx, "", func(s cadata.Store) (*channel.State, error) {
			op := channel.New()
			return op.NewEmpty(ctx, s, peers)
		})
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(`INSERT INTO persona_channels (persona_id, feed_id) VALUES (?, ?)`, ps.id, feedID); err != nil {
			return nil, err
		}
		fstate, err := loadFeed[channel.State](tx, feedID)
		if err != nil {
			return nil, err
		}
		return &fstate.ID, nil
	})
	if err != nil {
		return err
	}
	// add the channel to the persona's directory
	return ps.modifyDirectory(ctx, func(s cadata.Store, x directory.State) (*directory.State, error) {
		op := directory.New()
		return op.Put(ctx, s, x, cid.Name, directory.Value{
			Channel: rootID,
		})
	})
}

// JoinChannel adds an existing channel feed identified by cid.
func (s *Server) JoinChannel(ctx context.Context, cid ChannelID, fid feeds.ID) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	ps, err := s.getPersonaServer(ctx, cid.Persona)
	if err != nil {
		return err
	}
	return ps.modifyDirectory(ctx, func(s cadata.Store, x directory.State) (*directory.State, error) {
		op := directory.New()
		if exists, err := op.Exists(ctx, s, x, cid.Name); err != nil {
			return nil, err
		} else if exists {
			return nil, errors.New("channel already exists with that name")
		}
		return op.Put(ctx, s, x, cid.Name, directory.Value{Channel: &fid})
	})
}

func (s *Server) DeleteChannel(ctx context.Context, cid ChannelID) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	ps, err := s.getPersonaServer(ctx, cid.Persona)
	if err != nil {
		return err
	}
	return ps.modifyDirectory(ctx, func(s cadata.Store, x directory.State) (*directory.State, error) {
		op := directory.New()
		return op.Delete(ctx, s, x, cid.Name)
	})
}

func (s *Server) ListChannels(ctx context.Context, persona string, span state.Span[string], limit int) ([]string, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	if limit < 1 {
		limit = math.MaxInt
	}
	ps, err := s.getPersonaServer(ctx, persona)
	if err != nil {
		return nil, err
	}
	x, store, err := ps.viewDirectory(ctx)
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
	ps, err := s.getPersonaServer(ctx, cid.Persona)
	if err != nil {
		return nil, err
	}
	state, store, err := ps.viewDirectory(ctx)
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

func (s *Server) Send(ctx context.Context, cid ChannelID, mp MessageParams) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	ps, err := s.getPersonaServer(ctx, cid.Persona)
	if err != nil {
		return err
	}
	var msgData []byte = []byte("hello world " + time.Now().String())
	return ps.modifyChannel(ctx, cid.Name, func(s cadata.Store, x channel.State, author PeerID) (*channel.State, error) {
		op := channel.New()
		return op.Append(ctx, s, x, channel.Event{
			From:      author,
			Timestamp: tai64.FromGoTime(time.Now()),
			Message:   msgData,
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
	ps, err := s.getPersonaServer(ctx, cid.Persona)
	if err != nil {
		return nil, err
	}
	x, store, err := ps.viewChannel(ctx, cid.Name)
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
