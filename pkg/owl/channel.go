package owl

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-tai64"
	"github.com/jmoiron/sqlx"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/p/directory"
	"github.com/owlmessenger/owl/pkg/p/room"
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
		feedID, err := createFeed(tx, "", func(s cadata.Store) (*room.State, error) {
			op := room.New()
			return op.NewEmpty(ctx, s, peers)
		})
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(`INSERT INTO persona_channels (persona_id, feed_id) VALUES (?, ?)`, ps.id, feedID); err != nil {
			return nil, err
		}
		fstate, err := loadFeed[room.State](tx, feedID)
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
	msgData, err := json.Marshal(string(mp.Body))
	if err != nil {
		return err
	}
	return ps.modifyRoom(ctx, cid.Name, func(s cadata.Store, x room.State, author PeerID) (*room.State, error) {
		op := room.New()
		return op.Append(ctx, s, x, room.Event{
			Author:    author,
			Timestamp: tai64.FromGoTime(time.Now()),
			Data:      msgData,
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
	x, store, err := ps.viewRoom(ctx, cid.Name)
	if err != nil {
		return nil, err
	}
	op := room.New()
	buf := make([]room.Pair, 128)
	n, err := op.Read(ctx, store, *x, room.Path(begin), buf)
	if err != nil {
		return nil, err
	}
	var ret []Pair
	for _, x := range buf[:n] {
		y, err := s.convertRoomEvent(ctx, x.Event)
		if err != nil {
			return nil, err
		}
		ret = append(ret, Pair{
			Path:  EventPath(x.Path),
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

func (s *Server) convertRoomEvent(ctx context.Context, x room.Event) (*Event, error) {
	var y Event
	switch {
	case x.Data != nil:
		y.Message = &Message{
			FromPeer: x.Author,
			Body:     x.Data,
		}
	default:
		return nil, errors.New("empty event")
	}
	return &y, nil
}
