package owl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/brendoncarroll/go-state"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-tai64"
	"github.com/jmoiron/sqlx"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/p/directmsg"
	"github.com/owlmessenger/owl/pkg/p/directory"
	"github.com/owlmessenger/owl/pkg/p/room"
)

var _ ChannelAPI = &Server{}

// CreateChannel creates a new channel
func (s *Server) CreateChannel(ctx context.Context, cid ChannelID, p ChannelParams) error {
	if err := s.Init(ctx); err != nil {
		return err
	}

	// collect peer addresses
	ps, err := s.getPersonaServer(ctx, cid.Persona)
	if err != nil {
		return err
	}
	var peers []PeerID
	for _, member := range p.Members {
		c, err := s.GetContact(ctx, cid.Persona, member)
		if err != nil {
			return err
		}
		peers = append(peers, c.Addrs...)
	}

	var (
		newFeed     func(tx *sqlx.Tx) (int, error)
		newDirValue func(feeds.ID) directory.Value
	)
	switch p.Type {
	case DirectMessageV0:
		newFeed = func(tx *sqlx.Tx) (int, error) {
			return createFeed(tx, p.Type, func(s cadata.Store) (*directmsg.State, error) {
				op := directmsg.New()
				return op.NewEmpty(ctx, s)
			})
		}
		newDirValue = func(fid feeds.ID) directory.Value {
			return directory.Value{
				DirectMessage: &directory.DirectMessage{
					Members: p.Members,
					Feed:    fid,
				},
			}
		}
	default:
		return fmt.Errorf("%q is not a valid channel type", p.Type)
	}

	// create the new feed and channel state
	rootID, err := dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*feeds.ID, error) {
		feedID, err := newFeed(tx)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(`INSERT INTO persona_channels (persona_id, feed_id) VALUES (?, ?)`, ps.id, feedID); err != nil {
			return nil, err
		}
		fstate, err := loadFeed[json.RawMessage](tx, feedID)
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
		return op.Put(ctx, s, x, cid.Name, newDirValue(*rootID))
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
		return op.Put(ctx, s, x, cid.Name, directory.Value{
			Room: &directory.Room{Feed: fid},
		})
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
	ps, err := s.getPersonaServer(ctx, cid.Persona)
	if err != nil {
		return nil, err
	}
	v, err := ps.resolveChannel(ctx, cid)
	if err != nil {
		return nil, err
	}
	switch {
	case v.DirectMessage != nil:
		return &ChannelInfo{
			Type: DirectMessageV0,
			Feed: v.DirectMessage.Feed,
		}, nil
	default:
		return nil, errors.New("empty directory entry")
	}
}

func (s *Server) Send(ctx context.Context, cid ChannelID, mp MessageParams) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	ps, err := s.getPersonaServer(ctx, cid.Persona)
	if err != nil {
		return err
	}
	return ps.modifyDM(ctx, cid.Name, func(s cadata.Store, x directmsg.State, author PeerID) (*directmsg.State, error) {
		op := directmsg.New()
		return op.Append(ctx, s, x, directmsg.MessageParams{
			Author:    author,
			Timestamp: tai64.FromGoTime(time.Now()),
			Type:      mp.Type,
			Body:      mp.Body,
		})
	})
}

func (s *Server) Read(ctx context.Context, cid ChannelID, begin EntryPath, limit int) ([]Entry, error) {
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
	v, err := ps.resolveChannel(ctx, cid)
	if err != nil {
		return nil, err
	}
	switch {
	case v.DirectMessage != nil:
		x, store, err := ps.viewDM(ctx, cid.Name)
		if err != nil {
			return nil, err
		}
		op := directmsg.New()
		buf := make([]directmsg.Message, 128)
		n, err := op.Read(ctx, store, *x, room.Path(begin), buf)
		if err != nil {
			return nil, err
		}
		var ret []Entry
		for _, x := range buf[:n] {
			y, err := ps.convertDM(ctx, x)
			if err != nil {
				return nil, err
			}
			y.Path = EntryPath(x.Path)
			ret = append(ret, *y)
		}
		return ret, nil
	default:
		return nil, errors.New("empty directory value")
	}
}

func (s *Server) Wait(ctx context.Context, cid ChannelID, since EntryPath) (EntryPath, error) {
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

func (s *personaServer) convertDM(ctx context.Context, x directmsg.Message) (*Entry, error) {
	name, err := s.whoIs(ctx, x.Author)
	if err != nil {
		// TODO: also handle when we are the sender
		// return nil, err
	}
	return &Entry{
		Message: &Message{
			AuthorPeer:    x.Author,
			AuthorContact: name,
			Timestamp:     x.Timestamp.GoTime(),
			Type:          x.Type,
			Body:          x.Body,
		},
	}, nil
}
