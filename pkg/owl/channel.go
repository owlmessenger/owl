package owl

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jmoiron/sqlx"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/tai64"

	"github.com/owlmessenger/owl/pkg/cflog"
	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/owldag"
	"github.com/owlmessenger/owl/pkg/schemes/contactset"
	"github.com/owlmessenger/owl/pkg/schemes/directmsg"
	"github.com/owlmessenger/owl/pkg/schemes/directory"
	"github.com/owlmessenger/owl/pkg/slices2"
)

var _ ChannelAPI = &Server{}

// CreateChannel creates a new channel
func (s *Server) CreateChannel(ctx context.Context, req *CreateChannelReq) error {
	if err := s.Init(ctx); err != nil {
		return err
	}

	ps, err := s.getPersonaServer(ctx, req.Persona)
	if err != nil {
		return err
	}
	// collect peer addresses
	contactUIDs, err := slices2.ParMap(req.Members, func(x string) (contactset.UID, error) {
		id, err := ps.lookupContactUID(ctx, x)
		if err != nil {
			return contactset.UID{}, nil
		}
		return *id, nil
	})
	if err != nil {
		return err
	}

	// create the new feed and channel state
	dirValue, err := dbutil.DoTx1(ctx, s.db, func(tx *sqlx.Tx) (*directory.Value, error) {
		volID, err := createVolume(tx)
		if err != nil {
			return nil, err
		}
		if err := assocVol(tx, ps.id, volID, req.Scheme); err != nil {
			return nil, err
		}
		switch req.Scheme {
		case DirectMessageV0:
			fid, err := initDAG(tx, volID, func(s cadata.Store) (*directmsg.State, error) {
				op := directmsg.New()
				return op.NewEmpty(ctx, s)
			})
			if err != nil {
				return nil, err
			}
			return &directory.Value{
				DirectMessage: &directory.DirectMessage{
					Epochs:  owldag.NewIDSet(*fid),
					Members: contactUIDs,
				},
			}, nil
		default:
			return nil, fmt.Errorf("%q is not a valid channel scheme", req.Scheme)
		}
	})
	if err != nil {
		return err
	}
	// add the channel to the persona's directory
	return ps.modifyDirectory(ctx, func(s cadata.Store, x directory.State) (*directory.State, error) {
		op := directory.New()
		return op.Put(ctx, s, x, req.Name, *dirValue)
	})
}

// JoinChannel adds an existing channel feed identified by cid.
func (s *Server) JoinChannel(ctx context.Context, req *JoinChannelReq) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	ps, err := s.getPersonaServer(ctx, req.Persona)
	if err != nil {
		return err
	}
	return ps.modifyDirectory(ctx, func(s cadata.Store, x directory.State) (*directory.State, error) {
		op := directory.New()
		if exists, err := op.Exists(ctx, s, x, req.Name); err != nil {
			return nil, err
		} else if exists {
			return nil, errors.New("channel already exists with that name")
		}
		return op.Put(ctx, s, x, req.Name, directory.Value{
			DirectMessage: &directory.DirectMessage{
				Epochs:  owldag.NewIDSet(req.Epoch),
				Members: nil, // TODO: lookup contact UIDs
			},
		})
	})
}

func (s *Server) DeleteChannel(ctx context.Context, cid *ChannelID) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	ps, err := s.getPersonaServer(ctx, cid.Persona)
	if err != nil {
		return err
	}
	volID, err := ps.getChannelVol(ctx, cid.Name)
	if err != nil {
		return err
	}
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		return dropVolume(tx, volID)
	}); err != nil {
		return err
	}
	return ps.modifyDirectory(ctx, func(s cadata.Store, x directory.State) (*directory.State, error) {
		op := directory.New()
		return op.Delete(ctx, s, x, cid.Name)
	})
}

func (s *Server) ListChannels(ctx context.Context, req *ListChannelReq) ([]string, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit < 1 {
		limit = math.MaxInt
	}
	span := state.TotalSpan[string]().WithLowerIncl(req.Begin)
	ps, err := s.getPersonaServer(ctx, req.Persona)
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

func (s *Server) GetChannel(ctx context.Context, cid *ChannelID) (*ChannelInfo, error) {
	ps, err := s.getPersonaServer(ctx, cid.Persona)
	if err != nil {
		return nil, err
	}
	v, err := ps.resolveChannel(ctx, cid.Name)
	if err != nil {
		return nil, err
	}
	switch {
	case v.DirectMessage != nil:
		return &ChannelInfo{
			Scheme: DirectMessageV0,
		}, nil
	default:
		return nil, errors.New("empty directory entry")
	}
}

func (s *Server) Send(ctx context.Context, req *SendReq) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	ps, err := s.getPersonaServer(ctx, req.Persona)
	if err != nil {
		return err
	}
	return ps.modifyDM(ctx, req.Name, func(s cadata.Store, x directmsg.State, author PeerID) (*directmsg.State, error) {
		op := directmsg.New()
		return op.Append(ctx, s, x, directmsg.MessageParams{
			Author:    author,
			Timestamp: tai64.FromGoTime(time.Now()),
			Type:      req.Params.Type,
			Body:      req.Params.Body,
		})
	})
}

func (s *Server) Read(ctx context.Context, req *ReadReq) ([]Entry, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 1024
	}
	ps, err := s.getPersonaServer(ctx, req.Persona)
	if err != nil {
		return nil, err
	}
	v, err := ps.resolveChannel(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	switch {
	case v.DirectMessage != nil:
		x, store, err := ps.viewDM(ctx, req.Name)
		if err != nil {
			return nil, err
		}
		op := directmsg.New()
		buf := make([]directmsg.Message, 128)
		n, err := op.Read(ctx, store, *x, cflog.Path(req.Begin), buf)
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
