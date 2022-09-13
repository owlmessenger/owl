package owl

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/p/contactset"
)

var _ ContactAPI = &Server{}

// CreateContact looks up the contactset for persona, then puts an entry (name, contact) in that set.
func (s *Server) CreateContact(ctx context.Context, persona string, name string, c Contact) error {
	feedID, err := s.lookupContactSetFeed(s.db, persona)
	if err != nil {
		return err
	}
	peerID, err := s.GetLocalPeer(ctx, persona)
	if err != nil {
		return err
	}
	return s.contactSetCtrl.Modify(ctx, feedID, *peerID, func(feed *feeds.Feed[contactset.State], s cadata.Store) error {
		return feed.Modify(ctx, *peerID, func(prev []cadata.ID, x contactset.State) (*contactset.State, error) {
			op := contactset.New()
			return op.Create(ctx, s, x, name, c.Addrs)
		})
	})
}

func (s *Server) DeleteContact(ctx context.Context, persona, name string) error {
	feedID, err := s.lookupContactSetFeed(s.db, persona)
	if err != nil {
		return err
	}
	peerID, err := s.GetLocalPeer(ctx, persona)
	if err != nil {
		return err
	}
	op := s.getContactSetOp()
	return s.contactSetCtrl.Modify(ctx, feedID, *peerID, func(feed *feeds.Feed[contactset.State], s cadata.Store) error {
		return feed.Modify(ctx, *peerID, func(prev []cadata.ID, x contactset.State) (*contactset.State, error) {
			return op.Delete(ctx, s, x, name)
		})
	})
}

func (s *Server) ListContact(ctx context.Context, persona string) ([]string, error) {
	feedID, err := s.lookupContactSetFeed(s.db, persona)
	if err != nil {
		return nil, err
	}
	state, store, err := s.contactSetCtrl.View(ctx, feedID)
	if err != nil {
		return nil, err
	}
	op := s.getContactSetOp()
	return op.List(ctx, store, *state)
}

func (s *Server) GetContact(ctx context.Context, persona, name string) (*Contact, error) {
	feedID, err := s.lookupContactSetFeed(s.db, persona)
	if err != nil {
		return nil, err
	}
	state, store, err := s.contactSetCtrl.View(ctx, feedID)
	if err != nil {
		return nil, err
	}
	op := s.getContactSetOp()
	cinfo, err := op.Get(ctx, store, *state, name)
	if err != nil {
		return nil, err
	}
	return &Contact{
		Addrs: cinfo.Active,
	}, nil
}

// WhoIs returns the contact name for a peer
func (s *Server) WhoIs(ctx context.Context, persona string, peerID PeerID) (string, error) {
	feedID, err := s.lookupContactSetFeed(s.db, persona)
	if err != nil {
		return "", err
	}
	x, store, err := s.contactSetCtrl.View(ctx, feedID)
	if err != nil {
		return "", err
	}
	op := s.getContactSetOp()
	return op.WhoIs(ctx, store, *x, peerID)
}

func (s *Server) lookupContactSetFeed(tx dbutil.Reader, persona string) (int, error) {
	var feedID int
	err := tx.Get(&feedID, `SELECT contactset_feed FROM personas WHERE name = ?`, persona)
	return feedID, err
}

func (s *Server) getContactSetOp() contactset.Operator {
	return contactset.New()
}
