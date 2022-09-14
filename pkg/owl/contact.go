package owl

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"

	"github.com/owlmessenger/owl/pkg/p/contactset"
)

var _ ContactAPI = &Server{}

// CreateContact looks up the contactset for persona, then puts an entry (name, contact) in that set.
func (s *Server) CreateContact(ctx context.Context, persona string, name string, c Contact) error {
	ps, err := s.getPersonaServer(ctx, persona)
	if err != nil {
		return err
	}
	return ps.modifyContactSet(ctx, func(s cadata.Store, x contactset.State) (*contactset.State, error) {
		op := contactset.New()
		return op.Create(ctx, s, x, name, c.Addrs)
	})
}

func (s *Server) DeleteContact(ctx context.Context, persona, name string) error {
	ps, err := s.getPersonaServer(ctx, persona)
	if err != nil {
		return err
	}
	return ps.modifyContactSet(ctx, func(s cadata.Store, x contactset.State) (*contactset.State, error) {
		op := contactset.New()
		return op.Delete(ctx, s, x, name)
	})
}

func (s *Server) ListContact(ctx context.Context, persona string) ([]string, error) {
	ps, err := s.getPersonaServer(ctx, persona)
	if err != nil {
		return nil, err
	}
	x, store, err := ps.viewContactSet(ctx)
	if err != nil {
		return nil, err
	}
	op := contactset.New()
	return op.List(ctx, store, *x)
}

func (s *Server) GetContact(ctx context.Context, persona, name string) (*Contact, error) {
	ps, err := s.getPersonaServer(ctx, persona)
	if err != nil {
		return nil, err
	}
	x, store, err := ps.viewContactSet(ctx)
	if err != nil {
		return nil, err
	}
	op := contactset.New()
	cinfo, err := op.Get(ctx, store, *x, name)
	if err != nil {
		return nil, err
	}
	return &Contact{
		Addrs: cinfo.Active,
	}, nil
}

// WhoIs returns the contact name for a peer
func (s *Server) WhoIs(ctx context.Context, persona string, peerID PeerID) (string, error) {
	ps, err := s.getPersonaServer(ctx, persona)
	if err != nil {
		return "", err
	}
	x, store, err := ps.viewContactSet(ctx)
	if err != nil {
		return "", err
	}
	op := contactset.New()
	return op.WhoIs(ctx, store, *x, peerID)
}
