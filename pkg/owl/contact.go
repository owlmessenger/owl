package owl

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"

	"github.com/owlmessenger/owl/pkg/schemes/contactset"
)

var _ ContactAPI = &Server{}

// CreateContact looks up the contactset for persona, then puts an entry (name, contact) in that set.
func (s *Server) CreateContact(ctx context.Context, req *CreateContactReq) error {
	ps, err := s.getPersonaServer(ctx, req.Persona)
	if err != nil {
		return err
	}
	return ps.modifyContactSet(ctx, func(op *contactset.Operator, s cadata.Store, x contactset.State) (*contactset.State, error) {
		return op.Create(ctx, s, x, req.Name, req.Peers)
	})
}

func (s *Server) DeleteContact(ctx context.Context, req *DeleteContactReq) error {
	ps, err := s.getPersonaServer(ctx, req.Persona)
	if err != nil {
		return err
	}
	return ps.modifyContactSet(ctx, func(op *contactset.Operator, s cadata.Store, x contactset.State) (*contactset.State, error) {
		// TODO
		return &x, nil
	})
}

func (s *Server) ListContact(ctx context.Context, req *ListContactReq) ([]string, error) {
	ps, err := s.getPersonaServer(ctx, req.Persona)
	if err != nil {
		return nil, err
	}
	x, store, err := ps.viewContactSet(ctx)
	if err != nil {
		return nil, err
	}
	op := contactset.New()
	return op.ListNames(ctx, store, *x)
}

func (s *Server) GetContact(ctx context.Context, req *GetContactReq) (*Contact, error) {
	ps, err := s.getPersonaServer(ctx, req.Persona)
	if err != nil {
		return nil, err
	}
	x, store, err := ps.viewContactSet(ctx)
	if err != nil {
		return nil, err
	}
	op := contactset.New()
	cinfo, err := op.Get(ctx, store, *x, req.Name)
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
