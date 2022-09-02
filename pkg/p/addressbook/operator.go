package addressbook

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/owlmessenger/owl/pkg/feeds"
)

type PersonaID = cadata.ID

type State = gotfs.Root

type Operator struct {
	gotfs *gotfs.Operator
}

func (o *Operator) New(ctx context.Context, s cadata.Store) (*State, error) {
	return o.gotfs.NewEmpty(ctx, s)
}

func (o *Operator) AddContact(ctx context.Context, s cadata.Store, x State, name string, persona PersonaID) (*State, error) {
	// TODO: add directory for contact by personaID
	panic("")
}

func (o *Operator) RemoveContact(ctx context.Context, s cadata.Store, x State, name string, persona PersonaID) (*State, error) {
	panic("")
}

func (o *Operator) ListIDs(ctx context.Context, s cadata.Store, x State) ([]feeds.NodeID, error) {
	panic("")
}

func (o *Operator) ListNames(ctx context.Context, s cadata.Store, x State) ([]string, error) {
	panic("")
}
