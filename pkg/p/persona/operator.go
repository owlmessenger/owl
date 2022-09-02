package persona

import (
	"context"

	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/owlmessenger/owl/pkg/memberset"
)

type State struct {
	Members memberset.State `json:"members"`
	Info    glfs.Ref        `json:"info"`
}

type Operator struct {
	members *memberset.Operator
	glfs    *glfs.Operator
}

func New(kvop *gotkv.Operator) *Operator {
	mo := memberset.New(kvop)
	return &Operator{
		members: &mo,
		glfs:    glfs.NewOperator(),
	}
}

func (o *Operator) New(ctx context.Context, s cadata.Store, peers []memberset.PeerID) (*State, error) {
	membState, err := o.members.NewEmpty(ctx, s, peers)
	if err != nil {
		return nil, err
	}
	return &State{
		Members: *membState,
	}, nil
}
