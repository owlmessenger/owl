package persona

import (
	"bytes"
	"context"
	"fmt"

	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/memberset"
)

type State struct {
	Members memberset.State `json:"members"`
	Info    glfs.Ref        `json:"info"`
}

type Operator struct {
	members *memberset.Operator
	glfs    glfs.Operator
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
	ref, err := o.glfs.PostTree(ctx, s, glfs.Tree{})
	if err != nil {
		return nil, err
	}
	return &State{
		Members: *membState,
		Info:    *ref,
	}, nil
}

const (
	pathPicture = "PICTURE"
	pathName    = "NAME"
)

func (o *Operator) GetPicture(ctx context.Context, s cadata.Store, x State) ([]byte, error) {
	ref, err := o.glfs.GetAtPath(ctx, s, x.Info, pathPicture)
	if err != nil {
		return nil, err
	}
	return o.glfs.GetBlobBytes(ctx, s, *ref)
}

func (o *Operator) SetPicture(ctx context.Context, s cadata.Store, x State, data []byte) (*State, error) {
	return o.putPath(ctx, s, x, pathPicture, data)
}

func (o *Operator) GetName(ctx context.Context, s cadata.Store, x State) (string, error) {
	ref, err := o.glfs.GetAtPath(ctx, s, x.Info, pathName)
	if err != nil {
		return "", err
	}
	data, err := o.glfs.GetBlobBytes(ctx, s, *ref)
	return string(data), err
}

func (o *Operator) SetName(ctx context.Context, s cadata.Store, x State, name string) (*State, error) {
	return o.putPath(ctx, s, x, pathName, []byte(name))
}

func (o *Operator) putPath(ctx context.Context, s cadata.Store, x State, p string, data []byte) (*State, error) {
	t, err := o.glfs.GetTree(ctx, s, x.Info)
	if err != nil {
		return nil, err
	}
	ref, err := glfs.PostBlob(ctx, s, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	t.Replace(glfs.TreeEntry{
		Name:     p,
		FileMode: 0o755,
		Ref:      *ref,
	})
	return &State{Members: x.Members, Info: *ref}, nil
}

func (o *Operator) Validate(ctx context.Context, s cadata.Store, authorID feeds.PeerID, prev, next State) error {
	if err := o.members.Validate(ctx, s, authorID, prev.Members, next.Members); err != nil {
		return err
	}
	exists, err := o.members.Exists(ctx, s, prev.Members, authorID)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%v may not write to persona feed", authorID)
	}
	return nil
}

func (o *Operator) Merge(ctx context.Context, xs []State) (State, error) {
	panic("")
}
