package contactset

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotfs"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/owlmessenger/owl/pkg/feeds"
	"golang.org/x/exp/slices"
)

type PeerID = feeds.PeerID

type State = gotfs.Root

type ContactInfo struct {
	Active feeds.IDSet[PeerID] `json:"active"`
}

type Operator struct {
	gotkv *gotkv.Operator
}

func New() Operator {
	kvop := gotkv.NewOperator(1<<12, 1<<20)
	return Operator{gotkv: &kvop}
}

func (o *Operator) New(ctx context.Context, s cadata.Store) (*State, error) {
	return o.gotkv.NewEmpty(ctx, s)
}

func (o *Operator) Create(ctx context.Context, s cadata.Store, x State, name string, ids []PeerID) (*State, error) {
	names, err := o.List(ctx, s, x)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, name) {
		return nil, fmt.Errorf("address book already has a contact with name %q", name)
	}
	return o.Put(ctx, s, x, name, ContactInfo{
		Active: ids,
	})
}

func (o *Operator) Put(ctx context.Context, s cadata.Store, x State, name string, info ContactInfo) (*State, error) {
	v, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	return o.gotkv.Put(ctx, s, x, []byte(name), v)
}

func (o *Operator) Delete(ctx context.Context, s cadata.Store, x State, name string) (*State, error) {
	return o.gotkv.Delete(ctx, s, x, []byte(name))
}

func (o *Operator) Get(ctx context.Context, s cadata.Store, x State, name string) (ret *ContactInfo, _ error) {
	if err := o.gotkv.GetF(ctx, s, x, []byte(name), func(v []byte) error {
		var x ContactInfo
		if err := json.Unmarshal(v, &x); err != nil {
			return err
		}
		ret = &x
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (o *Operator) List(ctx context.Context, s cadata.Store, x State) (ret []string, _ error) {
	if err := o.gotkv.ForEach(ctx, s, x, gotkv.TotalSpan(), func(ent gotkv.Entry) error {
		ret = append(ret, string(ent.Key))
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (o *Operator) Merge(ctx context.Context, s cadata.Store, xs []State) (State, error) {
	var zero State
	var its []kvstreams.Iterator
	for _, x := range xs {
		its = append(its, o.gotkv.NewIterator(s, x, gotkv.TotalSpan()))
	}
	m := kvstreams.NewMerger(s, its)
	b := o.gotkv.NewBuilder(s)
	if err := gotkv.CopyAll(ctx, b, m); err != nil {
		return zero, err
	}
	y, err := b.Finish(ctx)
	if err != nil {
		return zero, err
	}
	return *y, nil
}

func (o *Operator) WhoIs(ctx context.Context, s cadata.Store, x State, peer PeerID) (string, error) {
	var ret string
	err := o.gotkv.ForEach(ctx, s, x, gotkv.TotalSpan(), func(ent gotkv.Entry) error {
		var c ContactInfo
		if err := json.Unmarshal(ent.Value, &c); err != nil {
			return err
		}
		if slices.Contains(c.Active, peer) {
			ret = string(ent.Key)
		}
		return nil
	})
	return ret, err
}
