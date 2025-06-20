package directory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"go.brendoncarroll.net/state"
	"go.brendoncarroll.net/state/cadata"
	"golang.org/x/exp/slices"

	"github.com/owlmessenger/owl/src/internal/slices2"
	"github.com/owlmessenger/owl/src/owldag"
	"github.com/owlmessenger/owl/src/schemes/contactset"
)

type DirectMessage struct {
	Epochs  owldag.IDSet[cadata.ID] `json:"epochs"`
	Members []contactset.UID        `json:"members"`
}

type Value struct {
	DirectMessage *DirectMessage `json:"direct_message"`

	// TODO: support shared directories
	// Directory *owldag.ID
}

type State = gotkv.Root

type Operator struct {
	gotkv *gotkv.Agent
}

func New() Operator {
	gkv := gotkv.NewAgent(1<<12, 1<<16)
	return Operator{
		gotkv: &gkv,
	}
}

func (o *Operator) New(ctx context.Context, s cadata.Store) (*State, error) {
	return o.gotkv.NewEmpty(ctx, s)
}

func (o *Operator) Get(ctx context.Context, s cadata.Store, x State, name string) (*Value, error) {
	var v Value
	if err := o.gotkv.GetF(ctx, s, x, []byte(name), func(data []byte) error {
		return json.Unmarshal(data, &v)
	}); err != nil {
		return nil, err
	}
	return &v, nil
}

func (o *Operator) Exists(ctx context.Context, s cadata.Store, x State, name string) (bool, error) {
	err := o.gotkv.GetF(ctx, s, x, []byte(name), func([]byte) error { return nil })
	if err != nil {
		if errors.Is(err, gotkv.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (o *Operator) Put(ctx context.Context, s cadata.Store, x State, name string, v Value) (*State, error) {
	switch {
	case v.DirectMessage != nil:
		slices.SortFunc(v.DirectMessage.Members, func(a, b contactset.UID) bool {
			return bytes.Compare(a[:], b[:]) < 0
		})
		v.DirectMessage.Members = slices2.DedupSorted(v.DirectMessage.Members)
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return o.gotkv.Put(ctx, s, x, []byte(name), data)
}

func (o *Operator) Delete(ctx context.Context, s cadata.Store, x State, name string) (*State, error) {
	return o.gotkv.Delete(ctx, s, x, []byte(name))
}

func (o *Operator) List(ctx context.Context, s cadata.Store, x State, span state.Span[string]) (ret []string, _ error) {
	if err := o.gotkv.ForEach(ctx, s, x, gotkv.TotalSpan(), func(ent gotkv.Entry) error {
		ret = append(ret, string(ent.Key))
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (o *Operator) Merge(ctx context.Context, s cadata.Store, xs []State) (*State, error) {
	var its []kvstreams.Iterator
	for _, x := range xs {
		its = append(its, o.gotkv.NewIterator(s, x, gotkv.TotalSpan()))
	}
	m := kvstreams.NewMerger(s, its)
	b := o.gotkv.NewBuilder(s)
	if err := gotkv.CopyAll(ctx, b, m); err != nil {
		return nil, err
	}
	return b.Finish(ctx)
}
