package contactset

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"github.com/owlmessenger/owl/src/owldag"
	"go.brendoncarroll.net/state/cadata"
	"go.inet256.org/inet256/pkg/inet256"
	"golang.org/x/exp/slices"
)

type (
	PeerID = owldag.PeerID
	State  = gotkv.Root
)

// UID uniquely identifies a contact.
type UID [16]byte

func (uid UID) MarshalJSON() ([]byte, error) {
	return json.Marshal(uid[:])
}

func (uid *UID) UnmarshalJSON(x []byte) error {
	var s []byte
	if err := json.Unmarshal(x, &s); err != nil {
		return err
	}
	*uid = UID{}
	copy(uid[:], s)
	return nil
}

type Contact struct {
	Active []PeerID
	Info   json.RawMessage
}

type Operator struct {
	gotkv *gotkv.Agent
}

func New() Operator {
	kvop := gotkv.NewAgent(1<<12, 1<<20)
	return Operator{gotkv: &kvop}
}

func (o *Operator) New(ctx context.Context, s cadata.Store) (*State, error) {
	return o.gotkv.NewEmpty(ctx, s)
}

func (o *Operator) Create(ctx context.Context, s cadata.Store, x State, name string, ids []PeerID) (*State, error) {
	names, err := o.ListNames(ctx, s, x)
	if err != nil {
		return nil, err
	}
	if slices.Contains(names, name) {
		return nil, fmt.Errorf("address book already has a contact with name %q", name)
	}
	uid := newUID()
	muts := []gotkv.Mutation{
		newPut(appendIndexKey(nil, name), uid[:]),
	}
	for i := range ids {
		muts = append(muts, newPut(appendPeerKey(nil, &uid, ids[i]), nil))
	}
	return o.gotkv.Mutate(ctx, s, x, muts...)
}

func (o *Operator) AddPeers(ctx context.Context, s cadata.Store, x State, name string, ids []PeerID) (*State, error) {
	uid, err := o.Lookup(ctx, s, x, name)
	if err != nil {
		return nil, err
	}
	if uid == nil {
		return nil, fmt.Errorf("contact %q not found", name)
	}
	var muts []gotkv.Mutation
	for i := range ids {
		muts = append(muts, newPut(appendPeerKey(nil, uid, ids[i]), nil))
	}
	return o.gotkv.Mutate(ctx, s, x, muts...)
}

func (o *Operator) RemovePeer(ctx context.Context, s cadata.Store, x State, name string, peer PeerID) (*State, error) {
	uid, err := o.Lookup(ctx, s, x, name)
	if err != nil {
		return nil, err
	}
	return o.gotkv.Delete(ctx, s, x, appendPeerKey(nil, uid, peer))
}

func (o *Operator) PutInfo(ctx context.Context, s cadata.Store, x State, name string, info json.RawMessage) (*State, error) {
	uid, err := o.Lookup(ctx, s, x, name)
	if err != nil {
		return nil, err
	}
	v, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	return o.gotkv.Put(ctx, s, x, appendInfoKey(nil, uid), v)
}

func (o *Operator) Lookup(ctx context.Context, s cadata.Store, x State, name string) (ret *UID, _ error) {
	err := o.gotkv.GetF(ctx, s, x, appendIndexKey(nil, name), func(v []byte) error {
		ret = new(UID)
		copy(ret[:], v)
		return nil
	})
	if errors.Is(err, gotkv.ErrKeyNotFound) {
		return nil, nil
	}
	return ret, err
}

func (o *Operator) Get(ctx context.Context, s cadata.Store, x State, name string) (*Contact, error) {
	uid, err := o.Lookup(ctx, s, x, name)
	if err != nil {
		return nil, err
	}
	if uid == nil {
		return nil, nil
	}
	var peers []PeerID
	var info json.RawMessage
	span := gotkv.PrefixSpan(uid[:])
	if err := o.gotkv.ForEach(ctx, s, x, span, func(ent gotkv.Entry) error {
		switch {
		case bytes.HasPrefix(ent.Key, append(uid[:], "PEER"...)):
			peerID := inet256.AddrFromBytes(ent.Key[len(ent.Key)-len(PeerID{}):])
			peers = append(peers, peerID)
		case bytes.HasPrefix(ent.Key, append(uid[:], "INFO"...)):
			info = append([]byte{}, ent.Key...)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &Contact{
		Active: peers,
		Info:   info,
	}, nil
}

func (o *Operator) ListNames(ctx context.Context, s cadata.Store, x State) (ret []string, _ error) {
	span := gotkv.PrefixSpan(make([]byte, 16))
	if err := o.gotkv.ForEach(ctx, s, x, span, func(ent gotkv.Entry) error {
		// if we OOB here there is a bug in GotKV
		ret = append(ret, string(ent.Key[16:]))
		return nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (o *Operator) Merge(ctx context.Context, s cadata.Store, xs []State) (*State, error) {
	// TODO: handle renames
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

func (o *Operator) WhoIs(ctx context.Context, s cadata.Store, x State, peer PeerID) (string, error) {
	var uid *UID
	span := gotkv.Span{Begin: gotkv.PrefixEnd(make([]byte, 16))}
	if err := o.gotkv.ForEach(ctx, s, x, span, func(ent gotkv.Entry) error {
		if bytes.HasSuffix(ent.Key, peer[:]) {
			var err error
			uid, _, err = splitPeerKey(ent.Key)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return "", err
	}
	if uid == nil {
		return "", nil
	}
	return o.getName(ctx, s, x, uid)
}

func (o *Operator) getName(ctx context.Context, s cadata.Store, x State, uid *UID) (ret string, _ error) {
	err := o.gotkv.ForEach(ctx, s, x, nameIndexSpan(), func(ent gotkv.Entry) error {
		if bytes.Equal(ent.Value, uid[:]) {
			ret = string(ent.Key[16:])
		}
		return nil
	})
	return ret, err
}

func nameIndexSpan() gotkv.Span {
	return gotkv.PrefixSpan(make([]byte, 16))
}

func appendIndexKey(out []byte, name string) []byte {
	zero := UID{}
	out = append(out, zero[:]...)
	out = append(out, name...)
	return out
}

func appendInfoKey(out []byte, uid *UID) []byte {
	out = append(out, uid[:]...)
	out = append(out, "INFO"...)
	return out
}

func appendPeerKey(out []byte, uid *UID, peer owldag.PeerID) []byte {
	out = append(out, uid[:]...)
	out = append(out, "PEER"...)
	out = append(out, peer[:]...)
	return out
}

func splitPeerKey(x []byte) (*UID, *PeerID, error) {
	if len(x) < 16+4+32 {
		return nil, nil, fmt.Errorf("too short to be peer key")
	}
	l := len(x)
	var uid UID
	copy(uid[:], x)
	peer := inet256.AddrFromBytes(x[l-32:])
	return &uid, &peer, nil
}

func newUID() (ret UID) {
	if _, err := io.ReadFull(rand.Reader, ret[:]); err != nil {
		panic(err)
	}
	return ret
}

func newPut(k, v []byte) gotkv.Mutation {
	return gotkv.Mutation{Span: gotkv.SingleKeySpan(k), Entries: []gotkv.Entry{{Key: k, Value: v}}}
}
