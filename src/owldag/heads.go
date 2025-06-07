package owldag

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/gotvc/got/pkg/gotkv"
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
	"go.brendoncarroll.net/state/cadata"
	"go.inet256.org/inet256/pkg/inet256"
)

const headPurpose = "owl/dag/head"

// Head is a signed reference to a Node in the DAG
// It represents a single peer's view of the DAG at the time of their last modification.
type Head struct {
	Ref       Ref    `json:"ref"`
	PublicKey []byte `json:"pub_key"`
	Sig       []byte `json:"sig"`
}

func NewHead(privKey PrivateKey, ref Ref) (Head, error) {
	sig := inet256.Sign(nil, privKey, headPurpose, ref[:])
	return Head{
		Ref:       ref,
		PublicKey: inet256.MarshalPublicKey(nil, privKey.Public()),
		Sig:       sig,
	}, nil
}

func (h Head) Verify() error {
	pubKey, err := inet256.ParsePublicKey(h.PublicKey)
	if err != nil {
		return err
	}
	if !inet256.Verify(pubKey, headPurpose, h.Ref[:], h.Sig) {
		return errors.New("invalid signature")
	}
	return nil
}

func (h Head) GetPeerID() (PeerID, error) {
	pubKey, err := inet256.ParsePublicKey(h.PublicKey)
	if err != nil {
		return PeerID{}, err
	}
	return inet256.NewAddr(pubKey), nil
}

func addHead(ctx context.Context, kvop *gotkv.Agent, s cadata.Store, x gotkv.Root, h Head) (*gotkv.Root, error) {
	pubKey, err := inet256.ParsePublicKey(h.PublicKey)
	if err != nil {
		return nil, err
	}
	peerID := inet256.NewAddr(pubKey)
	data, err := json.Marshal(h)
	if err != nil {
		return nil, err
	}
	return kvop.Put(ctx, s, x, peerID[:], data)
}

func getHead(ctx context.Context, kvop *gotkv.Agent, s cadata.Store, x gotkv.Root, id PeerID) (*Head, error) {
	var h Head
	if err := kvop.GetF(ctx, s, x, id[:], func(v []byte) error {
		return json.Unmarshal(v, &h)
	}); err != nil {
		if errors.Is(err, gotkv.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	pubKey, err := inet256.ParsePublicKey(h.PublicKey)
	if err != nil {
		return nil, err
	}
	actualID := inet256.NewAddr(pubKey)
	if actualID != id {
		return nil, errors.New("owldag: bad head")
	}
	if err := h.Verify(); err != nil {
		return nil, err
	}
	return &h, nil
}

func getHeadRef(ctx context.Context, kvop *gotkv.Agent, s cadata.Store, x gotkv.Root, id PeerID) (*Ref, error) {
	h, err := getHead(ctx, kvop, s, x, id)
	if err != nil {
		return nil, err
	}
	return &h.Ref, nil
}

func validateHeads(ctx context.Context, kvop *gotkv.Agent, s cadata.Store, left, right gotkv.Root) error {
	leftIt := kvop.NewIterator(s, left, gotkv.TotalSpan())
	rightIt := kvop.NewIterator(s, right, gotkv.TotalSpan())
	return kvstreams.Diff(ctx, s, leftIt, rightIt, gotkv.TotalSpan(), func(key, lv, rv []byte) error {
		return nil
	})
}

func checkReplay(ctx context.Context, kvop *gotkv.Agent, s cadata.Store, headRoot gotkv.Root, h Head) error {
	peerID, err := h.GetPeerID()
	if err != nil {
		return err
	}
	prevRef, err := getHeadRef(ctx, kvop, s, headRoot, peerID)
	if err != nil {
		return err
	}
	prevNode, err := GetNode[json.RawMessage](ctx, s, *prevRef)
	if err != nil {
		return err
	}
	nextRef := h.Ref
	nextNode, err := GetNode[json.RawMessage](ctx, s, nextRef)
	if err != nil {
		return err
	}
	if nextNode.N <= prevNode.N {
		return ErrReplayedN{Last: prevNode.N, N: nextNode.N, Peer: peerID}
	}
	return nil
}
