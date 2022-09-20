package owldag

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/gotvc/got/pkg/gotkv"
	"github.com/inet256/inet256/pkg/inet256"
)

const headPurpose = "owl/dag/head"

type Head struct {
	Ref       Ref    `json:"ref"`
	PublicKey []byte `json:"pub_key"`
	Sig       []byte `json:"sig"`
}

func NewHead(privKey PrivateKey, ref Ref) (Head, error) {
	sig, err := p2p.Sign(nil, privKey, headPurpose, ref[:])
	if err != nil {
		return Head{}, err
	}
	return Head{
		Ref:       ref,
		PublicKey: inet256.MarshalPublicKey(privKey.Public()),
		Sig:       sig,
	}, nil
}

func (h Head) Verify() error {
	pubKey, err := inet256.ParsePublicKey(h.PublicKey)
	if err != nil {
		return err
	}
	return p2p.Verify(pubKey, headPurpose, h.Ref[:], h.Sig)
}

func (h Head) GetPeerID() (PeerID, error) {
	pubKey, err := inet256.ParsePublicKey(h.PublicKey)
	if err != nil {
		return PeerID{}, err
	}
	return inet256.NewAddr(pubKey), nil
}

func addHead(ctx context.Context, kvop *gotkv.Operator, s cadata.Store, x gotkv.Root, h Head) (*gotkv.Root, error) {
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

func getHeadRef(ctx context.Context, kvop *gotkv.Operator, s cadata.Store, x gotkv.Root, id PeerID) (*Ref, error) {
	var h Head
	if err := kvop.GetF(ctx, s, x, id[:], func(v []byte) error {
		return json.Unmarshal(v, &h)
	}); err != nil {
		if errors.Is(err, gotkv.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	actualID := inet256.NewAddr(h.PublicKey)
	if actualID != id {
		return nil, errors.New("owldag: bad head")
	}
	if err := h.Verify(); err != nil {
		return nil, err
	}
	return &h.Ref, nil
}

func checkReplay(ctx context.Context, kvop *gotkv.Operator, s cadata.Store, headRoot gotkv.Root, h Head) error {
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
