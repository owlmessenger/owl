package owl

import (
	"context"
	"crypto"
	"crypto/x509"
	"errors"
	"sync"

	"github.com/brendoncarroll/go-p2p/p/mbapp"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/inet256/inet256/pkg/p2padapter"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/owlnet"
	"github.com/owlmessenger/owl/pkg/p/contactset"
	"github.com/owlmessenger/owl/pkg/p/directmsg"
	"github.com/owlmessenger/owl/pkg/p/directory"
)

// personaServer is a server managing a single persona
type personaServer struct {
	db      *sqlx.DB
	inet256 inet256.Service
	id      int
	members []PeerID

	mu    sync.Mutex
	nodes map[PeerID]*owlnet.Node
	cf    context.CancelFunc
	eg    errgroup.Group
}

func newPersonaServer(db *sqlx.DB, inetsrv inet256.Service, personaID int, members []PeerID) *personaServer {
	ctx := logctx.WithFmtLogger(context.Background(), logrus.StandardLogger())
	ctx, cf := context.WithCancel(ctx)
	ps := &personaServer{
		db:      db,
		inet256: inetsrv,
		id:      personaID,
		members: members,

		nodes: make(map[inet256.Addr]*owlnet.Node),
		cf:    cf,
	}
	ps.eg.Go(func() error { return ps.run(ctx) })
	return ps
}

func (ps *personaServer) run(ctx context.Context) error {
	// TODO: spawn read loops
	return nil
}

func (ps *personaServer) Close() error {
	ps.cf()
	return ps.eg.Wait()
}

func (ps *personaServer) readLoop(ctx context.Context, n *owlnet.Node) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return n.BlobPullServer(ctx, &owlnet.BlobPullServer{
			Open: func(peerID PeerID, rootID feeds.ID) cadata.Getter {
				return nil
			},
		})
	})
	eg.Go(func() error {
		return n.FeedsServer(ctx, &owlnet.FeedsServer{})
	})
	return eg.Wait()
}

func (ps *personaServer) modifyContactSet(ctx context.Context, fn func(s cadata.Store, x contactset.State) (*contactset.State, error)) error {
	volID, err := ps.getContactSetVol(ctx)
	if err != nil {
		return err
	}
	author, err := ps.getLocalPeer(ctx)
	if err != nil {
		return err
	}
	type T = contactset.State
	return dbutil.DoTx(ctx, ps.db, func(tx *sqlx.Tx) error {
		return modifyFeedInner(tx, volID, author, ps.newContactSetProtocol, func(s cadata.Store, x T) (*T, error) {
			return fn(s, x)
		})
	})
}

func (ps *personaServer) viewContactSet(ctx context.Context) (*contactset.State, cadata.Store, error) {
	volID, err := ps.getContactSetVol(ctx)
	if err != nil {
		return nil, nil, err
	}
	return viewFeedInner[contactset.State](ctx, ps.db, volID)
}

func (ps *personaServer) modifyDirectory(ctx context.Context, fn func(cadata.Store, directory.State) (*directory.State, error)) error {
	volID, err := ps.getDirectoryVol(ctx)
	if err != nil {
		return err
	}
	author, err := ps.getLocalPeer(ctx)
	if err != nil {
		return err
	}
	type T = directory.State
	return dbutil.DoTx(ctx, ps.db, func(tx *sqlx.Tx) error {
		return modifyFeedInner(tx, volID, author, ps.newDirectoryProtocol, func(s cadata.Store, x T) (*T, error) {
			return fn(s, x)
		})
	})
}

func (ps *personaServer) viewDirectory(ctx context.Context) (*directory.State, cadata.Store, error) {
	volID, err := ps.getDirectoryVol(ctx)
	if err != nil {
		return nil, nil, err
	}
	return viewFeedInner[directory.State](ctx, ps.db, volID)
}

func (ps *personaServer) modifyDM(ctx context.Context, name string, fn func(cadata.Store, directmsg.State, PeerID) (*directmsg.State, error)) error {
	volID, err := ps.getChannelVol(ctx, name)
	if err != nil {
		return err
	}
	author, err := ps.getLocalPeer(ctx)
	if err != nil {
		return err
	}
	type T = directmsg.State
	return dbutil.DoTx(ctx, ps.db, func(tx *sqlx.Tx) error {
		return modifyFeedInner(tx, volID, author, ps.newDirectMsgProtocol, func(s cadata.Store, x T) (*T, error) {
			return fn(s, x, author)
		})
	})
}

func (ps *personaServer) viewDM(ctx context.Context, name string) (*directmsg.State, cadata.Store, error) {
	volID, err := ps.getChannelVol(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	return viewFeedInner[directmsg.State](ctx, ps.db, volID)
}

func (s *personaServer) getNode(ctx context.Context, id PeerID) (*owlnet.Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if node, exists := s.nodes[id]; exists {
		return node, nil
	}
	privateKey, err := s.getPrivateKey(ctx, id)
	if err != nil {
		return nil, err
	}
	node, err := s.inet256.Open(ctx, privateKey)
	if err != nil {
		return nil, err
	}
	onode := owlnet.New(mbapp.New(p2padapter.SwarmFromNode(node), 1<<16))
	s.nodes[id] = onode
	s.eg.Go(func() error {
		defer func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.nodes, id)
		}()
		return s.readLoop(ctx, onode)
	})
	return s.nodes[id], nil
}

func (s *personaServer) getLocalPeer(ctx context.Context) (PeerID, error) {
	var x []byte
	err := s.db.GetContext(ctx, &x, `SELECT persona_keys.id FROM persona_keys WHERE persona_id = ?`, s.id)
	return inet256.AddrFromBytes(x), err
}

func (s *personaServer) getPrivateKey(ctx context.Context, localID PeerID) (inet256.PrivateKey, error) {
	var privKeyData []byte
	if err := s.db.GetContext(ctx, &privKeyData, `SELECT private_key FROM persona_keys WHERE persona_id = ? AND id = ?`, s.id, localID[:]); err != nil {
		return nil, err
	}
	privateKey, err := x509.ParsePKCS8PrivateKey(privKeyData)
	if err != nil {
		return nil, err
	}
	return privateKey.(crypto.Signer), nil
}

func (s *personaServer) getDirectoryVol(ctx context.Context) (int, error) {
	var volID int
	err := s.db.GetContext(ctx, &volID, `SELECT volume_id FROM persona_volumes WHERE persona_id = ? AND scheme = ?`, s.id, directoryScheme)
	return volID, err
}

func (s *personaServer) getContactSetVol(ctx context.Context) (int, error) {
	var volID int
	err := s.db.GetContext(ctx, &volID, `SELECT volume_id FROM persona_volumes WHERE persona_id = ? AND scheme = ?`, s.id, contactSetScheme)
	return volID, err
}

func (s *personaServer) getChannelVol(ctx context.Context, name string) (int, error) {
	x, store, err := s.viewDirectory(ctx)
	if err != nil {
		return 0, err
	}
	op := directory.New()
	v, err := op.Get(ctx, store, *x, name)
	if err != nil {
		return 0, err
	}
	switch {
	case v.DirectMessage != nil:
		return findVolume(s.db, s.id, v.DirectMessage.Epochs...)
	default:
		return 0, errors.New("non-dm directory entry not supported")
	}
}

func (ps *personaServer) resolveChannel(ctx context.Context, name string) (*directory.Value, error) {
	state, store, err := ps.viewDirectory(ctx)
	if err != nil {
		return nil, err
	}
	op := directory.New()
	return op.Get(ctx, store, *state, name)
}

func (ps *personaServer) newContactSetProtocol(s cadata.Store) feeds.Protocol[contactset.State] {
	return contactset.NewProtocol(s, ps.members)
}

func (ps *personaServer) newDirectoryProtocol(s cadata.Store) feeds.Protocol[directory.State] {
	return directory.NewProtocol(s, ps.members)
}

func (ps *personaServer) newDirectMsgProtocol(s cadata.Store) feeds.Protocol[directmsg.State] {
	return directmsg.NewProtocol(s, func(ctx context.Context) ([]PeerID, error) {
		return nil, nil
	})
}

func (ps *personaServer) getPeers(ctx context.Context, contact string) ([]PeerID, error) {
	return nil, nil
}

func (ps *personaServer) whoIs(ctx context.Context, peerID PeerID) (string, error) {
	x, store, err := ps.viewContactSet(ctx)
	if err != nil {
		return "", err
	}
	op := contactset.New()
	return op.WhoIs(ctx, store, *x, peerID)
}

func (ps *personaServer) lookupContactUID(ctx context.Context, name string) (*contactset.UID, error) {
	x, store, err := ps.viewContactSet(ctx)
	if err != nil {
		return nil, err
	}
	op := contactset.New()
	return op.Lookup(ctx, store, *x, name)
}

func viewFeedInner[T any](ctx context.Context, db *sqlx.DB, volID int) (*T, cadata.Store, error) {
	data, s0, _, err := viewVolume(ctx, db, volID)
	if err != nil {
		return nil, nil, err
	}
	if len(data) == 0 {
		return nil, s0, nil
	}
	x, err := feeds.ParseState[T](data)
	if err != nil {
		return nil, nil, err
	}
	return &x.State, s0, nil
}

// modifyFeedInner calls fn to modify the contents of the feed.
func modifyFeedInner[T any](tx *sqlx.Tx, volID int, author feeds.PeerID, newProto func(s cadata.Store) feeds.Protocol[T], fn func(s cadata.Store, x T) (*T, error)) error {
	return modifyVolumeTx(tx, volID, func(x []byte, s0, sTop cadata.Store) ([]byte, error) {
		fstate, err := feeds.ParseState[T](x)
		if err != nil {
			return nil, err
		}
		p := newProto(s0)
		f := feeds.New(p, *fstate, sTop)
		if err := f.Modify(context.TODO(), author, func(prev []feeds.Ref, x T) (*T, error) {
			return fn(s0, x)
		}); err != nil {
			return nil, err
		}
		fstateOut := f.GetState()
		return fstateOut.Marshal(), nil
	})
}

func initFeed[T any](tx *sqlx.Tx, volID int, initF func(s cadata.Store) (*T, error)) (*feeds.ID, error) {
	var ret *feeds.ID
	if err := modifyVolumeTx(tx, volID, func(x []byte, s0, sTop cadata.Store) ([]byte, error) {
		init, err := initF(s0)
		if err != nil {
			return nil, err
		}
		fstate, err := feeds.InitialState(context.Background(), sTop, init, nil)
		if err != nil {
			return nil, err
		}
		ret = &fstate.ID
		return fstate.Marshal(), nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}
