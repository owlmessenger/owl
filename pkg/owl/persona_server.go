package owl

import (
	"context"
	"crypto"
	"crypto/x509"
	"errors"
	"io"
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
	"github.com/owlmessenger/owl/pkg/owldag"
	"github.com/owlmessenger/owl/pkg/owlnet"
	"github.com/owlmessenger/owl/pkg/schemes/contactset"
	"github.com/owlmessenger/owl/pkg/schemes/directmsg"
	"github.com/owlmessenger/owl/pkg/schemes/directory"
	"github.com/owlmessenger/owl/pkg/stores"
)

// personaServer is a server managing a single persona
type personaServer struct {
	db      *sqlx.DB
	inet256 inet256.Service
	id      int
	members []PeerID

	contactRep   *replicator[contactset.State]
	directoryRep *replicator[directory.State]
	dmRep        *replicator[directmsg.State]

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
	ps.contactRep = newReplicator(replicatorParams[contactset.State]{
		Context: ctx,
		Scheme:  ps.newContactSetScheme(),
		View: func(ctx context.Context, vid int) (*owldag.DAG[contactset.State], error) {
			return viewDAG(ctx, ps.db, ps.newContactSetScheme(), vid)
		},
		Modify: func(ctx context.Context, vid int, fn func(*owldag.DAG[contactset.State]) error) error {
			return modifyDAG(ctx, ps.db, vid, ps.newContactSetScheme(), fn)
		},
		Push:     ps.pushHeads,
		GetHeads: ps.getHeads,
		GetStore: ps.newPeerStore,
	})
	ps.directoryRep = newReplicator(replicatorParams[directory.State]{
		Context: ctx,
		Scheme:  ps.newDirectoryScheme(),
		View: func(ctx context.Context, vid int) (*owldag.DAG[directory.State], error) {
			return viewDAG(ctx, ps.db, ps.newContactSetScheme(), vid)
		},
		Modify: func(ctx context.Context, vid int, fn func(*owldag.DAG[directory.State]) error) error {
			return modifyDAG(ctx, ps.db, vid, ps.newContactSetScheme(), fn)
		},
		Push:     ps.pushHeads,
		GetHeads: ps.getHeads,
		GetStore: ps.newPeerStore,
	})
	ps.dmRep = newReplicator(replicatorParams[directmsg.State]{
		Context: ctx,
		Scheme:  ps.newDirectMsgScheme(),
		View: func(ctx context.Context, vid int) (*owldag.DAG[directmsg.State], error) {
			return viewDAG(ctx, ps.db, ps.newContactSetScheme(), vid)
		},
		Modify: func(ctx context.Context, vid int, fn func(*owldag.DAG[directmsg.State]) error) error {
			return modifyDAG(ctx, ps.db, vid, ps.newContactSetScheme(), fn)
		},
		Push:     ps.pushHeads,
		GetHeads: ps.getHeads,
		GetStore: ps.newPeerStore,
	})
	ps.eg.Go(func() error { return ps.run(ctx) })
	return ps
}

func (ps *personaServer) run(ctx context.Context) error {
	logctx.Infof(ctx, "persona server started id=%d", ps.id)
	for i := range ps.members {
		if _, err := ps.loadNode(ctx, ps.members[i]); err != nil {
			return err
		}
	}
	return nil
}

func (ps *personaServer) Close() error {
	ps.cf()
	for _, closer := range []io.Closer{
		ps.contactRep,
		ps.directoryRep,
		ps.dmRep,
	} {
		closer.Close()
	}
	return ps.eg.Wait()
}

func (ps *personaServer) readLoop(ctx context.Context, n *owlnet.Node) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return n.BlobPullServer(ctx, &owlnet.BlobPullServer{
			Open: func(from PeerID, epoch owldag.Ref) cadata.Getter {
				volID, _, err := findVolume(ps.db, ps.id, epoch)
				if err != nil {
					logctx.Errorf(ctx, "no volumes for %v", epoch)
					return nil
				}
				s0, sTop, err := lookupVolumeStores(ps.db, volID)
				if err != nil {
					logctx.Errorf(ctx, "no stores for %v", volID)
					return nil
				}
				return stores.Union{newStore(ps.db, s0), newStore(ps.db, sTop)}
			},
		})
	})
	eg.Go(func() error {
		return n.DAGServer(ctx, &owlnet.DAGServer{
			OnPush: func(from owlnet.PeerID, epoch [32]byte, heads []owldag.Head) error {
				volID, scheme, err := findVolume(ps.db, ps.id, epoch)
				if err != nil {
					return err
				}
				return ps.getReplicator(scheme).HandlePush(ctx, from, volID, heads)
			},
			OnGet: func(from owlnet.PeerID, epoch [32]byte) ([]owldag.Head, error) {
				volID, scheme, err := findVolume(ps.db, ps.id, epoch)
				if err != nil {
					return nil, err
				}
				return ps.getReplicator(scheme).HandleGet(ctx, from, volID)
			},
		})
	})
	return eg.Wait()
}

func (ps *personaServer) syncContacts(ctx context.Context) error {
	fid, err := ps.getContactSetVol(ctx)
	if err != nil {
		return err
	}
	return ps.contactRep.Sync(ctx, fid)
}

func (ps *personaServer) syncDirectory(ctx context.Context) error {
	fid, err := ps.getDirectoryVol(ctx)
	if err != nil {
		return err
	}
	return ps.directoryRep.Sync(ctx, fid)
}

func (ps *personaServer) syncChannel(ctx context.Context, name string) error {
	_, err := ps.resolveChannel(ctx, name)
	if err != nil {
		return err
	}
	return nil
}

func (ps *personaServer) modifyContactSet(ctx context.Context, fn func(op *contactset.Operator, s cadata.Store, x contactset.State) (*contactset.State, error)) error {
	volID, err := ps.getContactSetVol(ctx)
	if err != nil {
		return err
	}
	peerID, err := ps.getLocalPeer(ctx)
	if err != nil {
		return err
	}
	privKey, err := ps.getPrivateKey(ctx, peerID)
	if err != nil {
		return err
	}
	op := contactset.New()
	type T = contactset.State
	if err := dbutil.DoTx(ctx, ps.db, func(tx *sqlx.Tx) error {
		return modifyDAGInner(tx, volID, privKey, ps.newContactSetScheme(), func(s cadata.Store, x T) (*T, error) {
			return fn(&op, s, x)
		})
	}); err != nil {
		return err
	}
	ps.contactRep.Notify(volID)
	return nil
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
	peerID, err := ps.getLocalPeer(ctx)
	if err != nil {
		return err
	}
	privKey, err := ps.getPrivateKey(ctx, peerID)
	if err != nil {
		return err
	}
	type T = directory.State
	if err := dbutil.DoTx(ctx, ps.db, func(tx *sqlx.Tx) error {
		return modifyDAGInner(tx, volID, privKey, ps.newDirectoryScheme(), func(s cadata.Store, x T) (*T, error) {
			return fn(s, x)
		})
	}); err != nil {
		return err
	}
	ps.directoryRep.Notify(volID)
	return nil
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
	peerID, err := ps.getLocalPeer(ctx)
	if err != nil {
		return err
	}
	privKey, err := ps.getPrivateKey(ctx, peerID)
	if err != nil {
		return err
	}
	type T = directmsg.State
	if err := dbutil.DoTx(ctx, ps.db, func(tx *sqlx.Tx) error {
		return modifyDAGInner(tx, volID, privKey, ps.newDirectMsgScheme(), func(s cadata.Store, x T) (*T, error) {
			return fn(s, x, peerID)
		})
	}); err != nil {
		return err
	}
	ps.dmRep.Notify(volID)
	return nil
}

func (ps *personaServer) viewDM(ctx context.Context, name string) (*directmsg.State, cadata.Store, error) {
	volID, err := ps.getChannelVol(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	return viewFeedInner[directmsg.State](ctx, ps.db, volID)
}

func (s *personaServer) getNode(ctx context.Context) (*owlnet.Node, error) {
	id, err := s.getLocalPeer(ctx)
	if err != nil {
		return nil, err
	}
	return s.loadNode(ctx, id)
}

func (s *personaServer) loadNode(ctx context.Context, id PeerID) (*owlnet.Node, error) {
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
		vol, _, err := findVolume(s.db, s.id, v.DirectMessage.Epochs...)
		return vol, err
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

func (ps *personaServer) newContactSetScheme() owldag.Scheme[contactset.State] {
	return contactset.NewScheme(ps.members)
}

func (ps *personaServer) newDirectoryScheme() owldag.Scheme[directory.State] {
	return directory.NewScheme(ps.members)
}

func (ps *personaServer) newDirectMsgScheme() owldag.Scheme[directmsg.State] {
	return directmsg.NewScheme(func(ctx context.Context) ([]PeerID, error) {
		return nil, nil
	})
}

func (ps *personaServer) getReplicator(scheme string) interface {
	Notify(int)
	Push(context.Context, int) error
	Pull(context.Context, int) error
	HandlePush(context.Context, PeerID, int, []owldag.Head) error
	HandleGet(context.Context, PeerID, int) ([]owldag.Head, error)
} {
	switch scheme {
	default:
		panic(scheme)
	}
}

func (ps *personaServer) pushHeads(ctx context.Context, dst PeerID, epoch owldag.Ref, heads []owldag.Head) error {
	return nil
}

func (ps *personaServer) getHeads(ctx context.Context, dst PeerID, epoch owldag.Ref) ([]owldag.Head, error) {
	return nil, nil
}

func (ps *personaServer) newPeerStore(peer PeerID) (cadata.Getter, error) {
	node, err := ps.getNode(context.TODO())
	if err != nil {
		return nil, err
	}
	return owlnet.NewStore(node.BlobPullClient(), peer), nil
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
	x, err := owldag.ParseState[T](data)
	if err != nil {
		return nil, nil, err
	}
	return &x.X, s0, nil
}

// modifyFeedInner calls fn to modify the contents of the feed.
func modifyDAGInner[T any](tx *sqlx.Tx, volID int, privKey owldag.PrivateKey, sch owldag.Scheme[T], fn func(s cadata.Store, x T) (*T, error)) error {
	ctx := context.TODO()
	return modifyVolumeTx(tx, volID, func(x []byte, s0, sTop cadata.Store) ([]byte, error) {
		dagState, err := owldag.ParseState[T](x)
		if err != nil {
			return nil, err
		}
		dag := owldag.New(sch, sTop, s0, *dagState)
		if err := dag.Modify(ctx, privKey, func(s cadata.Store, x T) (*T, error) {
			return fn(s0, x)
		}); err != nil {
			return nil, err
		}
		return dag.SaveBytes(), nil
	})
}

func initDAG[T any](tx *sqlx.Tx, volID int, initF func(s cadata.Store) (*T, error)) (*owldag.Ref, error) {
	ctx := context.Background()
	var ret *owldag.Ref
	if err := modifyVolumeTx(tx, volID, func(x []byte, s0, sTop cadata.Store) ([]byte, error) {
		init, err := initF(s0)
		if err != nil {
			return nil, err
		}
		state, err := owldag.InitState(ctx, sTop, init, nil)
		if err != nil {
			return nil, err
		}
		ret = &state.Epochs[0]
		return state.Marshal(), nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func viewDAG[T any](ctx context.Context, db *sqlx.DB, scheme owldag.Scheme[T], volID int) (*owldag.DAG[T], error) {
	data, sTop, s0, err := viewVolume(ctx, db, volID)
	if err != nil {
		return nil, err
	}
	state, err := owldag.ParseState[T](data)
	if err != nil {
		return nil, err
	}
	return owldag.New(scheme, sTop, s0, *state), nil
}

func modifyDAG[T any](ctx context.Context, db *sqlx.DB, volID int, scheme owldag.Scheme[T], fn func(*owldag.DAG[T]) error) error {
	return modifyVolume(ctx, db, volID, func(x []byte, s0 cadata.Store, sTop cadata.Store) ([]byte, error) {
		state, err := owldag.ParseState[T](x)
		if err != nil {
			return nil, err
		}
		dag := owldag.New(scheme, sTop, s0, *state)
		if err := fn(dag); err != nil {
			return nil, err
		}
		return dag.SaveBytes(), nil
	})
}
