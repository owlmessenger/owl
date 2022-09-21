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
	"github.com/owlmessenger/owl/pkg/owldag"
	"github.com/owlmessenger/owl/pkg/owlnet"
	"github.com/owlmessenger/owl/pkg/schemes/contactset"
	"github.com/owlmessenger/owl/pkg/schemes/directmsg"
	"github.com/owlmessenger/owl/pkg/schemes/directory"
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
		GetNode: ps.getNode,
		Scheme:  ps.newContactSetScheme(),
	})
	ps.directoryRep = newReplicator(replicatorParams[directory.State]{
		Context: ctx,
		GetNode: ps.getNode,
		Scheme:  ps.newDirectoryScheme(),
	})
	ps.dmRep = newReplicator(replicatorParams[directmsg.State]{
		Context: ctx,
		GetNode: ps.getNode,
		Scheme:  ps.newDirectMsgScheme(),
	})
	ps.eg.Go(func() error { return ps.run(ctx) })
	return ps
}

func (ps *personaServer) run(ctx context.Context) error {
	logctx.Infof(ctx, "persona server started id=%d", ps.id)
	defer logctx.Infof(ctx, "persona server stopped id=%d", ps.id)

	<-ctx.Done()
	return ctx.Err()
}

func (ps *personaServer) Close() error {
	ps.cf()
	return ps.eg.Wait()
}

func (ps *personaServer) readLoop(ctx context.Context, n *owlnet.Node) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return n.BlobPullServer(ctx, &owlnet.BlobPullServer{
			Open: func(peerID PeerID, rootID owldag.Ref) cadata.Getter {
				return nil
			},
		})
	})
	eg.Go(func() error {
		return n.DAGServer(ctx, &owlnet.DAGServer{})
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
	return dbutil.DoTx(ctx, ps.db, func(tx *sqlx.Tx) error {
		return modifyDAGInner(tx, volID, privKey, ps.newContactSetScheme(), func(s cadata.Store, x T) (*T, error) {
			return fn(&op, s, x)
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
	peerID, err := ps.getLocalPeer(ctx)
	if err != nil {
		return err
	}
	privKey, err := ps.getPrivateKey(ctx, peerID)
	if err != nil {
		return err
	}
	type T = directory.State
	return dbutil.DoTx(ctx, ps.db, func(tx *sqlx.Tx) error {
		return modifyDAGInner(tx, volID, privKey, ps.newDirectoryScheme(), func(s cadata.Store, x T) (*T, error) {
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
	peerID, err := ps.getLocalPeer(ctx)
	if err != nil {
		return err
	}
	privKey, err := ps.getPrivateKey(ctx, peerID)
	if err != nil {
		return err
	}
	type T = directmsg.State
	return dbutil.DoTx(ctx, ps.db, func(tx *sqlx.Tx) error {
		return modifyDAGInner(tx, volID, privKey, ps.newDirectMsgScheme(), func(s cadata.Store, x T) (*T, error) {
			return fn(s, x, peerID)
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

func (s *personaServer) getNode(ctx context.Context) (*owlnet.Node, error) {
	id, err := s.getLocalPeer(ctx)
	if err != nil {
		return nil, err
	}
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
