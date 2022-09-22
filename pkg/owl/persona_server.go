package owl

import (
	"context"
	"crypto"
	"crypto/x509"
	"encoding/json"
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

	rep *replicator

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
	ps.rep = newReplicator(replicatorParams{
		Context: ctx,
		View: func(ctx context.Context, vid int) (string, *owldag.DAG[json.RawMessage], error) {
			var row struct {
				Data   []byte `db:"cell"`
				S0     int    `db:"store_0"`
				STop   int    `db:"store_top"`
				Scheme string `db:"scheme"`
			}
			if err := ps.db.GetContext(ctx, &row, `SELECT cell, store_0, store_top, scheme FROM volumes WHERE id = ?`, vid); err != nil {
				return "", nil, err
			}
			state, err := owldag.ParseState[json.RawMessage](row.Data)
			if err != nil {
				return "", nil, err
			}
			scheme := ps.newScheme(row.Scheme)
			return "", owldag.New(scheme, newStore(ps.db, row.STop), newStore(ps.db, row.S0), *state), nil
		},
		Modify: func(ctx context.Context, vid int, fn func(dag *owldag.DAG[json.RawMessage]) error) error {
			var row struct {
				Data   []byte `db:"cell"`
				S0     int    `db:"store_0"`
				STop   int    `db:"store_top"`
				Scheme string `db:"scheme"`
			}
			if err := ps.db.GetContext(ctx, &row, `SELECT cell, store_0, store_top, scheme FROM volumes WHERE id = ?`, vid); err != nil {
				return err
			}
			state, err := owldag.ParseState[json.RawMessage](row.Data)
			if err != nil {
				return err
			}
			scheme := ps.newScheme(row.Scheme)
			dag := owldag.New(scheme, newStore(ps.db, row.STop), newStore(ps.db, row.S0), *state)
			if err := fn(dag); err != nil {
				return err
			}
			_, err = ps.db.ExecContext(ctx, `UPDATE volumes SET cell = ? WHERE id = ?`, dag.SaveBytes(), vid)
			return err
		},
		GetNode: ps.getNode,
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
	ps.rep.Close()
	return ps.eg.Wait()
}

func (ps *personaServer) readLoop(ctx context.Context, n *owlnet.Node) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return n.BlobPullServer(ctx, &owlnet.BlobPullServer{
			Open: func(from PeerID, volID uint32, sid uint8) cadata.Getter {
				s0, sTop, err := lookupVolumeStores(ps.db, int(volID))
				if err != nil {
					logctx.Errorf(ctx, "no stores for %v", volID)
					return nil
				}
				switch sid {
				case 0:
					return newStore(ps.db, s0)
				case 255:
					return newStore(ps.db, sTop)
				default:
					logctx.Errorf(ctx, "volume has no store %v", sid)
					return nil
				}
			},
		})
	})
	eg.Go(func() error {
		return n.DAGServer(ctx, &owlnet.DAGServer{})
	})
	return eg.Wait()
}

func (ps *personaServer) newScheme(name string) owldag.Scheme[json.RawMessage] {
	switch name {
	case contactSetScheme:
		return wrapScheme[contactset.State](contactset.NewScheme(ps.members))
	case directoryScheme:
		return wrapScheme[contactset.State](contactset.NewScheme(ps.members))
	case DirectMessageV0:
		return wrapScheme[directmsg.State](directmsg.NewScheme(func(ctx context.Context) ([]owldag.PeerID, error) {
			return nil, nil
		}))
	default:
		panic(name)
	}
}

func (ps *personaServer) syncContacts(ctx context.Context) error {
	fid, err := ps.getContactSetVol(ctx)
	if err != nil {
		return err
	}
	return ps.rep.Sync(ctx, fid)
}

func (ps *personaServer) syncDirectory(ctx context.Context) error {
	fid, err := ps.getDirectoryVol(ctx)
	if err != nil {
		return err
	}
	return ps.rep.Sync(ctx, fid)
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
		return modifyDAGInner(tx, volID, privKey, func(s cadata.Store, x T) (*T, error) {
			return fn(&op, s, x)
		})
	}); err != nil {
		return err
	}
	ps.rep.Notify(volID)
	return nil
}

func (ps *personaServer) viewContactSet(ctx context.Context) (*contactset.State, cadata.Store, error) {
	volID, err := ps.getContactSetVol(ctx)
	if err != nil {
		return nil, nil, err
	}
	return viewDAGInner[contactset.State](ctx, ps.db, volID)
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
		return modifyDAGInner(tx, volID, privKey, func(s cadata.Store, x T) (*T, error) {
			return fn(s, x)
		})
	}); err != nil {
		return err
	}
	ps.rep.Notify(volID)
	return nil
}

func (ps *personaServer) viewDirectory(ctx context.Context) (*directory.State, cadata.Store, error) {
	volID, err := ps.getDirectoryVol(ctx)
	if err != nil {
		return nil, nil, err
	}
	return viewDAGInner[directory.State](ctx, ps.db, volID)
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
		return modifyDAGInner(tx, volID, privKey, func(s cadata.Store, x T) (*T, error) {
			return fn(s, x, peerID)
		})
	}); err != nil {
		return err
	}
	ps.rep.Notify(volID)
	return nil
}

func (ps *personaServer) viewDM(ctx context.Context, name string) (*directmsg.State, cadata.Store, error) {
	volID, err := ps.getChannelVol(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	return viewDAGInner[directmsg.State](ctx, ps.db, volID)
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
