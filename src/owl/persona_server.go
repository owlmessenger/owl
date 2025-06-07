package owl

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/jmoiron/sqlx"
	"go.brendoncarroll.net/p2p/p/mbapp"
	"go.brendoncarroll.net/state/cadata"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.inet256.org/inet256/pkg/inet256"
	"go.inet256.org/inet256/pkg/p2padapter"
	"go.inet256.org/inet256/pkg/serde"
	"golang.org/x/sync/errgroup"

	"github.com/owlmessenger/owl/src/dbutil"
	"github.com/owlmessenger/owl/src/owldag"
	"github.com/owlmessenger/owl/src/owlnet"
	"github.com/owlmessenger/owl/src/schemes/contactset"
	"github.com/owlmessenger/owl/src/schemes/directmsg"
	"github.com/owlmessenger/owl/src/schemes/directory"
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
	ctx := context.Background()
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
			if err := ps.db.GetContext(ctx, &row, `SELECT cell, store_0, store_top, pv.scheme FROM volumes
				JOIN persona_volumes as pv ON pv.volume_id = volumes.id
				WHERE id = ?`, vid); err != nil {
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
			if err := ps.db.GetContext(ctx, &row, `SELECT cell, store_0, store_top, pv.scheme FROM volumes
				JOIN persona_volumes as pv ON pv.volume_id = volumes.id
				WHERE id = ?`, vid); err != nil {
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
		return n.DAGServer(ctx, &owlnet.DAGServer{
			OnPushHeads: func(from owlnet.PeerID, volID int, srcDAG owlnet.Handle, heads []owldag.Head) error {
				return ps.rep.HandlePush(ctx, from, volID, srcDAG, heads)
			},
			OnGetHeads: func(from owlnet.PeerID, volID int) ([]owldag.Head, error) {
				return ps.rep.HandleGet(ctx, from, volID)
			},
			OnList: func(from owlnet.PeerID, schemeIs string, mustContain []owldag.Ref) ([]owlnet.LocalDAGInfo, error) {
				return ps.listDAGs(ctx, from, schemeIs, mustContain)
			},
		})
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
			// TODO: need to look up the contacts for this message and return all their peers.
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
	privateKey, err := serde.ParsePrivateKey(privKeyData)
	if err != nil {
		return nil, err
	}
	return privateKey, nil
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

func (ps *personaServer) listDAGs(ctx context.Context, peer PeerID, schemeIs string, mustContain []owldag.Ref) (ret []owlnet.LocalDAGInfo, _ error) {
	err := volForEach(ctx, ps.db, ps.id, func(vid int, schemeName string) error {
		if schemeIs != "" && schemeName != schemeIs {
			return nil
		}
		scheme := ps.newScheme(schemeName)
		dag, err := viewDAG(ctx, ps.db, scheme, vid)
		if err != nil {
			return err
		}
		if len(mustContain) != 0 {
			if yes, err := dag.ContainsAll(ctx, mustContain); err != nil {
				return err
			} else if !yes {
				return nil
			}
		}
		epoch, err := dag.GetEpoch(ctx)
		if err != nil {
			return err
		}
		if yes, err := dag.CanRead(ctx, peer); err != nil {
			return err
		} else if yes {
			ret = append(ret, owlnet.LocalDAGInfo{
				ID:     vid,
				Scheme: schemeName,
				Epochs: []owldag.Ref{*epoch},
			})
		}
		return nil
	})
	return ret, err
}
