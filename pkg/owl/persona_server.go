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
	"github.com/owlmessenger/owl/pkg/p/channel"
	"github.com/owlmessenger/owl/pkg/p/contactset"
	"github.com/owlmessenger/owl/pkg/p/directory"
)

// personaServer is a server managing a single persona
type personaServer struct {
	db      *sqlx.DB
	inet256 inet256.Service
	id      int
	members []PeerID

	contactSetCtrl *feedController[contactset.State]
	directoryCtrl  *feedController[directory.State]
	channelCtrl    *feedController[channel.State]

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
	ps.contactSetCtrl = newFeedController(fcParams[contactset.State]{
		Context:      ctx,
		DB:           db,
		GetNode:      ps.getNode,
		ProtocolName: "contactset@v0",
		GetProtocol:  ps.newContactSetProtocol,
	})
	ps.directoryCtrl = newFeedController(fcParams[directory.State]{
		Context:      ctx,
		DB:           db,
		GetNode:      ps.getNode,
		ProtocolName: "directory@v0",
		GetProtocol:  ps.newDirectoryProtocol,
	})
	ps.channelCtrl = newFeedController(fcParams[channel.State]{
		Context:      ctx,
		DB:           db,
		GetNode:      ps.getNode,
		ProtocolName: "channel@v0",
		GetProtocol:  ps.newChannelProtocol,
	})
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
				feedID, err := lookupFeed(ps.db, ps.id, rootID)
				if err != nil {
					logctx.Errorf(ctx, "%v", err)
					return nil
				}
				allow, err := ps.channelCtrl.CanRead(ctx, feedID)
				if err != nil {
					logctx.Errorf(ctx, "opening store: %v", err)
					return nil
				}
				if !allow {
					return nil
				}
				storeID, err := lookupFeedStore(ps.db, feedID)
				if err != nil {
					logctx.Errorf(ctx, "%v", err)
					return nil
				}
				return newStore(ps.db, storeID)
			},
		})
	})
	eg.Go(func() error {
		return n.FeedsServer(ctx, &owlnet.FeedsServer{})
	})
	return eg.Wait()
}

func (ps *personaServer) modifyContactSet(ctx context.Context, fn func(s cadata.Store, x contactset.State) (*contactset.State, error)) error {
	type T = contactset.State
	feedID, err := ps.getContactSetFeed(ctx)
	if err != nil {
		return err
	}
	peerID, err := ps.getLocalPeer(ctx)
	if err != nil {
		return err
	}
	return dbutil.DoTx(ctx, ps.db, func(tx *sqlx.Tx) error {
		return modifyFeed(tx, feedID, peerID, func(x feeds.State[T], sf cadata.Store, s0 cadata.Store) (*feeds.State[T], error) {
			feed := feeds.New(ps.newContactSetProtocol(s0), x, sf)
			if err := feed.Modify(ctx, peerID, func(prev []feeds.Ref, x directory.State) (*directory.State, error) {
				return fn(s0, x)
			}); err != nil {
				return nil, err
			}
			y := feed.GetState()
			return &y, nil
		})
	})
}

func (ps *personaServer) viewContactSet(ctx context.Context) (*contactset.State, cadata.Store, error) {
	feedID, err := ps.getContactSetFeed(ctx)
	if err != nil {
		return nil, nil, err
	}
	return ps.contactSetCtrl.View(ctx, feedID)
}

func (ps *personaServer) modifyDirectory(ctx context.Context, fn func(s cadata.Store, x directory.State) (*directory.State, error)) error {
	feedID, err := ps.getDirectoryFeed(ctx)
	if err != nil {
		return err
	}
	peerID, err := ps.getLocalPeer(ctx)
	if err != nil {
		return err
	}
	return ps.directoryCtrl.Modify(ctx, feedID, peerID, func(feed *feeds.Feed[contactset.State], s cadata.Store) error {
		return feed.Modify(ctx, peerID, func(prev []feeds.Ref, x directory.State) (*directory.State, error) {
			return fn(s, x)
		})
	})
}

func (ps *personaServer) viewDirectory(ctx context.Context) (*directory.State, cadata.Store, error) {
	feedID, err := ps.getDirectoryFeed(ctx)
	if err != nil {
		return nil, nil, err
	}
	return ps.directoryCtrl.View(ctx, feedID)
}

func (ps *personaServer) modifyChannel(ctx context.Context, name string, fn func(cadata.Store, channel.State, PeerID) (*channel.State, error)) error {
	feedID, err := ps.getChannelFeed(ctx, name)
	if err != nil {
		return err
	}
	author, err := ps.getLocalPeer(ctx)
	if err != nil {
		return err
	}
	return ps.channelCtrl.Modify(ctx, feedID, author, func(feed *feeds.Feed[channel.State], s cadata.Store) error {
		return feed.Modify(ctx, author, func(prev []feeds.Ref, x channel.State) (*channel.State, error) {
			return fn(s, x, author)
		})
	})
}

func (ps *personaServer) viewChannel(ctx context.Context, name string) (*channel.State, cadata.Store, error) {
	feedID, err := ps.getChannelFeed(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	return ps.channelCtrl.View(ctx, feedID)
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

func (s *personaServer) getDirectoryFeed(ctx context.Context) (int, error) {
	var feedID int
	err := s.db.GetContext(ctx, &feedID, `SELECT directory_feed FROM personas WHERE id = ?`, s.id)
	return feedID, err
}

func (s *personaServer) getContactSetFeed(ctx context.Context) (int, error) {
	var feedID int
	err := s.db.GetContext(ctx, &feedID, `SELECT contactset_feed FROM personas WHERE id = ?`, s.id)
	return feedID, err
}

func (s *personaServer) getChannelFeed(ctx context.Context, name string) (int, error) {
	x, store, err := s.viewDirectory(ctx)
	if err != nil {
		return 0, err
	}
	op := directory.New()
	v, err := op.Get(ctx, store, *x, name)
	if err != nil {
		return 0, err
	}
	if v.Channel == nil {
		return 0, errors.New("non-channel directory entry not supported")
	}
	return lookupFeed(s.db, s.id, *v.Channel)
}

func (ps *personaServer) newContactSetProtocol(s cadata.Store) feeds.Protocol[contactset.State] {
	return contactset.NewProtocol(s, ps.members)
}

func (ps *personaServer) newDirectoryProtocol(s cadata.Store) feeds.Protocol[directory.State] {
	return directory.NewProtocol(s, ps.members)
}

func (ps *personaServer) newChannelProtocol(s cadata.Store) feeds.Protocol[channel.State] {
	return channel.NewProtocol(s)
}
