package owl

import (
	"context"
	"encoding/json"
	"math"

	"github.com/blobcache/glfs"
	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/go-tai64"
	"github.com/jmoiron/sqlx"

	"github.com/owlmessenger/owl/pkg/dbutil"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/owlnet"
)

var _ ChannelAPI = &Server{}

func (s *Server) CreateChannel(ctx context.Context, cid ChannelID, members []string) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	var feedID *owlnet.FeedID
	var localID PeerID
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		var err error
		localID, err = s.getAuthor(tx, cid)
		if err != nil {
			return err
		}
		peers := []PeerID{localID}
		for _, member := range members {
			peers2, err := s.getPeersFor(tx, cid.Persona, member)
			if err != nil {
				return err
			}
			peers = append(peers, peers2...)
		}
		feedID, err = createFeed(tx, peers)
		if err != nil {
			return err
		}
		_, err = s.createChannel(tx, cid, *feedID)
		if err != nil {
			return err
		}
		return s.buildChannelIndex(tx, cid)
	}); err != nil {
		return err
	}
	return nil
	// TODO: no-op to trigger a sync
	// return s.feedController.modifyFeed(ctx, *feedID, localID, func(_ cadata.Store, x *feeds.Feed) (*feeds.Feed, error) {
	// 	return x, nil
	// })
}

func (s *Server) DeleteChannel(ctx context.Context, cid ChannelID) error {
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		feedID, _, err := s.lookupChannelFeed(tx, cid)
		if err != nil {
			return err
		}
		if err := deleteFeed(tx, feedID); err != nil {
			return err
		}
		return s.deleteChannel(tx, cid)
	})
}

func (s *Server) ListChannels(ctx context.Context, persona, begin string, limit int) ([]string, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	if limit < 1 {
		limit = math.MaxInt
	}
	var ret []string
	if err := s.db.SelectContext(ctx, &ret, `SELECT channels.name FROM personas
		JOIN channels ON  personas.id = channels.persona_id
		WHERE personas.name = ? AND channels.name >= ?
		ORDER BY channels.name
		LIMIT ?
	`, persona, begin, limit); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Server) Send(ctx context.Context, cid ChannelID, mp MessageParams) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	// TODO: marshal message
	// determine localID to send as and the feedID of the channel's feed.
	var localID owlnet.PeerID
	var feedID owlnet.FeedID
	var msgData []byte
	if err := dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		var err error
		localID, err = s.getAuthor(tx, cid)
		if err != nil {
			return err
		}
		var chanID int
		feedID, chanID, err = s.lookupChannelFeed(tx, cid)
		if err != nil {
			return err
		}
		msgData = marshalMessage(s.newMessage(chanID, mp))
		return err
	}); err != nil {
		return err
	}
	if err := s.feedController.modifyFeed(ctx, feedID, localID, func(store cadata.Store, x *feeds.Feed) error {
		if err := x.AppendData(nil, store, localID, msgData); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return dbutil.DoTx(ctx, s.db, func(tx *sqlx.Tx) error {
		return s.buildChannelIndex(tx, cid)
	})
}

func (s *Server) Read(ctx context.Context, cid ChannelID, begin EventPath, limit int) ([]Event, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 1024
	}
	var datas [][]byte
	if err := s.db.SelectContext(ctx, &datas, `SELECT blobs.data FROM personas
		JOIN channels on personas.id = channels.persona_id
		JOIN channel_events ON channels.id = channel_events.channel_id
		JOIN blobs ON channel_events.blob_id = blobs.id
		WHERE personas.name = ? AND channels.name = ? AND channel_events.path >= ?
		LIMIT ?
	`, cid.Persona, cid.Name, begin.Marshal(), limit); err != nil {
		return nil, err
	}
	events := make([]Event, len(datas))
	for i, data := range datas {
		node, err := feeds.ParseNode(data)
		if err != nil {
			return nil, err
		}
		ev, err := s.eventFromNode(s.db, cid.Persona, *node)
		if err != nil {
			return nil, err
		}
		events[i] = *ev
	}
	return events, nil
}

func (s *Server) GetChannel(ctx context.Context, cid ChannelID) (*ChannelInfo, error) {
	var data []byte
	if err := s.db.SelectContext(ctx, &data, `SELECT max(channel_events.path) FROM personas
		JOIN channels on personas.id = channels.persona_id
		JOIN channel_events ON channels.id = channel_events.channel_id
		WHERE personas.name = ? AND channels.name = ?
	`, cid.Persona, cid.Name); err != nil {
		return nil, err
	}
	ep, err := ParseEventPath(data)
	if err != nil {
		return nil, err
	}
	return &ChannelInfo{Latest: ep}, nil
}

func (s *Server) Wait(ctx context.Context, cid ChannelID, since EventPath) (EventPath, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *Server) Flush(ctx context.Context, cid ChannelID) error {
	if err := s.Init(ctx); err != nil {
		return err
	}
	return nil
}

// WireMessage is the structure used to represent messages in the feed.
type WireMessage struct {
	SentAt      tai64.TAI64N        `json:"sent_at"`
	Parent      EventPath           `json:"parent"`
	Type        string              `json:"type"`
	Headers     map[string]string   `json:"headers"`
	Body        string              `json:"body"`
	Attachments map[string]glfs.Ref `json:"attachments"`
}

func (s *Server) newMessage(chanInt int, mp MessageParams) WireMessage {
	if len(mp.Parent) > 0 {
		// TODO: lookup the NodeID of the message and include that.
		panic("parent messages not yet support")
	}
	return WireMessage{
		SentAt:      tai64.Now(),
		Type:        mp.Type,
		Parent:      mp.Parent,
		Body:        string(mp.Body),
		Attachments: mp.Attachments,
	}
}

func parseMessage(x []byte) (*WireMessage, error) {
	var ret WireMessage
	if err := json.Unmarshal(x, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func marshalMessage(m WireMessage) []byte {
	data, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return data
}

// eventFromNode converts a node in the Feed into an event.
func (s *Server) eventFromNode(tx dbGetter, persona string, node feeds.Node) (*Event, error) {
	switch {
	case node.Init != nil:
		return &Event{ChannelCreated: &struct{}{}}, nil
	case node.AddPeer != nil:
		return &Event{PeerAdded: &PeerAdded{
			Peer:    node.AddPeer.Peer,
			AddedBy: node.Author,
		}}, nil
	case node.RemovePeer != nil:
		return &Event{PeerRemoved: &PeerRemoved{
			Peer:      node.RemovePeer.Peer,
			RemovedBy: node.Author,
		}}, nil
	case node.Data != nil:
		wmsg, err := parseMessage(node.Data)
		if err != nil {
			return nil, err
		}
		contact, err := s.lookupName(tx, persona, node.Author)
		if err != nil {
			return nil, err
		}
		return &Event{
			Message: &Message{
				FromContact: contact,
				FromPeer:    node.Author,
				SentAt:      wmsg.SentAt.GoTime(),
				Type:        wmsg.Type,
				Headers:     wmsg.Headers,
				Body:        []byte(wmsg.Body),
			},
		}, nil
	default:
		panic(node)
	}
}
