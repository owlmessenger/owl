package owl

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/blobcache/glfs"
	"github.com/inet256/inet256/pkg/inet256"
	"github.com/owlmessenger/owl/pkg/owlnet"
)

type PeerID = owlnet.PeerID

// ChannelID uniquely identifies a channel
type ChannelID struct {
	Persona string
	Name    string
}

func (a ChannelID) Compare(b ChannelID) int {
	switch {
	case a.Persona < b.Persona:
		return -1
	case a.Persona > b.Persona:
		return 1
	case a.Name < b.Name:
		return -1
	case a.Name > b.Name:
		return 1
	default:
		return 0
	}
}

// EventPath identifies an event within a channel
// EventIndexes will always have a length > 0.
// If the length is > 1, then all but the last element are considered the ThreadID
type EventPath []uint64

func ParseEventIndex(data []byte) (EventPath, error) {
	if len(data) < 8 || len(data)%8 != 0 {
		return nil, errors.New("wrong length for message index")
	}
	ret := EventPath{}
	for i := 0; i < len(data)/8; i++ {
		x := binary.BigEndian.Uint64(data[i*8:])
		ret = append(ret, x)
	}
	return ret, nil
}

// ThreadID is the component of the index which referes to a thread.
// ThreadID will be nil for messages in the root.
func (mi EventPath) ThreadID() []uint64 {
	l := len(mi)
	return mi[:l]
}

func (mi EventPath) Marshal() []byte {
	out := make([]byte, len(mi)*8)
	for i := range mi {
		binary.BigEndian.PutUint64(out[i*8:], mi[i])
	}
	return out
}

// Event is an element in a Channel.
// Events each have a unique EventPath.
type Event struct {
	ChannelCreated *struct{}
	PeerAdded      *PeerAddedEvent
	PeerRemoved    *PeerRemovedEvent
	Message        *Message
}

// MessageParams are used to create a message
type MessageParams struct {
	Type        string
	Headers     map[string]string
	Parent      EventPath
	Body        []byte
	Attachments map[string]glfs.Ref
}

type PeerAddedEvent struct {
	Peer, AddedBy PeerID
}

type PeerRemovedEvent struct {
	Peer, RemovedBy PeerID
}

type Message struct {
	From     string
	FromAddr inet256.ID
	SentAt   time.Time

	Type    string
	Headers map[string]string
	Body    []byte
}

// A Persona is a collection of inet256.IDs
// LocalIDs are IDs which the instance has a private key for, and can therefore send as those Personas.
type Persona struct {
	PublicFeed  *owlnet.FeedID
	PrivateFeed *owlnet.FeedID

	LocalIDs  []inet256.ID
	RemoteIDs []inet256.ID
}

type PersonaAPI interface {
	// CreatePersona creates a new persona called name.
	// If any ids are provided then the persona will not have a feed, and will attempt to join
	// a feed provided by one of the IDs.
	CreatePersona(ctx context.Context, name string, ids []PeerID) error
	// GetPersona retrieves the Persona at name
	GetPersona(ctx context.Context, name string) (*Persona, error)
	// ListPersonas lists personas
	ListPersonas(ctx context.Context) ([]string, error)
	// ExpandPersona adds a peer to the Persona at name.
	ExpandPersona(ctx context.Context, name string, peer PeerID) error
	// ShrinkPersona removes a peer from the Persona at name
	ShrinkPersona(ctx context.Context, name string, peer PeerID) error
	// GetPeer returns a peer that others can use to contact the persona
	GetPeer(ctx context.Context, name string) (*PeerID, error)
}

type ContactAPI interface {
	// AddContact adds a contact to the contact list for persona
	AddContact(ctx context.Context, persona, name string, ids inet256.ID) error
	// RemoveContact removes a contact.
	RemoveContact(ctx context.Context, persona, name string) error
	// ListContacts lists contacts starting with begin.
	ListContact(ctx context.Context, persona string) ([]string, error)
}

type ChannelAPI interface {
	// CreateChannel creates a new channel with name
	CreateChannel(ctx context.Context, cid ChannelID, members []string) error
	// DeleteChannel deletes the channel with name if it exists
	DeleteChannel(ctx context.Context, cid ChannelID) error
	// ListChannels lists channels starting with begin
	ListChannels(ctx context.Context, persona string, begin string, limit int) ([]string, error)
	// GetLatest returns the latest EventIndex
	GetLatest(ctx context.Context, cid ChannelID) (EventPath, error)
	// Send, sends a message to a channel.
	Send(ctx context.Context, cid ChannelID, mp MessageParams) error
	// Read reads events from a channel
	Read(ctx context.Context, cid ChannelID, begin EventPath, limit int) ([]Event, error)
	// Wait blocks until the latest message in a channel changes is different from since
	Wait(ctx context.Context, cid ChannelID, since EventPath) (EventPath, error)
}

type API interface {
	PersonaAPI
	ContactAPI
	ChannelAPI
}
