package owl

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/brendoncarroll/go-state"
	"github.com/inet256/inet256/pkg/inet256"

	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/owlnet"
	"github.com/owlmessenger/owl/pkg/p/channel"
)

// PeerID uniquely identifies peers on the network.
type PeerID = owlnet.PeerID

// A Persona is a collection of inet256.IDs
// LocalIDs are IDs which the instance has a private key for, and can therefore send as those Peers.
type Persona struct {
	LocalIDs  []PeerID
	RemoteIDs []PeerID
}

type PersonaAPI interface {
	// CreatePersona creates a new persona called name.
	// If any ids are provided then the persona will not have a feed, and will attempt to join
	// a feed provided by one of the IDs.
	CreatePersona(ctx context.Context, name string) error
	// JoinPersona joins a persona, which has already been created on another peer.
	JoinPersona(ctx context.Context, name string, peers []PeerID) error
	// GetPersona retrieves the Persona at name
	GetPersona(ctx context.Context, name string) (*Persona, error)
	// ListPersonas lists personas on the instance.
	ListPersonas(ctx context.Context) ([]string, error)
	// ExpandPersona adds a peer to the Persona at name.
	ExpandPersona(ctx context.Context, name string, peer PeerID) error
	// ShrinkPersona removes a peer from the Persona at name
	ShrinkPersona(ctx context.Context, name string, peer PeerID) error
}

type Contact struct {
	Addrs []inet256.Addr
}

type ContactAPI interface {
	// CreateContact adds a contact to the contact list for persona
	CreateContact(ctx context.Context, persona, name string, c Contact) error
	// RemoveContact removes a contact.
	DeleteContact(ctx context.Context, persona, name string) error
	// ListContacts lists contacts starting with begin.
	ListContact(ctx context.Context, persona string) ([]string, error)
	// GetContact returns information about a contact.
	GetContact(ctx context.Context, persona, name string) (*Contact, error)
}

var validContactName = regexp.MustCompile(`^[A-Za-z0-9 \-_.]$`)

func CheckContactName(x string) error {
	if !validContactName.MatchString(x) {
		return errors.New("invalid contact name")
	}
	return nil
}

// ChannelID uniquely identifies a channel
type ChannelID struct {
	Persona string
	Name    string
}

func (a ChannelID) Compare(b ChannelID) int {
	if a.Persona != b.Persona {
		return strings.Compare(a.Persona, b.Persona)
	}
	if a.Name != b.Name {
		return strings.Compare(a.Persona, b.Persona)
	}
	return 0
}

type ChannelInfo struct {
	Feed   feeds.ID
	Latest EventPath
}

// EventPath identifies an event within a channel
// EventPaths will always have a length > 0.
// If the length is > 1, then all but the last element are considered the ThreadID
type EventPath channel.Path

func ParseEventPath(data []byte) (EventPath, error) {
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

func (p *EventPath) Scan(x interface{}) error {
	switch x := x.(type) {
	case []byte:
		p2, err := ParseEventPath(x)
		if err != nil {
			return err
		}
		*p = p2
		return nil
	default:
		return fmt.Errorf("cannot scan from type %T", x)
	}
}

func (p EventPath) Value() (driver.Value, error) {
	return p.Marshal(), nil
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

func (mi EventPath) String() string {
	sb := strings.Builder{}
	for i, n := range mi {
		if i > 0 {
			sb.WriteString(".")
		}
		sb.WriteString(strconv.FormatUint(n, 10))
	}
	return sb.String()
}

// Event is an element in a Channel.
// Events each have a unique EventPath.
type Event struct {
	ChannelCreated *struct{}
	PeerAdded      *PeerAdded
	PeerRemoved    *PeerRemoved
	Message        *Message
}

// PeerAdded is a type of Event
type PeerAdded struct {
	Peer, AddedBy PeerID
}

// PeerRemoved is a type of Event
type PeerRemoved struct {
	Peer, RemovedBy PeerID
}

// Message
type Message struct {
	FromContact string
	FromPeer    PeerID
	After       []EventPath

	SentAt  time.Time
	Type    string
	Headers map[string]string
	Body    []byte
}

// MessageParams are used to create a message
type MessageParams struct {
	Type   string
	Parent EventPath
	Body   []byte
}

// Text creates parameters for a simple text message
func PlainText(x string) MessageParams {
	return MessageParams{
		Type: "text/plain",
		Body: []byte(x),
	}
}

// Pair is an Event and the Path to it
// Pairs are returned from Read
type Pair struct {
	Path  EventPath
	Event *Event
}

type ChannelAPI interface {
	// CreateChannel creates a new channel with name
	CreateChannel(ctx context.Context, cid ChannelID, members []string) error
	// JoinChannel joins an existing channel
	JoinChannel(ctx context.Context, cid ChannelID, root feeds.ID) error
	// DeleteChannel deletes the channel with name if it exists
	DeleteChannel(ctx context.Context, cid ChannelID) error
	// ListChannels lists channels starting with begin
	ListChannels(ctx context.Context, persona string, span state.Span[string], limit int) ([]string, error)
	// GetLatest returns the latest EventIndex
	GetChannel(ctx context.Context, cid ChannelID) (*ChannelInfo, error)

	// Send, sends a message to a channel.
	Send(ctx context.Context, cid ChannelID, mp MessageParams) error
	// Read reads events from a channel
	Read(ctx context.Context, cid ChannelID, begin EventPath, limit int) ([]Pair, error)
	// Wait blocks until the latest message in a channel changes is different from since
	Wait(ctx context.Context, cid ChannelID, since EventPath) (EventPath, error)
}

type API interface {
	PersonaAPI
	ContactAPI
	ChannelAPI
}
