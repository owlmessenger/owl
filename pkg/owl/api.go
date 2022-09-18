package owl

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/inet256/inet256/pkg/inet256"

	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/owlnet"
)

// PeerID uniquely identifies peers on the network.
type PeerID = owlnet.PeerID

// A Persona is a collection of inet256.IDs
// LocalIDs are IDs which the instance has a private key for, and can therefore send as those Peers.
type Persona struct {
	LocalIDs  []PeerID `json:"local_ids"`
	RemoteIDs []PeerID `json:"remote_ids"`
}

type CreatePersonaReq struct {
	Name string `json:"name"`
}

type JoinPersonaReq struct {
	Name  string   `json:"name"`
	Peers []PeerID `json:"peers"`
}

type GetPersonaReq struct {
	Name string `json:"name"`
}

type ExpandPersonaReq struct {
	Name  string   `json:"name"`
	Peers []PeerID `json:"peers"`
}

type ShrinkPersonaReq struct {
	Name string `json:"name'`
	Peer PeerID `json:"peer"`
}

type PersonaAPI interface {
	// CreatePersona creates a new persona called name.
	// If any ids are provided then the persona will not have a feed, and will attempt to join
	// a feed provided by one of the IDs.
	CreatePersona(ctx context.Context, req *CreatePersonaReq) error
	// JoinPersona joins a persona, which has already been created on another peer.
	JoinPersona(ctx context.Context, req *JoinPersonaReq) error
	// GetPersona retrieves the Persona at name
	GetPersona(ctx context.Context, req *GetPersonaReq) (*Persona, error)
	// ListPersonas lists personas on the instance.
	ListPersonas(ctx context.Context) ([]string, error)
	// ExpandPersona adds a peer to the Persona at name.
	ExpandPersona(ctx context.Context, req *ExpandPersonaReq) error
	// ShrinkPersona removes a peer from the Persona at name
	ShrinkPersona(ctx context.Context, req *ShrinkPersonaReq) error
}

type Contact struct {
	Addrs []inet256.Addr `json:"addrs"`
}

type CreateContactReq struct {
	Persona string   `json:"persona"`
	Name    string   `json:"name"`
	Peers   []PeerID `json:"peers"`
}

type DeleteContactReq struct {
	Persona string `json:"persona"`
	Name    string `json:"name"`
}

type GetContactReq struct {
	Persona string `json:"persona"`
	Name    string `json:"name"`
}

type ListContactReq struct {
	Persona string `json:"persona"`
}

type ContactAPI interface {
	// CreateContact adds a contact to the contact list for persona
	CreateContact(ctx context.Context, req *CreateContactReq) error
	// RemoveContact removes a contact.
	DeleteContact(ctx context.Context, req *DeleteContactReq) error
	// ListContacts lists contacts starting with begin.
	ListContact(ctx context.Context, res *ListContactReq) ([]string, error)
	// GetContact returns information about a contact.
	GetContact(ctx context.Context, req *GetContactReq) (*Contact, error)
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
	Persona string `json:"persona"`
	Name    string `json:"name"`
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
	Type   string
	Feed   feeds.ID
	Latest EntryPath
}

type EntryPath = Path

// Entry is an element in a Channel.
// Entries each have a unique Path.
type Entry struct {
	Path EntryPath
	ID   cadata.ID

	PeerAdded   *PeerAdded
	PeerRemoved *PeerRemoved
	Message     *Message
}

// PeerAdded is a type of Entry
type PeerAdded struct {
	Peer, AddedBy PeerID
}

// PeerRemoved is a type of Entry
type PeerRemoved struct {
	Peer, RemovedBy PeerID
}

// Message is a type of Entry
type Message struct {
	// AuthorPeer is the PeerID that wrote the message
	AuthorPeer PeerID
	// AuthorContact is the name of the contact which the peer corresponds to (if any).
	AuthorContact string
	After         []EntryPath

	Timestamp time.Time
	Type      string
	Body      json.RawMessage
}

func (m *Message) AsString() (ret string) {
	json.Unmarshal(m.Body, &ret)
	return ret
}

// MessageParams are used to create a message
type MessageParams struct {
	Thread EntryPath
	Parent EntryPath

	Type string
	Body json.RawMessage
}

// NewText creates parameters for a simple text message
func NewText(x string) MessageParams {
	data, _ := json.Marshal(x)
	return MessageParams{
		Type: "text",
		Body: data,
	}
}

const (
	DirectMessageV0 = "directmsg@v0"
)

type CreateChannelReq struct {
	Persona string `json:"persona"`
	Name    string `json:"name"`

	Type    string   `json:"type"`
	Members []string `json:"members'`
}

type JoinChannelReq struct {
	Persona string   `json:"persona"`
	Name    string   `json:"name"`
	Root    feeds.ID `json:"root"`
}

type ListChannelReq struct {
	Persona string `json:"persona"`
	Begin   string `json:"begin"`
	Limit   int    `json:"limit"`
}

type SendReq struct {
	Persona string        `json:"persona"`
	Name    string        `json:"name"`
	Params  MessageParams `json:"params"`
}

type ReadReq struct {
	Persona string    `json:"persona"`
	Name    string    `json:"name"`
	Begin   EntryPath `json:"begin"`
	Limit   int       `json:"limit"`
}

type WaitReq struct {
	Persona string    `json:"persona"`
	Name    string    `json:"name"`
	Since   EntryPath `json:"since"`
}

type ChannelAPI interface {
	// CreateChannel creates a new channel with name
	CreateChannel(ctx context.Context, req *CreateChannelReq) error
	// JoinChannel joins an existing channel
	JoinChannel(ctx context.Context, req *JoinChannelReq) error
	// DeleteChannel deletes the channel with name if it exists
	DeleteChannel(ctx context.Context, cid *ChannelID) error
	// ListChannels lists channels starting with begin
	ListChannels(ctx context.Context, req *ListChannelReq) ([]string, error)
	// GetLatest returns the latest EntryIndex
	GetChannel(ctx context.Context, cid *ChannelID) (*ChannelInfo, error)

	// Send, sends a message to a channel.
	Send(ctx context.Context, req *SendReq) error
	// Read reads events from a channel
	Read(ctx context.Context, req *ReadReq) ([]Entry, error)
	// Wait blocks until the latest message in a channel changes is different from since
	Wait(ctx context.Context, cid ChannelID, since EntryPath) (EntryPath, error)
}

type API interface {
	PersonaAPI
	ContactAPI
	ChannelAPI
}
