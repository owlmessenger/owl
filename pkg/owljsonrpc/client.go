package owljsonrpc

import (
	"context"
	"io"
	"runtime"
	"strings"

	"github.com/owlmessenger/owl/pkg/owl"
	"github.com/sourcegraph/jsonrpc2"
)

var _ owl.API = &Client{}

type Client struct {
	c *jsonrpc2.Conn
}

func NewClient(rwc io.ReadWriteCloser) *Client {
	ctx := context.Background()
	objStream := jsonrpc2.NewBufferedStream(rwc, jsonrpc2.PlainObjectCodec{})
	c := jsonrpc2.NewConn(ctx, objStream, nil)
	return &Client{c: c}
}

func (c *Client) Close() error {
	return c.c.Close()
}

// CreatePersona creates a new persona called name.
// If any ids are provided then the persona will not have a feed, and will attempt to join
// a feed provided by one of the IDs.
func (c *Client) CreatePersona(ctx context.Context, req *owl.CreatePersonaReq) error {
	res := &struct{}{}
	return c.c.Call(ctx, currentMethodName(), req, &res)
}

// JoinPersona joins a persona, which has already been created on another peer.
func (c *Client) JoinPersona(ctx context.Context, req *owl.JoinPersonaReq) error {
	res := &struct{}{}
	return c.c.Call(ctx, currentMethodName(), req, &res)
}

// GetPersona retrieves the Persona at name
func (c *Client) GetPersona(ctx context.Context, req *owl.GetPersonaReq) (*owl.Persona, error) {
	var res owl.Persona
	if err := c.c.Call(ctx, currentMethodName(), req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// ListPersonas lists personas on the instance.
func (c *Client) ListPersonas(ctx context.Context) ([]string, error) {
	var res []string
	if err := c.c.Call(ctx, currentMethodName(), nil, &res); err != nil {
		return nil, err
	}
	return res, nil
}

// ExpandPersona adds a peer to the Persona at name.
func (c *Client) ExpandPersona(ctx context.Context, req *owl.ExpandPersonaReq) error {
	var res struct{}
	return c.c.Call(ctx, currentMethodName(), req, &res)
}

// ShrinkPersona removes a peer from the Persona at name
func (c *Client) ShrinkPersona(ctx context.Context, req *owl.ShrinkPersonaReq) error {
	var res struct{}
	return c.c.Call(ctx, currentMethodName(), req, &res)
}

// CreateContact adds a contact to the contact list for persona
func (c *Client) CreateContact(ctx context.Context, req *owl.CreateContactReq) error {
	var res struct{}
	return c.c.Call(ctx, currentMethodName(), req, &res)
}

// RemoveContact removes a contact.
func (c *Client) DeleteContact(ctx context.Context, req *owl.DeleteContactReq) error {
	var res struct{}
	return c.c.Call(ctx, currentMethodName(), req, &res)
}

// ListContacts lists contacts starting with begin.
func (c *Client) ListContact(ctx context.Context, req *owl.ListContactReq) ([]string, error) {
	var res []string
	if err := c.c.Call(ctx, currentMethodName(), req, &res); err != nil {
		return nil, err
	}
	return res, nil
}

// GetContact returns information about a contact.
func (c *Client) GetContact(ctx context.Context, req *owl.GetContactReq) (*owl.Contact, error) {
	var res owl.Contact
	if err := c.c.Call(ctx, currentMethodName(), req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// CreateChannel creates a new channel with name
func (c *Client) CreateChannel(ctx context.Context, req *owl.CreateChannelReq) error {
	var res struct{}
	return c.c.Call(ctx, currentMethodName(), req, &res)
}

// JoinChannel joins an existing channel
func (c *Client) JoinChannel(ctx context.Context, req *owl.JoinChannelReq) error {
	var res struct{}
	return c.c.Call(ctx, currentMethodName(), req, &res)
}

// DeleteChannel deletes the channel with name if it exists
func (c *Client) DeleteChannel(ctx context.Context, cid *owl.ChannelID) error {
	var res struct{}
	return c.c.Call(ctx, currentMethodName(), cid, &res)
}

// ListChannels lists channels starting with begin
func (c *Client) ListChannels(ctx context.Context, req *owl.ListChannelReq) ([]string, error) {
	var res []string
	if err := c.c.Call(ctx, currentMethodName(), req, &res); err != nil {
		return nil, err
	}
	return res, nil
}

// GetLatest returns the latest EntryIndex
func (c *Client) GetChannel(ctx context.Context, cid *owl.ChannelID) (*owl.ChannelInfo, error) {
	var res owl.ChannelInfo
	if err := c.c.Call(ctx, currentMethodName(), cid, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// Send, sends a message to a channel.
func (c *Client) Send(ctx context.Context, req *owl.SendReq) error {
	var res struct{}
	return c.c.Call(ctx, currentMethodName(), req, &res)
}

// Read reads events from a channel
func (c *Client) Read(ctx context.Context, req *owl.ReadReq) ([]owl.Entry, error) {
	var res []owl.Entry
	if err := c.c.Call(ctx, currentMethodName(), req, &res); err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Client) Wait(ctx context.Context, req *owl.WaitReq) (*owl.WaitRes, error) {
	var res owl.WaitRes
	if err := c.c.Call(ctx, currentMethodName(), req, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *Client) Sync(ctx context.Context, req *owl.SyncReq) error {
	var res struct{}
	return c.c.Call(ctx, currentMethodName(), req, &res)
}

func currentMethodName() string {
	// https://stackoverflow.com/questions/52507676/name-of-current-function
	fpcs := make([]uintptr, 1)
	n := runtime.Callers(2, fpcs)
	if n == 0 {
		return ""
	}
	caller := runtime.FuncForPC(fpcs[0] - 1)
	if caller == nil {
		return ""
	}
	parts := strings.Split(caller.Name(), ".")
	return parts[len(parts)-1]
}
