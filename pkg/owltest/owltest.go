package owltest

import (
	"context"
	"testing"

	"github.com/owlmessenger/owl/pkg/owl"
	"github.com/stretchr/testify/require"
)

type (
	API = owl.API

	PeerID  = owl.PeerID
	Persona = owl.Persona
	Contact = owl.Contact

	ChannelID = owl.ChannelID
	Entry     = owl.Entry
	EntryPath = owl.EntryPath
)

type SetupFunc = func(t testing.TB, xs []owl.API)

func TestAPI(t *testing.T, sf SetupFunc) {
	t.Run("PersonaCRUD", func(t *testing.T) {
		t.Parallel()
		TestPersonasCRUD(t, sf)
	})
	t.Run("AddContact", func(t *testing.T) {
		t.Parallel()
		TestAddContact(t, sf)
	})
	t.Run("ChannelsCRUD", func(t *testing.T) {
		t.Parallel()
		TestChannelsCRUD(t, sf)
	})
	t.Run("ChannelsRW", func(t *testing.T) {
		t.Parallel()
		TestChannelsCRUD(t, sf)
	})
}

func TestPersonasCRUD(t *testing.T, sf SetupFunc) {
	xs := doSetup(t, 1, sf)
	s := xs[0]

	names := listPersonas(t, s)
	require.Len(t, names, 0)
	createPersona(t, s, "test")
	names = listPersonas(t, s)
	require.Len(t, names, 1)
	p := getPersona(t, s, "test")
	require.NotNil(t, p)
}

func TestAddContact(t *testing.T, sf SetupFunc) {
	xs := doSetup(t, 1, sf)
	s := xs[0]

	createPersona(t, s, "A")
	createPersona(t, s, "B")
	createContact(t, s, "A", "b", getPeer(t, s, "B"))
	createContact(t, s, "B", "a", getPeer(t, s, "A"))
	acs := listContacts(t, s, "A")
	bcs := listContacts(t, s, "B")
	require.Len(t, acs, 1)
	require.Len(t, bcs, 1)
}

func TestChannelsCRUD(t *testing.T, sf SetupFunc) {
	xs := doSetup(t, 1, sf)
	s := xs[0]

	createPersona(t, s, "test")
	cs := listChannels(t, s, "test")
	require.Len(t, cs, 0)
	createChannel(t, s, "test", "chan1", owl.DirectMessageV0, nil)
	cs = listChannels(t, s, "test")
	require.Len(t, cs, 1)
}

func TestChannelRW(t *testing.T, sf SetupFunc) {
	xs := doSetup(t, 1, sf)
	s := xs[0]

	createPersona(t, s, "A")
	createPersona(t, s, "B")
	createContact(t, s, "A", "b", getPeer(t, s, "B"))
	createContact(t, s, "B", "a", getPeer(t, s, "A"))

	// A invites B to a new channel
	createChannel(t, s, "A", "chan1", owl.DirectMessageV0, []string{"b"})

	msgBody := "hello world"
	sendMessage(t, s, "A", "chan1", msgBody)
	ents := readChannel(t, s, "A", "chan1")
	t.Log(ents)
	require.Len(t, ents, 1)
	require.NotNil(t, ents[0].Message)
	require.Equal(t, msgBody, ents[0].Message.AsString())
}

func doSetup(t testing.TB, n int, sf SetupFunc) (ret []owl.API) {
	ret = make([]owl.API, n)
	sf(t, ret)
	return ret
}

func createChannel(t testing.TB, x API, persona, name string, ty string, members []string) {
	ctx := context.Background()
	require.NoError(t, x.CreateChannel(ctx, &owl.CreateChannelReq{
		Persona: persona,
		Name:    name,
		Type:    ty,
		Members: members,
	}))
}

func listChannels(t testing.TB, x API, persona string) []string {
	ctx := context.Background()
	ret, err := x.ListChannels(ctx, &owl.ListChannelReq{
		Persona: persona,
		Limit:   0,
	})
	require.NoError(t, err)
	return ret
}

func sendMessage(t testing.TB, x API, persona, chanName string, msg string) {
	ctx := context.Background()
	mp := owl.NewText(msg)
	err := x.Send(ctx, &owl.SendReq{
		Persona: persona,
		Name:    chanName,
		Params:  mp,
	})
	require.NoError(t, err)
}

func readChannel(t testing.TB, x API, persona, chanName string) []Entry {
	ctx := context.Background()
	ents, err := x.Read(ctx, &owl.ReadReq{
		Persona: persona,
		Name:    chanName,
	})
	require.NoError(t, err)
	return ents
}

func createPersona(t testing.TB, x API, name string) {
	ctx := context.Background()
	err := x.CreatePersona(ctx, &owl.CreatePersonaReq{Name: name})
	require.NoError(t, err)
}

func joinPersona(t testing.TB, x API, name string, peers []PeerID) {
	ctx := context.Background()
	err := x.JoinPersona(ctx, &owl.JoinPersonaReq{
		Name:  name,
		Peers: peers,
	})
	require.NoError(t, err)
}

func listPersonas(t testing.TB, x API) []string {
	ctx := context.Background()
	names, err := x.ListPersonas(ctx)
	require.NoError(t, err)
	return names
}

func getPersona(t testing.TB, x API, name string) *Persona {
	ctx := context.Background()
	p, err := x.GetPersona(ctx, &owl.GetPersonaReq{Name: name})
	require.NoError(t, err)
	return p
}

func createContact(t testing.TB, x API, persona, name string, peerID PeerID) {
	ctx := context.Background()
	err := x.CreateContact(ctx, &owl.CreateContactReq{
		Persona: persona,
		Name:    name,
		Peers:   []PeerID{peerID},
	})
	require.NoError(t, err)
}

func listContacts(t testing.TB, x API, persona string) []string {
	ctx := context.Background()
	ret, err := x.ListContact(ctx, &owl.ListContactReq{Persona: persona})
	require.NoError(t, err)
	return ret
}

func getPeer(t testing.TB, x API, persona string) PeerID {
	ctx := context.Background()
	p, err := x.GetPersona(ctx, &owl.GetPersonaReq{Name: persona})
	require.NoError(t, err)
	return p.LocalIDs[0]
}
