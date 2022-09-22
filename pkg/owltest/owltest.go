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
	t.Run("Persona", func(t *testing.T) {
		t.Parallel()
		TestPersonaAPI(t, sf)
	})
	t.Run("Contact", func(t *testing.T) {
		t.Parallel()
		TestContactAPI(t, sf)
	})
	t.Run("Channel", func(t *testing.T) {
		t.Parallel()
		TestChannelAPI(t, sf)
	})
}

var ctx = context.Background()

func setup(t testing.TB, n int, sf SetupFunc) (ret []owl.API) {
	ret = make([]owl.API, n)
	sf(t, ret)
	return ret
}

func setupOne(t testing.TB, sf SetupFunc) owl.API {
	return setup(t, 1, sf)[0]
}

func createChannel(t testing.TB, x API, persona, name string, sch string, members []string) {
	ctx := context.Background()
	require.NoError(t, x.CreateChannel(ctx, &owl.CreateChannelReq{
		Persona: persona,
		Name:    name,
		Scheme:  sch,
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

func expandPersona(t testing.TB, x API, name string, peers []PeerID) {
	err := x.ExpandPersona(ctx, &owl.ExpandPersonaReq{Name: name, Peers: peers})
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

func createContact(t testing.TB, x API, persona, name string, peerIDs ...PeerID) {
	ctx := context.Background()
	err := x.CreateContact(ctx, &owl.CreateContactReq{
		Persona: persona,
		Name:    name,
		Peers:   peerIDs,
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
