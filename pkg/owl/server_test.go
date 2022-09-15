package owl

import (
	"context"
	"testing"

	"github.com/brendoncarroll/go-state"
	"github.com/inet256/inet256/client/go_client/inet256client"
	"github.com/jmoiron/sqlx"
	"github.com/owlmessenger/owl/pkg/slices2"
	"github.com/stretchr/testify/require"
)

func TestSetupDB(t *testing.T) {
	db := newTestDB(t)
	db.SetMaxOpenConns(1)
	err := setupDB(context.Background(), db)
	require.NoError(t, err)
}

func TestPersonasCRUD(t *testing.T) {
	s := newTestServer(t)
	names := listPersonas(t, s)
	require.Len(t, names, 0)
	createPersona(t, s, "test")
	names = listPersonas(t, s)
	require.Len(t, names, 1)
	p := getPersona(t, s, "test")
	require.NotNil(t, p)
}

func TestChannelsCRUD(t *testing.T) {
	s := newTestServer(t)
	createPersona(t, s, "test")
	cs := listChannels(t, s, "test")
	require.Len(t, cs, 0)
	createChannel(t, s, "test", "chan1", nil)
	cs = listChannels(t, s, "test")
	require.Len(t, cs, 1)
}

func TestAddContact(t *testing.T) {
	s := newTestServer(t)
	createPersona(t, s, "A")
	createPersona(t, s, "B")
	createContact(t, s, "A", "b", getPeer(t, s, "B"))
	createContact(t, s, "B", "a", getPeer(t, s, "A"))
	acs := listContacts(t, s, "A")
	bcs := listContacts(t, s, "B")
	require.Len(t, acs, 1)
	require.Len(t, bcs, 1)
}

func TestChannelRW(t *testing.T) {
	s := newTestServer(t)
	createPersona(t, s, "A")
	createPersona(t, s, "B")
	createContact(t, s, "A", "b", getPeer(t, s, "B"))
	createContact(t, s, "B", "a", getPeer(t, s, "A"))

	// A invites B to a new channel
	createChannel(t, s, "A", "chan1", []string{"b"})
	require.Len(t, readEvents(t, s, "A", "chan1"), 1)

	msgBody := "hello world"
	sendMessage(t, s, "A", "chan1", MessageParams{Type: "text", Body: []byte(msgBody)})
	events := readEvents(t, s, "A", "chan1")
	t.Log(events)
	require.Len(t, events, 2)
	require.NotNil(t, events[1].Message)
	require.Equal(t, `"`+msgBody+`"`, string(events[1].Message.Body))
}

func createChannel(t testing.TB, x API, persona, name string, members []string) {
	ctx := context.Background()
	require.NoError(t, x.CreateChannel(ctx, ChannelID{Persona: persona, Name: name}, ChannelParams{Members: members}))
}

func listChannels(t testing.TB, x API, persona string) []string {
	ctx := context.Background()
	ret, err := x.ListChannels(ctx, persona, state.TotalSpan[string](), 0)
	require.NoError(t, err)
	return ret
}

func sendMessage(t testing.TB, x API, persona, chanName string, p MessageParams) {
	ctx := context.Background()
	err := x.Send(ctx, ChannelID{Persona: persona, Name: chanName}, p)
	require.NoError(t, err)
}

func readEvents(t testing.TB, x API, persona, chanName string) []Event {
	ctx := context.Background()
	pairs, err := x.Read(ctx, ChannelID{Persona: persona, Name: chanName}, EventPath{}, 0)
	require.NoError(t, err)
	return slices2.Map(pairs, func(p Pair) Event { return *p.Event })
}

func createPersona(t testing.TB, x API, name string) {
	ctx := context.Background()
	err := x.CreatePersona(ctx, name)
	require.NoError(t, err)
}

func joinPersona(t testing.TB, x API, name string, peers []PeerID) {
	ctx := context.Background()
	err := x.JoinPersona(ctx, name, peers)
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
	p, err := x.GetPersona(ctx, name)
	require.NoError(t, err)
	return p
}

func createContact(t testing.TB, x API, persona, name string, peerID PeerID) {
	ctx := context.Background()
	err := x.CreateContact(ctx, persona, name, Contact{
		Addrs: []PeerID{peerID},
	})
	require.NoError(t, err)
}

func listContacts(t testing.TB, x API, persona string) []string {
	ctx := context.Background()
	ret, err := x.ListContact(ctx, persona)
	require.NoError(t, err)
	return ret
}

func getPeer(t testing.TB, x API, persona string) PeerID {
	ctx := context.Background()
	p, err := x.GetPersona(ctx, persona)
	require.NoError(t, err)
	return p.LocalIDs[0]
}

func newTestServer(t testing.TB) *Server {
	db := newTestDB(t)
	s := NewServer(db, inet256client.NewTestService(t))
	t.Cleanup(func() {
		//s.Close()
	})
	return s
}

func newTestDB(t testing.TB) *sqlx.DB {
	db, err := sqlx.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}
