package owltest

import (
	"testing"

	"github.com/owlmessenger/owl/src/owl"
	"github.com/stretchr/testify/require"
)

func TestChannelAPI(t *testing.T, sf SetupFunc) {
	test := func(name string, x func(t *testing.T, sf SetupFunc)) {
		t.Run(name, func(t *testing.T) {
			t.Helper()
			t.Parallel()
			x(t, sf)
		})
	}
	test("CRUD", TestChannelCRUD)
	test("RW", TestChannelRW)
}

func TestChannelCRUD(t *testing.T, sf SetupFunc) {
	s := setupOne(t, sf)

	createPersona(t, s, "test")
	cs := listChannels(t, s, "test")
	require.Len(t, cs, 0)
	createChannel(t, s, "test", "chan1", owl.DirectMessageV0, nil)
	cs = listChannels(t, s, "test")
	require.Len(t, cs, 1)
}

func TestChannelRW(t *testing.T, sf SetupFunc) {
	s := setupOne(t, sf)

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

func TestDMSync(t *testing.T, sf SetupFunc) {
	xs := setup(t, 2, sf)
	a, b := xs[0], xs[1]

	createPersona(t, a, "A")
	createPersona(t, b, "B")

	createContact(t, a, "A", "B", getPeer(t, b, "B"))
	createContact(t, b, "B", "A", getPeer(t, a, "A"))

	createChannel(t, a, "A", "dmA", owl.DirectMessageV0, []string{"B"})
	sendMessage(t, a, "A", "dmA", "hello world")
}
