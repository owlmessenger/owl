package owltest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContactAPI(t *testing.T, sf SetupFunc) {
	test := func(name string, x func(t *testing.T, sf SetupFunc)) {
		t.Run(name, func(t *testing.T) {
			t.Helper()
			t.Parallel()
			x(t, sf)
		})
	}
	test("Add", TestAddContact)
}

func TestAddContact(t *testing.T, sf SetupFunc) {
	s := setupOne(t, sf)

	createPersona(t, s, "A")
	createPersona(t, s, "B")
	createContact(t, s, "A", "b", getPeer(t, s, "B"))
	createContact(t, s, "B", "a", getPeer(t, s, "A"))
	acs := listContacts(t, s, "A")
	bcs := listContacts(t, s, "B")
	require.Len(t, acs, 1)
	require.Len(t, bcs, 1)
}
