package owltest

import (
	"testing"

	"github.com/inet256/inet256/pkg/inet256"
	"github.com/stretchr/testify/require"
)

func TestPersonaAPI(t *testing.T, sf SetupFunc) {
	test := func(name string, x func(t *testing.T, sf SetupFunc)) {
		t.Run(name, func(t *testing.T) {
			t.Helper()
			t.Parallel()
			x(t, sf)
		})
	}
	test("CRUD", TestPersonasCRUD)
}

func TestPersonasCRUD(t *testing.T, sf SetupFunc) {
	s := setupOne(t, sf)

	names := listPersonas(t, s)
	require.Len(t, names, 0)
	createPersona(t, s, "test")
	names = listPersonas(t, s)
	require.Len(t, names, 1)
	p := getPersona(t, s, "test")
	require.NotNil(t, p)
}

func TestJoinPersona(t *testing.T, sf SetupFunc) {
	xs := setup(t, 2, sf)
	a, b := xs[0], xs[1]

	createPersona(t, a, "X")
	createPersona(t, b, "X")

	expandPersona(t, a, "X", []inet256.Addr{getPeer(t, b, "X")})
	expandPersona(t, b, "X", []inet256.Addr{getPeer(t, a, "X")})
}
