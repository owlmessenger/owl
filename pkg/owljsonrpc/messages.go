package owljsonrpc

import "github.com/owlmessenger/owl/pkg/owl"

type CreatePersonaReq struct {
	Name string
}

type CreatePersonaRes struct{}

type JoinPersonaReq struct {
	Name  string
	Peers []owl.PeerID
}

type JoinPersonaRes struct {
}

type ExpandPersonaReq struct {
	Name string
	Peer owl.PeerID
}

type ExpandPersonaRes struct{}

type ListPersonaReq struct{}

type ListPersonaRes []string
