package owldag

import "fmt"

type ErrNotAllowed struct {
	Peer PeerID
}

func (e ErrNotAllowed) Error() string {
	return fmt.Sprintf("peer %v is not allowed", e.Peer)
}

type ErrBadN[T any] struct {
	Have, Want uint64
	Node       Node[T]
}

func (e ErrBadN[T]) Error() string {
	return fmt.Sprintf("node has incorrect value of n HAVE: %v WANT: %v", e.Have, e.Want)
}

type ErrReplayedN struct {
	N, Last uint64
	Peer    PeerID
}

func (e ErrReplayedN) Error() string {
	return fmt.Sprintf("peer %v replayed n with value %v, last was %v", e.Peer, e.N, e.Last)
}
