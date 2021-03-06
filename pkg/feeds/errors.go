package feeds

import "fmt"

type ErrPeerNotInFeed struct {
	Peer PeerID
}

func (e ErrPeerNotInFeed) Error() string {
	return fmt.Sprintf("peer %v not in feed", e.Peer)
}

type ErrBadN struct {
	Have, Want uint64
	Node       Node
}

func (e ErrBadN) Error() string {
	return fmt.Sprintf("node has incorrect value of n HAVE: %v WANT: %v", e.Have, e.Want)
}

type ErrReplayedN struct {
	N, Last uint64
	Peer    PeerID
}

func (e ErrReplayedN) Error() string {
	return fmt.Sprintf("peer %v replayed n with value %v, last was %v", e.Peer, e.N, e.Last)
}
