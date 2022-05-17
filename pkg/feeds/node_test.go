package feeds

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeMarshalParse(t *testing.T) {
	testCases := []Node{
		{
			Previous: IDSet[NodeID]{new32Byte(0), new32Byte(1)},
			Author:   new32Byte(3),
			N:        5,
			Init: &Init{
				Salt:  new32Byte(6),
				Peers: IDSet[PeerID]{new32Byte(7)},
			},
		},
		{
			N:        1,
			Previous: IDSet[NodeID]{new32Byte(2), new32Byte(3)},
			Author:   new32Byte(4),
			Data:     []byte("hello world"),
		},
	}
	for _, x := range testCases {
		data := x.Marshal()
		id1 := Hash(data)
		y, err := ParseNode(data)
		require.NoError(t, err)
		require.Equal(t, x, *y)
		id2 := NewNodeID(*y)
		require.Equal(t, id1, id2)
	}
}

func new32Byte(i int) (ret [32]byte) {
	binary.BigEndian.PutUint64(ret[:], uint64(i))
	return ret
}
