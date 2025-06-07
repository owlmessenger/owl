package stores

import (
	"context"
	"errors"
	"math/rand"

	"go.brendoncarroll.net/state/cadata"
)

type Union []cadata.Getter

func (s Union) Get(ctx context.Context, id cadata.ID, buf []byte) (int, error) {
	for i := range s {
		n, err := s[i].Get(ctx, id, buf)
		if errors.Is(err, cadata.ErrNotFound{Key: id}) {
			continue
		}
		return n, err
	}
	return 0, cadata.ErrNotFound{Key: id}
}

func (s Union) MaxSize() (ret int) {
	for i := range s {
		if max := s[i].MaxSize(); max > ret {
			ret = max
		}
	}
	return ret
}

func (s Union) Hash(x []byte) cadata.ID {
	i := rand.Intn(len(s))
	return s[i].Hash(x)
}
