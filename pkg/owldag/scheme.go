package owldag

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
)

type ConsultFunc = func(PeerID) bool

type Scheme[T any] interface {
	// Validate checks that the state is valid
	Validate(ctx context.Context, s cadata.Getter, consult ConsultFunc, x T) error

	// ValidateStep checks that next is valid, given that prev is known to be valid.
	ValidateStep(ctx context.Context, s cadata.Getter, consult ConsultFunc, prev, next T) error

	Merge(ctx context.Context, s cadata.Store, xs []T) (*T, error)

	// Sync ensures that all of the data reachable by x is in dst, using src
	// to get missing data.
	Sync(ctx context.Context, src cadata.Getter, dst cadata.Store, x T) error
}
