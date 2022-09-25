package owl

import (
	"context"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/jmoiron/sqlx"
	"github.com/owlmessenger/owl/pkg/owldag"
)

func initDAG[T any](tx *sqlx.Tx, volID int, initF func(s cadata.Store) (*T, error)) (*owldag.Ref, error) {
	ctx := context.Background()
	var ret *owldag.Ref
	if err := modifyVolumeTx(tx, volID, func(x []byte, s0, sTop cadata.Store) ([]byte, error) {
		init, err := initF(s0)
		if err != nil {
			return nil, err
		}
		state, err := owldag.InitState(ctx, sTop, init, nil)
		if err != nil {
			return nil, err
		}
		ret = &state.Epochs[0]
		return state.Marshal(), nil
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

// modifyFeedInner calls fn to modify the contents of the feed.
func modifyDAGInner[T any](tx *sqlx.Tx, volID int, privKey owldag.PrivateKey, fn func(s cadata.Store, x T) (*T, error)) error {
	ctx := context.TODO()
	return modifyVolumeTx(tx, volID, func(x []byte, s0, sTop cadata.Store) ([]byte, error) {
		dagState, err := owldag.ParseState[T](x)
		if err != nil {
			return nil, err
		}
		dag := owldag.New(nil, sTop, s0, *dagState)
		if err := dag.Modify(ctx, privKey, func(s cadata.Store, x T) (*T, error) {
			return fn(s0, x)
		}); err != nil {
			return nil, err
		}
		return dag.SaveBytes(), nil
	})
}

func viewDAG[T any](ctx context.Context, db *sqlx.DB, scheme owldag.Scheme[T], volID int) (*owldag.DAG[T], error) {
	data, sTop, s0, err := viewVolume(ctx, db, volID)
	if err != nil {
		return nil, err
	}
	state, err := owldag.ParseState[T](data)
	if err != nil {
		return nil, err
	}
	return owldag.New(scheme, sTop, s0, *state), nil
}

func viewDAGInner[T any](ctx context.Context, db *sqlx.DB, volID int) (*T, cadata.Store, error) {
	data, s0, _, err := viewVolume(ctx, db, volID)
	if err != nil {
		return nil, nil, err
	}
	if len(data) == 0 {
		return nil, s0, nil
	}
	x, err := owldag.ParseState[T](data)
	if err != nil {
		return nil, nil, err
	}
	return &x.X, s0, nil
}

func modifyDAG[T any](ctx context.Context, db *sqlx.DB, volID int, scheme owldag.Scheme[T], fn func(*owldag.DAG[T]) error) error {
	return modifyVolume(ctx, db, volID, func(x []byte, s0 cadata.Store, sTop cadata.Store) ([]byte, error) {
		state, err := owldag.ParseState[T](x)
		if err != nil {
			return nil, err
		}
		dag := owldag.New(scheme, sTop, s0, *state)
		if err := fn(dag); err != nil {
			return nil, err
		}
		return dag.SaveBytes(), nil
	})
}
