package stores

import "go.brendoncarroll.net/state/cadata"

type GetExister interface {
	cadata.Getter
	cadata.Exister
}
