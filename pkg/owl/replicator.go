package owl

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/owlmessenger/owl/pkg/owldag"
	"github.com/owlmessenger/owl/pkg/owlnet"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
)

type replicatorParams[T any] struct {
	Context context.Context
	Scheme  owldag.Scheme[T]

	View     func(context.Context, int) (*owldag.DAG[T], error)
	Modify   func(context.Context, int, func(dag *owldag.DAG[T]) error) error
	Push     func(ctx context.Context, dst PeerID, epoch owldag.Ref, heads []owldag.Head) error
	GetHeads func(ctx context.Context, dst PeerID, epoch owldag.Ref) ([]owldag.Head, error)
	GetStore func(dst PeerID) (cadata.Getter, error)
}

type replicator[T any] struct {
	params replicatorParams[T]

	ctx context.Context
	cf  context.CancelFunc
}

func newReplicator[T any](params replicatorParams[T]) *replicator[T] {
	ctx, cf := context.WithCancel(params.Context)
	r := &replicator[T]{
		params: params,

		ctx: ctx,
		cf:  cf,
	}
	return r
}

func (r *replicator[T]) Close() error {
	r.cf()
	return nil
}

// Notify tells the replicator that a dag has changed so it will begin replicating it.
func (r *replicator[T]) Notify(vid int) {
	logctx.Infof(r.ctx, "volume %d has changed. syncing...", vid)
	// TODO: use a queue
	go func() {
		if err := r.Sync(r.ctx, vid); err != nil {
			logctx.Errorf(r.ctx, "replicator.Push", err)
		}
	}()
}

// Wait blocks on the DAG in vid, until cond(dag_state) == false
func (r *replicator[T]) Wait(ctx context.Context, vid int, cond func(T) bool) error {
	// TODO:
	return nil
}

// Sync blocks until a pull and push have been performed sucessfully.
func (r *replicator[T]) Sync(ctx context.Context, fid int) error {
	if err := r.Push(ctx, fid); err != nil {
		return err
	}
	if err := r.Pull(ctx, fid); err != nil {
		return err
	}
	return nil
}

func (r *replicator[T]) Push(ctx context.Context, fid int) error {
	dag, err := r.params.View(ctx, fid)
	if err != nil {
		return err
	}
	peers, err := dag.ListPeers(ctx)
	if err != nil {
		return err
	}
	epoch, err := dag.GetEpoch(ctx)
	if err != nil {
		return err
	}
	heads := dag.GetHeads()
	eg := errgroup.Group{}
	errs := make([]error, len(peers))
	for i, dst := range peers {
		i, dst := i, dst
		eg.Go(func() error {
			errs[i] = r.params.Push(ctx, dst, *epoch, heads)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	if count := countNils(errs); count <= (len(peers)-1)/2 {
		return fmt.Errorf("push failed")
	}
	return nil
}

func (r *replicator[T]) Pull(ctx context.Context, id int) error {
	dag, err := r.params.View(ctx, id)
	if err != nil {
		return err
	}
	peers, err := dag.ListPeers(ctx)
	if err != nil {
		return err
	}
	epoch, err := dag.GetEpoch(ctx)
	if err != nil {
		return err
	}
	eg := errgroup.Group{}
	errs := make([]error, len(peers))
	for i, dst := range peers {
		i, dst := i, dst
		eg.Go(func() error {
			heads, err := r.params.GetHeads(ctx, dst, *epoch)
			if err != nil {
				errs[i] = err
				return nil
			}
			logctx.Infof(ctx, "heads: %v", heads)
			return nil
		})
	}
	return eg.Wait()
}

func (r *replicator[T]) HandlePush(ctx context.Context, src PeerID, volID int, heads []owldag.Head) error {
	peerStore, err := r.params.GetStore(src)
	if err != nil {
		return err
	}
	for _, h := range heads {
		if err := r.params.Modify(ctx, volID, func(dag *owldag.DAG[T]) error {
			return dag.Pull(ctx, peerStore, h)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *replicator[T]) HandleGet(ctx context.Context, src PeerID, volID int) ([]owldag.Head, error) {
	dag, err := r.params.View(ctx, volID)
	if err != nil {
		return nil, err
	}
	if yes, err := dag.CanRead(ctx, src); err != nil {
		return nil, err
	} else if !yes {
		return nil, owlnet.NewError(codes.PermissionDenied, "no read access")
	}
	return dag.GetHeads(), nil
}

func countNils(errs []error) (ret int) {
	for i := range errs {
		if errs[i] == nil {
			ret++
		}
	}
	return ret
}
