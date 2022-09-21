package owl

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/owlmessenger/owl/pkg/owldag"
	"github.com/owlmessenger/owl/pkg/owlnet"
	"golang.org/x/sync/errgroup"
)

type replicatorParams[T any] struct {
	Context context.Context
	Scheme  owldag.Scheme[T]
	GetNode func(ctx context.Context) (*owlnet.Node, error)

	Resolve func(context.Context, owldag.Ref) (int, error)

	View   func(context.Context, int) (*owldag.DAG[T], error)
	Modify func(context.Context, int, func(dag *owldag.DAG[T]) error) error
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
func (r *replicator[T]) Notify(fid int) {
	// TODO: use a queue
	go func() {
		if err := r.Sync(r.ctx, fid); err != nil {
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
	node, err := r.params.GetNode(ctx)
	if err != nil {
		return err
	}
	epoch, err := dag.GetEpoch(ctx)
	if err != nil {
		return err
	}
	heads := dag.GetHeads()
	dagClient := node.DAGClient()
	eg := errgroup.Group{}
	errs := make([]error, len(peers))
	for i, dst := range peers {
		i, dst := i, dst
		eg.Go(func() error {
			errs[i] = dagClient.PushHeads(ctx, dst, *epoch, heads)
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
	node, err := r.params.GetNode(ctx)
	if err != nil {
		return err
	}
	epoch, err := dag.GetEpoch(ctx)
	if err != nil {
		return err
	}
	dagClient := node.DAGClient()
	eg := errgroup.Group{}
	errs := make([]error, len(peers))
	for i, dst := range peers {
		i, dst := i, dst
		eg.Go(func() error {
			heads, err := dagClient.GetHeads(ctx, dst, *epoch)
			if err != nil {
				errs[i] = err
				return nil
			}
			logctx.Infof(ctx, "heads: %v", heads)
			// TODO: sync heads
			return nil
		})
	}
	return eg.Wait()
}

func (r *replicator[T]) HandlePush(ctx context.Context, src PeerID, epoch owldag.Ref, heads []owldag.Head) error {
	node, err := r.params.GetNode(ctx)
	if err != nil {
		return err
	}
	volID, err := r.params.Resolve(ctx, epoch)
	if err != nil {
		return err
	}
	peerStore := owlnet.NewStore(node.BlobPullClient(), src)
	for _, h := range heads {
		if err := r.params.Modify(ctx, volID, func(dag *owldag.DAG[T]) error {
			return dag.Pull(ctx, peerStore, h)
		}); err != nil {
			return err
		}
	}
	return nil
}

func countNils(errs []error) (ret int) {
	for i := range errs {
		if errs[i] == nil {
			ret++
		}
	}
	return ret
}
