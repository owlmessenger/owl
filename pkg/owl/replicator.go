package owl

import (
	"context"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/owlmessenger/owl/pkg/feeds"
	"github.com/owlmessenger/owl/pkg/owlnet"
	"golang.org/x/sync/errgroup"
)

type replicatorParams[T any] struct {
	Context     context.Context
	NewProtocol func(s cadata.Store) feeds.Protocol[T]
	GetNode     func(ctx context.Context) (*owlnet.Node, error)

	View    func(context.Context, int) (*feeds.Feed[T], cadata.Store, error)
	Resolve func(context.Context, feeds.ID) (int, error)
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

// Notify tells the replicator that a feed has changed so it will begin replicating it.
func (r *replicator[T]) Notify(fid int) {
	// TODO: use a queue
	go func() {
		if err := r.Sync(r.ctx, fid); err != nil {
			logctx.Errorf(r.ctx, "replicator.Push", err)
		}
	}()
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
	feed, _, err := r.params.View(ctx, fid)
	if err != nil {
		return err
	}
	peers, err := feed.ListPeers(ctx)
	if err != nil {
		return err
	}
	node, err := r.params.GetNode(ctx)
	if err != nil {
		return err
	}
	feedClient := node.FeedsClient()
	eg := errgroup.Group{}
	errs := make([]error, len(peers))
	for i, dst := range peers {
		i, dst := i, dst
		eg.Go(func() error {
			errs[i] = feedClient.PushHeads(ctx, dst, feed.GetRoot(), feed.GetHeads())
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
	feed, _, err := r.params.View(ctx, id)
	if err != nil {
		return err
	}
	peers, err := feed.ListPeers(ctx)
	if err != nil {
		return err
	}
	node, err := r.params.GetNode(ctx)
	if err != nil {
		return err
	}
	feedClient := node.FeedsClient()
	eg := errgroup.Group{}
	errs := make([]error, len(peers))
	for i, dst := range peers {
		i, dst := i, dst
		eg.Go(func() error {
			heads, err := feedClient.GetHeads(ctx, dst, feed.GetRoot())
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

func (r *replicator[T]) HandlePush(ctx context.Context, src PeerID, heads []feeds.Ref) error {
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
