package owl

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/brendoncarroll/go-state/cadata"
	"github.com/brendoncarroll/stdctx/logctx"
	"github.com/owlmessenger/owl/pkg/owldag"
	"github.com/owlmessenger/owl/pkg/owlnet"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
)

type replicatorParams struct {
	Context context.Context

	View    func(context.Context, int) (string, *owldag.DAG[json.RawMessage], error)
	Modify  func(context.Context, int, func(dag *owldag.DAG[json.RawMessage]) error) error
	GetNode func(ctx context.Context) (*owlnet.Node, error)
}

type replicator struct {
	params replicatorParams

	ctx context.Context
	cf  context.CancelFunc
	eg  errgroup.Group
}

func newReplicator(params replicatorParams) *replicator {
	ctx, cf := context.WithCancel(params.Context)
	r := &replicator{
		params: params,

		ctx: ctx,
		cf:  cf,
	}
	return r
}

func (r *replicator) Close() error {
	r.cf()
	return r.eg.Wait()
}

// Notify tells the replicator that a dag has changed so it will begin replicating it.
func (r *replicator) Notify(vid int) {
	logctx.Infof(r.ctx, "volume %d has changed. syncing...", vid)
	// TODO: use a queue
	r.eg.Go(func() error {
		if err := r.Sync(r.ctx, vid); err != nil {
			logctx.Errorf(r.ctx, "replicator.Push %v", err)
		}
		return nil
	})
}

// Wait blocks on the DAG in vid, until cond(dag_state) == false
func (r *replicator) Wait(ctx context.Context, vid int, cond func(json.RawMessage) bool) error {
	// TODO:
	return nil
}

// Sync blocks until a pull and push have been performed sucessfully.
func (r *replicator) Sync(ctx context.Context, fid int) error {
	if err := r.Push(ctx, fid); err != nil {
		return err
	}
	if err := r.Pull(ctx, fid); err != nil {
		return err
	}
	return nil
}

func (r *replicator) Push(ctx context.Context, vid int) error {
	scheme, dag, err := r.params.View(ctx, vid)
	if err != nil {
		return err
	}
	epoch, err := dag.GetEpoch(ctx)
	if err != nil {
		return err
	}
	peers, err := dag.ListPeers(ctx)
	if err != nil {
		return err
	}
	heads := dag.GetHeads()
	node, err := r.params.GetNode(ctx)
	if err != nil {
		return err
	}
	dagClient := node.DAGClient()
	eg := errgroup.Group{}
	errs := make([]error, len(peers))
	for i, dst := range peers {
		i, dst := i, dst
		eg.Go(func() error {
			errs[i] = func() error {
				info, err := dagClient.Get(ctx, dst, scheme, []owldag.Ref{*epoch})
				if err != nil {
					return nil
				}
				return dagClient.PushHeads(ctx, dst, uint32(vid), info.Handle, heads)
			}()
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

func (r *replicator) Pull(ctx context.Context, id int) error {
	scheme, dag, err := r.params.View(ctx, id)
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
	node, err := r.params.GetNode(ctx)
	if err != nil {
		return err
	}
	dagClient := node.DAGClient()
	eg := errgroup.Group{}
	errs := make([]error, len(peers))
	heads := make([][]owldag.Head, len(peers))
	for i, dst := range peers {
		i, dst := i, dst
		eg.Go(func() error {
			heads[i], errs[i] = func() ([]owldag.Head, error) {
				info, err := dagClient.Get(ctx, dst, scheme, []owldag.Ref{*epoch})
				if err != nil {
					return nil, err
				}
				return dagClient.GetHeads(ctx, dst, info.Handle)
			}()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func (r *replicator) HandlePush(ctx context.Context, src PeerID, volID int, srcDAG owlnet.Handle, heads []owldag.Head) error {
	node, err := r.params.GetNode(ctx)
	if err != nil {
		return err
	}
	peerStore := owlnet.NewStore(node.BlobPullClient(), src, srcDAG)
	for _, h := range heads {
		if err := r.params.Modify(ctx, volID, func(dag *owldag.DAG[json.RawMessage]) error {
			return dag.Pull(ctx, peerStore, h)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *replicator) HandleGet(ctx context.Context, src PeerID, volID int) ([]owldag.Head, error) {
	_, dag, err := r.params.View(ctx, volID)
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

var _ owldag.Scheme[json.RawMessage] = &Scheme[int]{}

type Scheme[T any] struct {
	inner owldag.Scheme[T]
}

func wrapScheme[T any](inner owldag.Scheme[T]) owldag.Scheme[json.RawMessage] {
	return Scheme[T]{inner}
}

func (sch Scheme[T]) Validate(ctx context.Context, s cadata.Getter, consult owldag.ConsultFunc, data json.RawMessage) error {
	var x T
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	return sch.inner.Validate(ctx, s, consult, x)
}

func (sch Scheme[T]) ValidateStep(ctx context.Context, s cadata.Getter, consult owldag.ConsultFunc, d1, d2 json.RawMessage) error {
	var prev, next T
	if err := json.Unmarshal(d1, &prev); err != nil {
		return err
	}
	if err := json.Unmarshal(d2, &next); err != nil {
		return err
	}
	return sch.inner.ValidateStep(ctx, s, consult, prev, next)
}

func (sch Scheme[T]) Merge(ctx context.Context, s cadata.Store, datas []json.RawMessage) (*json.RawMessage, error) {
	xs := make([]T, len(datas))
	for i := range xs {
		var err error
		if err = json.Unmarshal(datas[i], &xs[i]); err != nil {
			return nil, err
		}
	}
	y, err := sch.inner.Merge(ctx, s, xs)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(y)
	if err != nil {
		return nil, err
	}
	return (*json.RawMessage)(&data), nil
}

func (sch Scheme[T]) CanRead(ctx context.Context, s cadata.Getter, data json.RawMessage, peer PeerID) (bool, error) {
	var x T
	if err := json.Unmarshal(data, &x); err != nil {
		return false, err
	}
	return sch.inner.CanRead(ctx, s, x, peer)
}

func (sch Scheme[T]) ListPeers(ctx context.Context, s cadata.Getter, data json.RawMessage) ([]PeerID, error) {
	var x T
	if err := json.Unmarshal(data, &x); err != nil {
		return nil, err
	}
	return sch.inner.ListPeers(ctx, s, x)
}

func (sch Scheme[T]) Sync(ctx context.Context, src cadata.Getter, dst cadata.Store, data json.RawMessage) error {
	var x T
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	return sch.inner.Sync(ctx, src, dst, x)
}
