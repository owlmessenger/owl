package owl

import (
	"context"
	"sync"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.inet256.org/inet256/pkg/inet256"
	"golang.org/x/sync/errgroup"
)

const (
	contactSetScheme = "contactset@v0"
	directoryScheme  = "directory@v0"
	channelScheme    = DirectMessageV0
)

var _ API = &Server{}

type Server struct {
	db      *sqlx.DB
	inet256 inet256.Service

	initOnce sync.Once
	mu       sync.Mutex
	personas map[int]*personaServer
}

func NewServer(db *sqlx.DB, inet256srv inet256.Service) *Server {
	s := &Server{
		db:       db,
		inet256:  inet256srv,
		personas: make(map[int]*personaServer),
	}
	return s
}

func (s *Server) Init(ctx context.Context) (err error) {
	s.initOnce.Do(func() {
		err = func() error {
			if err := setupDB(ctx, s.db); err != nil {
				return err
			}
			return nil
		}()
	})
	return err
}

func (s *Server) Close() (retErr error) {
	s.mu.Lock()
	for _, ps := range s.personas {
		if err := ps.Close(); retErr == nil {
			retErr = err
		}
	}
	s.mu.Unlock()
	s.db.Close()
	return nil
}

func (s *Server) Sync(ctx context.Context, req *SyncReq) error {
	eg := errgroup.Group{}
	for i := range req.Targets {
		target := req.Targets[i]
		eg.Go(func() error {
			switch {
			case target.Contacts != nil:
				ps, err := s.getPersonaServer(ctx, *target.Contacts)
				if err != nil {
					return err
				}
				return ps.syncContacts(ctx)
			case target.Directory != nil:
				ps, err := s.getPersonaServer(ctx, *target.Directory)
				if err != nil {
					return err
				}
				return ps.syncDirectory(ctx)
			case target.Channel != nil:
				ps, err := s.getPersonaServer(ctx, target.Channel.Persona)
				if err != nil {
					return err
				}
				return ps.syncChannel(ctx, target.Channel.Name)
			default:
				logctx.Warnf(ctx, "empty sync target")
				return nil
			}
		})
	}
	return eg.Wait()
}

func (s *Server) Wait(ctx context.Context, req *WaitReq) (*WaitRes, error) {
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *Server) reloadPersona(ctx context.Context, name string) error {
	id, err := s.lookupPersona(s.db, name)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if ps, exists := s.personas[id]; !exists {
		ps.Close()
		delete(s.personas, id)
	}
	s.mu.Unlock()
	_, err = s.getPersonaServer(ctx, name)
	return err
}

func (s *Server) getPersonaServer(ctx context.Context, name string) (*personaServer, error) {
	id, err := s.lookupPersona(s.db, name)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.personas[id]; !exists {
		members, err := getPersonaMembers(s.db, id)
		if err != nil {
			return nil, err
		}
		s.personas[id] = newPersonaServer(s.db, s.inet256, id, members)
	}
	return s.personas[id], nil
}
