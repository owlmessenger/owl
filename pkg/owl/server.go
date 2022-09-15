package owl

import (
	"context"
	"io"
	"sync"

	"github.com/inet256/inet256/pkg/inet256"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/crypto/sha3"
)

const (
	contactSetProto = "contactset@v0"
	directoryProto  = "directory@v0"
	channelProto    = "channel@v0"
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

func (s *Server) Close() error {
	s.mu.Lock()
	for _, ps := range s.personas {
		ps.Close()
	}
	s.mu.Unlock()
	s.db.Close()
	return nil
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

func deriveSeed(seed *[32]byte, other string) *[32]byte {
	var ret [32]byte
	h := sha3.NewShake256()
	h.Write(seed[:])
	h.Write([]byte(other))
	if _, err := io.ReadFull(h, ret[:]); err != nil {
		panic(err)
	}
	return &ret
}
