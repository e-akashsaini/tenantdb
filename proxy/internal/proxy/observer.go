package proxy

import "github.com/jackc/pgx/v5/pgproto3"

// Session describes one client connection passing through the proxy.
type Session struct {
	ID     uint64
	User   string
	DB     string
	Params map[string]string

	// State is private storage for the Observer, one value per connection.
	// Guard it yourself: both observer callbacks may run concurrently.
	State any
}

// Observer sees every protocol message crossing the proxy. It is how tenant
// attribution plugs in without the relay knowing about tenants.
//
// OnFrontend (client -> database) and OnBackend (database -> client) are called
// from two different goroutines for the same Session, so implementations must be
// safe for concurrent use. Messages are only valid for the duration of the call:
// copy anything you keep.
type Observer interface {
	OnFrontend(s *Session, msg pgproto3.FrontendMessage)
	OnBackend(s *Session, msg pgproto3.BackendMessage)
}

// NopObserver ignores everything.
type NopObserver struct{}

func (NopObserver) OnFrontend(*Session, pgproto3.FrontendMessage) {}
func (NopObserver) OnBackend(*Session, pgproto3.BackendMessage)   {}
