package spiffetls

import (
	"errors"
	"net"
	"sync"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
)

// ConnTracker tracks active TLS connections created by Dial, DialWithMode,
// NewListenerWithMode and ListenWithMode so that they can be closed when the
// workload identity presented by the local X509-SVID source changes.
//
// A TLS connection pins the identity established during its handshake:
// rotating the SVID only affects connections established afterwards, while
// existing connections keep presenting the old identity until they happen to
// close. ConnTracker closes the tracked connections whenever the local source
// reports an update carrying a different SPIFFE ID, giving the application a
// deterministic point at which to reconnect and re-handshake with the new
// identity.
//
// A ConnTracker must be created with NewConnTracker and stopped with Stop
// once no longer needed. The same tracker can be shared across multiple
// dialers and listeners backed by the same source.
type ConnTracker struct {
	source *workloadapi.X509Source

	stopCh chan struct{}
	doneCh chan struct{}

	stopOnce sync.Once

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// NewConnTracker creates a ConnTracker for connections backed by the given
// X509 source. It spawns a goroutine that watches the source for SVID
// updates and closes tracked connections when the local SPIFFE ID changes.
func NewConnTracker(source *workloadapi.X509Source) *ConnTracker {
	tracker := &ConnTracker{
		source: source,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
		conns:  make(map[net.Conn]struct{}),
	}
	go tracker.watch()
	return tracker
}

// Stop terminates the update watcher. It does not close any connections that
// are still tracked; those remain the caller's responsibility. Stop is
// idempotent and safe to call concurrently.
func (t *ConnTracker) Stop() {
	t.stopOnce.Do(func() {
		close(t.stopCh)
	})
	<-t.doneCh
}

func (t *ConnTracker) watch() {
	defer close(t.doneCh)

	var currentID spiffeid.ID
	if svid, err := t.source.GetX509SVID(); err == nil {
		currentID = svid.ID
	}

	for {
		select {
		case <-t.stopCh:
			return
		case <-t.source.Updated():
			svid, err := t.source.GetX509SVID()
			if err != nil {
				continue
			}
			if svid.ID == currentID {
				continue
			}
			currentID = svid.ID
			t.closeAll()
		}
	}
}

func (t *ConnTracker) closeAll() {
	t.mu.Lock()
	conns := make([]net.Conn, 0, len(t.conns))
	for conn := range t.conns {
		conns = append(conns, conn)
	}
	t.mu.Unlock()

	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (t *ConnTracker) track(conn net.Conn) net.Conn {
	t.mu.Lock()
	t.conns[conn] = struct{}{}
	t.mu.Unlock()
	return &trackedConn{Conn: conn, tracker: t}
}

func (t *ConnTracker) untrack(conn net.Conn) {
	t.mu.Lock()
	delete(t.conns, conn)
	t.mu.Unlock()
}

// trackedConn removes the connection from the tracker when it is closed, so
// connections closed by the application are not retained in the tracker's set.
type trackedConn struct {
	net.Conn
	tracker  *ConnTracker
	once     sync.Once
	closeErr error
}

func (c *trackedConn) Close() error {
	c.once.Do(func() {
		c.tracker.untrack(c.Conn)
		c.closeErr = c.Conn.Close()
	})
	return c.closeErr
}

// PeerID preserves the PeerID getter exposed by the spiffetls connection
// types so that PeerIDFromConn keeps working on tracked connections.
func (c *trackedConn) PeerID() (spiffeid.ID, error) {
	if getter, ok := c.Conn.(PeerIDGetter); ok {
		return getter.PeerID()
	}
	return spiffeid.ID{}, wrapSpiffetlsErr(errors.New("connection does not expose peer ID"))
}
