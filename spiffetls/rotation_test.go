package spiffetls_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/internal/test"
	"github.com/spiffe/go-spiffe/v2/internal/test/fakeworkloadapi"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type rotationEnv struct {
	ca           *test.CA
	serverAPI    *fakeworkloadapi.WorkloadAPI
	clientAPI    *fakeworkloadapi.WorkloadAPI
	serverSource *workloadapi.X509Source
	clientSource *workloadapi.X509Source
}

func setupRotationEnv(t *testing.T) *rotationEnv {
	t.Helper()

	ca := test.NewCA(t, td)

	serverAPI := fakeworkloadapi.New(t)
	t.Cleanup(serverAPI.Stop)
	setWorkloadAPIResponse(ca, serverAPI, serverID)

	clientAPI := fakeworkloadapi.New(t)
	t.Cleanup(clientAPI.Stop)
	setWorkloadAPIResponse(ca, clientAPI, clientID)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	serverClient, err := workloadapi.New(ctx, workloadapi.WithAddr(serverAPI.Addr()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = serverClient.Close() })

	clientClient, err := workloadapi.New(ctx, workloadapi.WithAddr(clientAPI.Addr()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientClient.Close() })

	serverSource, err := workloadapi.NewX509Source(ctx, workloadapi.WithClient(serverClient))
	require.NoError(t, err)
	t.Cleanup(func() { _ = serverSource.Close() })

	clientSource, err := workloadapi.NewX509Source(ctx, workloadapi.WithClient(clientClient))
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientSource.Close() })

	return &rotationEnv{
		ca:           ca,
		serverAPI:    serverAPI,
		clientAPI:    clientAPI,
		serverSource: serverSource,
		clientSource: clientSource,
	}
}

// TestConnTrackerClosesClientConnOnIdentityRotation verifies that a
// connection established with an old identity is closed once the workload's
// X509-SVID source rotates to a different SPIFFE ID, and that a subsequent
// connection presents the rotated identity.
func TestConnTrackerClosesClientConnOnIdentityRotation(t *testing.T) {
	env := setupRotationEnv(t)

	listener, err := spiffetls.ListenWithMode(
		context.Background(), "tcp", "localhost:0",
		spiffetls.MTLSServerWithSource(tlsconfig.AuthorizeMemberOf(td), env.serverSource),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	serverConnCh := acceptLoop(t, listener)

	tracker := spiffetls.NewConnTracker(env.clientSource)
	t.Cleanup(tracker.Stop)

	conn, err := spiffetls.DialWithMode(
		context.Background(), "tcp", listener.Addr().String(),
		spiffetls.MTLSClientWithSource(tlsconfig.AuthorizeID(serverID), env.clientSource),
		spiffetls.WithDialConnTracker(tracker),
	)
	require.NoError(t, err)

	var serverConn net.Conn
	select {
	case serverConn = <-serverConnCh:
		require.NotNil(t, serverConn)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for server to accept the connection")
	}

	peerID, err := spiffetls.PeerIDFromConn(serverConn)
	require.NoError(t, err)
	require.Equal(t, clientID, peerID)

	// Rotate the client workload to a new identity.
	rotatedClientID := spiffeid.RequireFromString("spiffe://example.org/rotated-client-workload")
	setWorkloadAPIResponse(env.ca, env.clientAPI, rotatedClientID)

	// The connection pinned at handshake time must be closed rather than
	// keep presenting the old identity until its certificate expires.
	requireClosed(t, conn)

	// A new connection after rotation presents the new identity.
	rotatedConn, err := spiffetls.DialWithMode(
		context.Background(), "tcp", listener.Addr().String(),
		spiffetls.MTLSClientWithSource(tlsconfig.AuthorizeID(serverID), env.clientSource),
		spiffetls.WithDialConnTracker(tracker),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rotatedConn.Close() })

	select {
	case nextConn := <-serverConnCh:
		require.NotNil(t, nextConn)
		t.Cleanup(func() { _ = nextConn.Close() })
		nextPeerID, peerErr := spiffetls.PeerIDFromConn(nextConn)
		require.NoError(t, peerErr)
		assert.Equal(t, rotatedClientID, nextPeerID)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the rotated connection")
	}
}

// TestConnTrackerClosesServerConnOnIdentityRotation verifies that accepted
// connections are closed when the server-side X509-SVID source rotates to a
// different SPIFFE ID.
func TestConnTrackerClosesServerConnOnIdentityRotation(t *testing.T) {
	env := setupRotationEnv(t)

	tracker := spiffetls.NewConnTracker(env.serverSource)
	t.Cleanup(tracker.Stop)

	listener, err := spiffetls.ListenWithMode(
		context.Background(), "tcp", "localhost:0",
		spiffetls.MTLSServerWithSource(tlsconfig.AuthorizeID(clientID), env.serverSource),
		spiffetls.WithListenConnTracker(tracker),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	serverConnCh := acceptLoop(t, listener)

	conn, err := spiffetls.DialWithMode(
		context.Background(), "tcp", listener.Addr().String(),
		spiffetls.MTLSClientWithSource(tlsconfig.AuthorizeID(serverID), env.clientSource),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	var serverConn net.Conn
	select {
	case serverConn = <-serverConnCh:
		require.NotNil(t, serverConn)
		t.Cleanup(func() { _ = serverConn.Close() })
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for server to accept the connection")
	}

	// Rotate the server workload identity.
	rotatedServerID := spiffeid.RequireFromString("spiffe://example.org/rotated-server-workload")
	setWorkloadAPIResponse(env.ca, env.serverAPI, rotatedServerID)

	requireClosed(t, serverConn)
	requireClosed(t, conn)
}

// TestConnTrackerKeepsConnWhenIdentityUnchanged verifies that a certificate
// renewal that keeps the same SPIFFE ID does not tear down the connection.
func TestConnTrackerKeepsConnWhenIdentityUnchanged(t *testing.T) {
	env := setupRotationEnv(t)

	tracker := spiffetls.NewConnTracker(env.clientSource)
	t.Cleanup(tracker.Stop)

	listener, err := spiffetls.ListenWithMode(
		context.Background(), "tcp", "localhost:0",
		spiffetls.MTLSServerWithSource(tlsconfig.AuthorizeMemberOf(td), env.serverSource),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	acceptedCh := acceptLoop(t, listener)

	conn, err := spiffetls.DialWithMode(
		context.Background(), "tcp", listener.Addr().String(),
		spiffetls.MTLSClientWithSource(tlsconfig.AuthorizeID(serverID), env.clientSource),
		spiffetls.WithDialConnTracker(tracker),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	select {
	case accepted := <-acceptedCh:
		require.NotNil(t, accepted)
		t.Cleanup(func() { _ = accepted.Close() })
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for server to accept the connection")
	}

	oldSVID, err := env.clientSource.GetX509SVID()
	require.NoError(t, err)
	oldSerial := oldSVID.Certificates[0].SerialNumber

	// Renew the SVID without changing the SPIFFE ID.
	setWorkloadAPIResponse(env.ca, env.clientAPI, clientID)
	// Wait until the source itself reflects the renewed certificate.
	require.Eventually(t, func() bool {
		svid, svidErr := env.clientSource.GetX509SVID()
		return svidErr == nil && svid.Certificates[0].SerialNumber.Cmp(oldSerial) != 0
	}, 10*time.Second, 50*time.Millisecond)

	// The SPIFFE ID is unchanged, so the connection must stay open; a write
	// should still succeed.
	require.NoError(t, conn.SetWriteDeadline(time.Now().Add(5*time.Second)))
	_, err = conn.Write([]byte("still alive\n"))
	require.NoError(t, err)
}

func requireClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	require.Eventually(t, func() bool {
		buf := make([]byte, 1)
		_ = conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
		_, err := conn.Read(buf)
		return err != nil
	}, 10*time.Second, 100*time.Millisecond, "connection was not closed after identity rotation")
}

func acceptLoop(t *testing.T, listener net.Listener) <-chan net.Conn {
	t.Helper()
	connCh := make(chan net.Conn, 4)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// The TLS listener does not perform the handshake on Accept.
			// Drive it explicitly so the mTLS client's handshake, which
			// waits for the server's CertificateRequest, can complete.
			if _, peerErr := spiffetls.PeerIDFromConn(conn); peerErr != nil {
				_ = conn.Close()
				continue
			}
			connCh <- conn
		}
	}()
	return connCh
}
