package spiffetls_test

import (
	"context"
	"crypto/tls"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
)

func ExampleListen_mTLS() {
	td := spiffeid.RequireTrustDomainFromString("example.org")

	listener, err := spiffetls.Listen(context.TODO(), "tcp", ":8443", tlsconfig.AuthorizeMemberOf(td))
	if err != nil {
		// TODO: error handling
	}
	defer listener.Close()
}

func ExampleListen_mTLSCustomTLSConfigBase() {
	td := spiffeid.RequireTrustDomainFromString("example.org")

	baseConfig := &tls.Config{
		// TODO: set up custom configuration. Note that the spiffetls package
		// will override certificate and verification related fields.
		MinVersion: tls.VersionTLS12,
	}

	listener, err := spiffetls.Listen(context.TODO(), "tcp", ":8443", tlsconfig.AuthorizeMemberOf(td), spiffetls.WithListenTLSConfigBase(baseConfig))
	if err != nil {
		// TODO: error handling
	}
	defer listener.Close()
}

func ExampleConnTracker() {
	ctx := context.TODO()
	td := spiffeid.RequireTrustDomainFromString("example.org")

	// The source watches the Workload API for SVID rotations.
	source, err := workloadapi.NewX509Source(ctx)
	if err != nil {
		// TODO: error handling
	}
	defer source.Close()

	// Connections registered with the tracker are closed when the source
	// rotates to a different SPIFFE ID, so they re-handshake with the new
	// identity instead of keeping the identity pinned at connection time.
	tracker := spiffetls.NewConnTracker(source)
	defer tracker.Stop()

	listener, err := spiffetls.ListenWithMode(
		ctx, "tcp", ":8443",
		spiffetls.MTLSServerWithSource(tlsconfig.AuthorizeMemberOf(td), source),
		spiffetls.WithListenConnTracker(tracker),
	)
	if err != nil {
		// TODO: error handling
	}
	defer listener.Close()

	conn, err := spiffetls.DialWithMode(
		ctx, "tcp", "server.example:8443",
		spiffetls.MTLSClientWithSource(tlsconfig.AuthorizeMemberOf(td), source),
		spiffetls.WithDialConnTracker(tracker),
	)
	if err != nil {
		// TODO: error handling
	}
	defer conn.Close()
}
