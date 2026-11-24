package spiffetls

import (
	"crypto/tls"
	"fmt"
	"net"

	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
)

// DialOption is an option for dialing. Option's are also DialOption's.
type DialOption interface {
	apply(*dialConfig)
}

type dialOption func(*dialConfig)

func (fn dialOption) apply(c *dialConfig) {
	fn(c)
}

type dialConfig struct {
	baseTLSConf *tls.Config
	dialer      *net.Dialer
	tlsOptions  []tlsconfig.Option
	connTracker *ConnTracker
}

type listenOption func(*listenConfig)

type listenConfig struct {
	baseTLSConf *tls.Config
	tlsOptions  []tlsconfig.Option
	connTracker *ConnTracker
}

func (fn listenOption) apply(c *listenConfig) {
	fn(c)
}

// WithDialTLSConfigBase provides a base TLS configuration to use. Fields
// related to certificates and verification will be overwritten by this package
// as necessary to facilitate SPIFFE authentication.
func WithDialTLSConfigBase(base *tls.Config) DialOption {
	return dialOption(func(c *dialConfig) {
		c.baseTLSConf = base
	})
}

// WithDialTLSOptions provides options to use for the TLS config.
func WithDialTLSOptions(opts ...tlsconfig.Option) DialOption {
	return dialOption(func(c *dialConfig) {
		c.tlsOptions = opts
	})
}

// WithDialer provides a net dialer to use. If unset, the standard net dialer
// will be used.
func WithDialer(dialer *net.Dialer) DialOption {
	return dialOption(func(c *dialConfig) {
		c.dialer = dialer
	})
}

// WithDialConnTracker tracks the dialed connection with the given ConnTracker.
// When the X509 source watched by the tracker reports a new SPIFFE ID, the
// connection is closed so that the application can reconnect using the
// rotated identity instead of continuing to present the identity pinned
// during the original handshake.
func WithDialConnTracker(tracker *ConnTracker) DialOption {
	return dialOption(func(c *dialConfig) {
		c.connTracker = tracker
	})
}

// ListenOption is an option for listening. Option's are also ListenOption's.
type ListenOption interface {
	apply(*listenConfig)
}

// WithListenTLSConfigBase provides a base TLS configuration to use. Fields
// related to certificates and verification will be overwritten by this package
// as necessary to facilitate SPIFFE authentication.
func WithListenTLSConfigBase(base *tls.Config) ListenOption {
	return listenOption(func(c *listenConfig) {
		c.baseTLSConf = base
	})
}

// WithListenTLSOptions provides options to use when doing Server mTLS.
func WithListenTLSOptions(opts ...tlsconfig.Option) ListenOption {
	return listenOption(func(c *listenConfig) {
		c.tlsOptions = opts
	})
}

// WithListenConnTracker tracks every connection accepted by the listener with
// the given ConnTracker. When the X509 source watched by the tracker reports
// a new SPIFFE ID, the tracked connections are closed so that peers
// reconnect and observe the rotated identity rather than the identity pinned
// during each original handshake.
func WithListenConnTracker(tracker *ConnTracker) ListenOption {
	return listenOption(func(c *listenConfig) {
		c.connTracker = tracker
	})
}

func wrapSpiffetlsErr(err error) error {
	return fmt.Errorf("spiffetls: %w", err)
}
