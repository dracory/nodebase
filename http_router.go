package nodebase

import "net/http"

// HTTPRouter is the host's shared-listener seam: an optional router
// the host installs (same optional-field contract as HTTPAuthenticator,
// ContextStore or DeployObserver) so HTTP-serving nodes can mount
// routes on the host's own HTTP server instead of binding a private TCP
// listener — the Node-RED model where an http-in node just names a path
// and serves it on the admin port. Nil — the default — means no shared
// router exists; a node configured for shared-listener mode then fails
// activation with an explicit error rather than silently falling back
// to a private listener (the caller asked for the host's auth/TLS
// surface; serving the route unauthenticated on another port would
// surprise).
//
// Patterns are Go 1.22 ServeMux patterns (e.g. "GET /hello",
// "POST /submit/{job}"). Handle must report a route conflict as an
// error — the host surfaces it as a deploy failure on the second
// registrant. Unregister removes a previously mounted pattern and must
// tolerate patterns that were never mounted (Stop is idempotent).
//
// Routes mounted here serve inside whatever middleware the host
// applies — for the flowadmin adapter that means admin auth when an
// auth file is configured. There is no per-route opt-out in v1.
type HTTPRouter interface {
	// Handle mounts handler for pattern. A conflict with an already
	// mounted route is returned as an error, never panicked.
	Handle(pattern string, handler http.Handler) error
	// Unregister removes the route mounted for pattern (a no-op when
	// the pattern was never mounted).
	Unregister(pattern string)
}
