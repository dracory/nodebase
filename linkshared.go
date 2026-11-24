package nodebase

import "time"

// linkCallStackKey is the Metadata key holding the link call stack.
const LinkCallStackKey = "linkCallStack"

// defaultLinkCallTimeout bounds the wait for a linkOut return when the
// node configures no explicit timeout.
const DefaultLinkCallTimeout = 30 * time.Second
