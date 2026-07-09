package common

import (
	"fmt"
	"hash/fnv"
)

// HashBody returns a hex-encoded FNV-128a hash of body.
// Used for deduplication and rate-limit message IDs.
func HashBody(body []byte) string {
	h := fnv.New128a()
	h.Write(body)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// HashBodyPrefixed returns a prefixed hex-encoded FNV-128a hash.
// Use prefix to namespace different hash domains (e.g., "rl" for rate-limit).
func HashBodyPrefixed(prefix string, body []byte) string {
	return fmt.Sprintf("%s-%s", prefix, HashBody(body))
}
