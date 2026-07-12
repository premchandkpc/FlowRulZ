package common

import "crypto/tls"

// TLSCipherSuites is the project-wide allowlist of TLS cipher suites.
// Only ECDHE+AES-GCM suites are permitted (no CBC, no 3DES).
var TLSCipherSuites = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
}

// MaxRequestBodySize is the default limit for incoming request bodies.
const MaxRequestBodySize = 10 * 1024 * 1024 // 10 MB
