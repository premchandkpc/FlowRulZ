# Admin HTTP API

**Status:** Implemented. The `AdminHTTPServer` (`server/internal/node/admin_http.go`) provides the HTTP API layer for node management.

## Endpoints

### Health & Readiness

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check — returns `status`, `node_id`, `is_leader`, `term` |
| `GET` | `/readyz` | Readiness — returns 503 if leader with `term == 0` (not yet initialized) |

### Service Registry

| Method | Path | Description |
|---|---|---|
| `POST` | `/register` | Service registration |
| `POST` | `/heartbeat` | Service heartbeat |
| `GET` | `/services` | List registered services |

### Cluster Management

| Method | Path | Description | Auth |
|---|---|---|---|
| `POST` | `/cluster/join` | Join node to Raft cluster | Required |

Cluster join is protected by `FLOWRULZ_API_KEY` Bearer token auth with constant-time comparison. Rejects localhost addresses and validates raft address format.

### Metrics

| Method | Path | Description |
|---|---|---|
| `GET` | `/metrics` | Metrics snapshot — includes `pending_requests`, `dlq_size`, `inflight_execs` |

### Executions

| Method | Path | Description |
|---|---|---|
| `DELETE` | `/executions/{id}` | Cancel a running execution by ID |
| `GET` | `/executions` | List all in-flight executions |

### Partitions

| Method | Path | Description |
|---|---|---|
| `GET` | `/partitions` | List partition assignments per node |
| `POST` | `/partitions/rebalance` | Trigger manual partition rebalance (leader-only) |

### Rules (delegated to `admin.Server`)

| Method | Path | Description |
|---|---|---|
| `*` | `/admin/*` | Rules CRUD, validate, promote, rollback, lanes, DLQ |

## Readiness Semantics

`/readyz` returns 503 if the node is leader but `term == 0`. This prevents traffic from being routed to a leader that hasn't completed initialization.

## Security

- `/cluster/join` requires `Authorization: Bearer <key>` where `key` matches `FLOWRULZ_API_KEY`
- Constant-time comparison to prevent timing attacks
- Rejects localhost addresses for cluster join
- All other endpoints are unauthenticated (internal network only)

## Server Lifecycle

- `ServeHTTP()` starts a goroutine with `ListenAndServe`
- `Shutdown()` uses 5-second timeout context for graceful shutdown
- Handler registration in `registerHandlers()` wires all endpoints

## Files

| File | Purpose |
|---|---|
| `node/admin_http.go` | HTTP API server, handlers, lifecycle |
