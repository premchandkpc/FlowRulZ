---
title: Admin
tags:
  - module
  - api
  - go
---

# Admin

> [!info] Admin HTTP API
> Path: `server/internal/admin/`

Provides the HTTP REST API for cluster management, rule deployment, and health checks.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Node health (leader/follower status) |
| `POST` | `/rules` | Deploy a rule |
| `DELETE` | `/rules/:id` | Remove a rule |
| `GET` | `/rules` | List deployed rules |
| `GET` | `/cluster` | Cluster status (peers, leader) |
| `POST` | `/execute` | Execute a message against a rule |

## Dependencies

- [[Node]] — delegates to ExecuteAll / engine operations
- [[Cluster]] — cluster status endpoints
