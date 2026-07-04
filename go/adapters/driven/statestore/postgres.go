// Package statestore implements ports.StateStore with pluggable backends.
package statestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/premchandkpc/FlowRulZ/go/ports"
	_ "github.com/lib/pq"
)

// PostgresStore implements ports.StateStore using PostgreSQL.
// This is the production adapter for HPA-scaled deployments where
// local-disk state would be lost on pod rescheduling.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore creates a new PostgresStore.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// InitSchema creates the execution_states table if it doesn't exist.
func (s *PostgresStore) InitSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS execution_states (
			id TEXT PRIMARY KEY,
			rule_id TEXT,
			version BIGINT,
			plan_bytes BYTEA,
			ctx_bytes BYTEA,
			status INT,
			pending_svc INT,
			pending_body BYTEA,
			error TEXT,
			output BYTEA,
			created_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ
		)
	`)
	return err
}

func (s *PostgresStore) Create(ctx context.Context, state *ports.ExecutionState) error {
	state.CreatedAt = time.Now().UTC()
	state.UpdatedAt = state.CreatedAt

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO execution_states (id, rule_id, version, plan_bytes, ctx_bytes, status, pending_svc, pending_body, error, output, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`,
		state.ID, state.RuleID, state.Version, state.PlanBytes, state.CtxBytes,
		state.Status, state.PendingSvc, state.PendingBody, state.Error, state.Output,
		state.CreatedAt, state.UpdatedAt,
	)
	return err
}

func (s *PostgresStore) Save(ctx context.Context, state *ports.ExecutionState) error {
	state.UpdatedAt = time.Now().UTC()

	_, err := s.db.ExecContext(ctx, `
		UPDATE execution_states
		SET rule_id = $2, version = $3, plan_bytes = $4, ctx_bytes = $5, status = $6,
		    pending_svc = $7, pending_body = $8, error = $9, output = $10, updated_at = $11
		WHERE id = $1
	`,
		state.ID, state.RuleID, state.Version, state.PlanBytes, state.CtxBytes,
		state.Status, state.PendingSvc, state.PendingBody, state.Error, state.Output,
		state.UpdatedAt,
	)
	return err
}

func (s *PostgresStore) Load(ctx context.Context, id string) (*ports.ExecutionState, error) {
	state := &ports.ExecutionState{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, rule_id, version, plan_bytes, ctx_bytes, status, pending_svc, pending_body, error, output, created_at, updated_at
		FROM execution_states WHERE id = $1
	`, id).Scan(
		&state.ID, &state.RuleID, &state.Version, &state.PlanBytes, &state.CtxBytes,
		&state.Status, &state.PendingSvc, &state.PendingBody, &state.Error, &state.Output,
		&state.CreatedAt, &state.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("execution state not found: %s", id)
	}
	return state, err
}

func (s *PostgresStore) ListByStatus(ctx context.Context, statuses ...ports.ExecutionStatus) ([]*ports.ExecutionState, error) {
	query := `SELECT id, rule_id, version, plan_bytes, ctx_bytes, status, pending_svc, pending_body, error, output, created_at, updated_at FROM execution_states`
	args := []interface{}{}

	if len(statuses) > 0 {
		query += ` WHERE status = ANY($1)`
		args = append(args, statuses)
	}

	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*ports.ExecutionState
	for rows.Next() {
		state := &ports.ExecutionState{}
		err := rows.Scan(
			&state.ID, &state.RuleID, &state.Version, &state.PlanBytes, &state.CtxBytes,
			&state.Status, &state.PendingSvc, &state.PendingBody, &state.Error, &state.Output,
			&state.CreatedAt, &state.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, state)
	}
	return result, nil
}

func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM execution_states WHERE id = $1`, id)
	return err
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}

func (s *PostgresStore) SavePending(ctx context.Context, execID string, pendingSvc uint16, pendingBody, ctxBytes []byte) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE execution_states
		SET status = $2, pending_svc = $3, pending_body = $4, ctx_bytes = $5, updated_at = $6
		WHERE id = $1
	`, execID, ports.StatusWaitingForService, pendingSvc, pendingBody, ctxBytes, time.Now().UTC())
	return err
}

func (s *PostgresStore) SaveRunning(ctx context.Context, execID string, ctxBytes []byte) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE execution_states
		SET status = $2, pending_svc = 0, pending_body = NULL, ctx_bytes = $3, updated_at = $4
		WHERE id = $1
	`, execID, ports.StatusRunning, ctxBytes, time.Now().UTC())
	return err
}

func (s *PostgresStore) SaveCompleted(ctx context.Context, execID string, output []byte) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE execution_states
		SET status = $2, output = $3, updated_at = $4
		WHERE id = $1
	`, execID, ports.StatusCompleted, output, time.Now().UTC())
	return err
}

func (s *PostgresStore) SaveFailed(ctx context.Context, execID string, errMsg string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE execution_states
		SET status = $2, error = $3, updated_at = $4
		WHERE id = $1
	`, execID, ports.StatusFailed, errMsg, time.Now().UTC())
	return err
}

// Ensure PostgresStore implements ports.StateStore
var _ ports.StateStore = (*PostgresStore)(nil)