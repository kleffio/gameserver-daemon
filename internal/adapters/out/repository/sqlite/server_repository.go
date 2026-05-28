package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/kleffio/kleff-daemon/internal/application/ports"
)

type ServerRepository struct {
	db *sql.DB
}

func NewServerRepository(db *sql.DB) *ServerRepository {
	return &ServerRepository{db: db}
}

func (r *ServerRepository) Save(ctx context.Context, s *ports.ServerRecord) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO servers (id, name, address, port, status, node_id, runtime, runtime_ref, project_id, environment_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name            = excluded.name,
			address         = excluded.address,
			port            = excluded.port,
			status          = excluded.status,
			node_id         = excluded.node_id,
			runtime         = excluded.runtime,
			runtime_ref     = excluded.runtime_ref,
			project_id      = excluded.project_id,
			environment_id  = excluded.environment_id,
			updated_at      = CURRENT_TIMESTAMP
	`, s.ID, s.Name, s.Address, s.Port, s.Status, s.NodeID, s.Runtime, s.RuntimeRef, s.ProjectID, s.EnvironmentID)
	return err
}

func (r *ServerRepository) FindByID(ctx context.Context, id string) (*ports.ServerRecord, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, name, address, port, status, node_id, runtime, runtime_ref, project_id, environment_id
		FROM servers WHERE id = ?
	`, id)
	s := &ports.ServerRecord{}
	err := row.Scan(&s.ID, &s.Name, &s.Address, &s.Port, &s.Status, &s.NodeID, &s.Runtime, &s.RuntimeRef, &s.ProjectID, &s.EnvironmentID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("server not found: %s", id)
	}
	return s, err
}

func (r *ServerRepository) UpdateStatus(ctx context.Context, id string, status string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE servers SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
	`, status, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("server not found: %s", id)
	}
	return nil
}

func (r *ServerRepository) ListAll(ctx context.Context) ([]*ports.ServerRecord, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, address, port, status, node_id, runtime, runtime_ref, project_id, environment_id
		FROM servers
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ports.ServerRecord
	for rows.Next() {
		s := &ports.ServerRecord{}
		if err := rows.Scan(&s.ID, &s.Name, &s.Address, &s.Port, &s.Status, &s.NodeID, &s.Runtime, &s.RuntimeRef, &s.ProjectID, &s.EnvironmentID); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
