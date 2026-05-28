package ports

import "context"

type ServerRecord struct {
	ID         string
	Name       string
	Address    string
	Port       int
	Status     string
	NodeID     string
	Runtime    string
	RuntimeRef string
	ProjectID  string
	EnvironmentID string
}

type ServerRepository interface {
	Save(ctx context.Context, server *ServerRecord) error
	FindByID(ctx context.Context, id string) (*ServerRecord, error)
	UpdateStatus(ctx context.Context, id string, status string) error
	ListAll(ctx context.Context) ([]*ServerRecord, error)
}
