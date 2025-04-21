package repository

import (
	"context"
	"log/slog"

	"github.com/pkg/errors"
)

// Repository defines the interface for data storage operations
type Repository interface {
	// SaveGeneralMessageHistory saves a general message history record to the database
	SaveGeneralMessageHistory(ctx context.Context, history *GeneralMessageHistory) error

	// SaveSlotMessageStatsMulti saves multiple message statistics records in a batch
	SaveSlotMessageStatsMulti(ctx context.Context, stats []*SlotMessageStats) error

	// Close closes the database connection
	Close() error
}

// RepositoryType defines the type of database to use
type RepositoryType string

const (
	RepositoryTypeClickHouse RepositoryType = "clickhouse"
	// Additional repository types can be added here in the future
)

// adapter is a wrapper that adapts different database implementations to the Repository interface
type adapter struct {
	chRepo *ClickHouseRepository
}

type RepositoryOption struct {
	dbType     string
	dbName     string
	dbHost     string
	dbPort     int
	dbUser     string
	dbPassword string
}

type RepositoryOptionFunc func(*RepositoryOption)

func WithDBType(dbType string) RepositoryOptionFunc {
	return func(o *RepositoryOption) {
		o.dbType = dbType
	}
}

func WithDBName(dbName string) RepositoryOptionFunc {
	return func(o *RepositoryOption) {
		o.dbName = dbName
	}
}

func WithDBHost(dbHost string) RepositoryOptionFunc {
	return func(o *RepositoryOption) {
		o.dbHost = dbHost
	}
}

func WithDBPort(dbPort int) RepositoryOptionFunc {
	return func(o *RepositoryOption) {
		o.dbPort = dbPort
	}
}

func WithDBUser(dbUser string) RepositoryOptionFunc {
	return func(o *RepositoryOption) {
		o.dbUser = dbUser
	}
}

func WithDBPassword(dbPassword string) RepositoryOptionFunc {
	return func(o *RepositoryOption) {
		o.dbPassword = dbPassword
	}
}

// NewRepository creates a new repository based on the provided configuration
func NewRepository(opts ...RepositoryOptionFunc) (Repository, error) {
	o := &RepositoryOption{
		dbType:     "clickhouse",
		dbName:     "peekd",
		dbHost:     "localhost",
		dbPort:     9000,
		dbUser:     "default",
		dbPassword: "",
	}

	for _, opt := range opts {
		opt(o)
	}

	switch o.dbType {
	case "clickhouse":
		chConfig := clickhouseConfig{
			Host:     o.dbHost,
			Port:     uint16(o.dbPort),
			Database: o.dbName,
			Username: o.dbUser,
			Password: o.dbPassword,
		}

		chRepo, err := NewClickHouseRepository(chConfig)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create ClickHouse repository")
		}

		slog.With("db_name", o.dbName).
			Info("successfully created clickhouse repository")

		return &adapter{chRepo: chRepo}, nil
	default:
		return nil, errors.Errorf("unsupported database type: %s", o.dbType)
	}
}

// SaveGeneralMessageHistory adapter method for ClickHouse
func (a *adapter) SaveGeneralMessageHistory(ctx context.Context, history *GeneralMessageHistory) error {
	return a.chRepo.SaveGeneralMessageHistory(ctx, history)
}

// SaveSlotMessageStatsMulti adapter method for ClickHouse
func (a *adapter) SaveSlotMessageStatsMulti(ctx context.Context, stats []*SlotMessageStats) error {
	return a.chRepo.SaveSlotMessageStatsMulti(ctx, stats)
}

// Close the database connection
func (a *adapter) Close() error {
	return a.chRepo.Close()
}
