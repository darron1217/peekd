package repository

import (
	"context"
	"github.com/pkg/errors"
	"log/slog"

	"github.com/a41-official/peekd/repository/clickhouse"
)

// Repository defines the interface for data storage operations
type Repository interface {
	// SaveMessageStats saves a message statistics record to the database
	SaveMessageStats(ctx context.Context, stats *MessageStats) error

	// SaveMessageStatsMulti saves multiple message statistics records in a batch
	SaveMessageStatsMulti(ctx context.Context, stats []*MessageStats) error

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
	chRepo *clickhouse.ClickHouseRepository
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
		chConfig := clickhouse.Config{
			Host:     o.dbHost,
			Port:     uint16(o.dbPort),
			Database: o.dbName,
			Username: o.dbUser,
			Password: o.dbPassword,
		}

		chRepo, err := clickhouse.NewClickHouseRepository(chConfig)
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

// SaveMessageStats adapter method for ClickHouse
func (a *adapter) SaveMessageStats(ctx context.Context, stats *MessageStats) error {
	// Convert to ClickHouse-specific model
	chStats := &clickhouse.MessageStats{
		Slot:             stats.Slot,
		Topic:            stats.Topic,
		NodeRegion:       stats.NodeRegion,
		MessageID:        stats.MessageID,
		SlotStartTime:    stats.SlotStartTime,
		FirstArrivalTime: stats.FirstArrivalTime,
		LatencyMS:        stats.LatencyMS,
		SizeBytes:        stats.SizeBytes,
		SeenCount:        stats.SeenCount,
		NodeID:           stats.NodeID,
	}
	return a.chRepo.SaveMessageStats(ctx, chStats)
}

// SaveMessageStatsMulti adapter method for ClickHouse
func (a *adapter) SaveMessageStatsMulti(ctx context.Context, stats []*MessageStats) error {
	// Convert to ClickHouse-specific model
	chStats := make([]*clickhouse.MessageStats, len(stats))
	for i, stat := range stats {
		chStats[i] = &clickhouse.MessageStats{
			Slot:             stat.Slot,
			Topic:            stat.Topic,
			NodeRegion:       stat.NodeRegion,
			MessageID:        stat.MessageID,
			SlotStartTime:    stat.SlotStartTime,
			FirstArrivalTime: stat.FirstArrivalTime,
			LatencyMS:        stat.LatencyMS,
			SizeBytes:        stat.SizeBytes,
			SeenCount:        stat.SeenCount,
			NodeID:           stat.NodeID,
		}
	}
	return a.chRepo.SaveMessageStatsMulti(ctx, chStats)
}

// Close the database connection
func (a *adapter) Close() error {
	return a.chRepo.Close()
}
