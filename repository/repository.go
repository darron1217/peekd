package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/a41-official/peekd/repository/clickhouse"
)

// MessageStats represents message statistics as stored in the database
type MessageStats struct {
	Slot             uint64    // Beacon slot number (6-second interval)
	Topic            string    // e.g., block, attestation, blob
	NodeRegion       string    // e.g., "CENTRAL_EUROPE", "NORTH_AMERICA", "EAST_ASIA", etc.
	MessageID        string    // Unique message identifier
	SlotStartTime    time.Time // Corresponding timestamp for slot
	FirstArrivalTime time.Time // When message was first seen
	LatencyMS        uint32    // Arrival delay from slot start
	SizeBytes        uint32    // Message size in bytes
	SeenCount        uint32    // How many times the message was received in this slot
	NodeID           string    // ID of the watcher node
}

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
	// RepositoryTypeClickHouse indicates using ClickHouse as the storage backend
	RepositoryTypeClickHouse RepositoryType = "clickhouse"
	// Additional repository types can be added here in the future
)

// Config holds configuration options for the repository
type Config struct {
	Type       RepositoryType
	ClickHouse clickhouse.Config
	// Future database configs can be added here
}

// adapter is a wrapper that adapts different database implementations to the Repository interface
type adapter struct {
	chRepo *clickhouse.ClickHouseRepository
}

// NewRepository creates a new repository based on the provided configuration
func NewRepository(cfg Config) (Repository, error) {
	switch cfg.Type {
	case RepositoryTypeClickHouse:
		chRepo, err := clickhouse.NewClickHouseRepository(cfg.ClickHouse)
		if err != nil {
			return nil, fmt.Errorf("failed to create ClickHouse repository: %w", err)
		}
		return &adapter{chRepo: chRepo}, nil
	default:
		return nil, fmt.Errorf("unsupported repository type: %s", cfg.Type)
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
