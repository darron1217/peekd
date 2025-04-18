package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

// Config holds ClickHouse connection configuration
type Config struct {
	Host     string
	Port     uint16
	Database string
	Username string
	Password string
}

// Repository defines the interface for ClickHouse operations
type Repository interface {
	SaveMessageStats(ctx context.Context, stats *MessageStats) error
	SaveMessageStatsMulti(ctx context.Context, stats []*MessageStats) error
	Close() error
}

// ClickHouseRepository implements the Repository interface for ClickHouse
type ClickHouseRepository struct {
	conn driver.Conn
}

// NewClickHouseRepository creates a new ClickHouse repository instance
func NewClickHouseRepository(cfg Config) (*ClickHouseRepository, error) {
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Protocol: clickhouse.Native,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Test connection
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return &ClickHouseRepository{
		conn: conn,
	}, nil
}

// SaveMessageStats saves a single message stats record to ClickHouse
func (r *ClickHouseRepository) SaveMessageStats(ctx context.Context, stats *MessageStats) error {
	query := `
		INSERT INTO message_stats (
			slot, 
			topic, 
			node_region, 
			message_id, 
			slot_start_time, 
			first_arrival_time, 
			latency_ms, 
			size_bytes, 
			seen_count, 
			node_id
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		)
	`

	err := r.conn.Exec(ctx, query,
		stats.Slot,
		stats.Topic,
		stats.NodeRegion,
		stats.MessageID,
		stats.SlotStartTime,
		stats.FirstArrivalTime,
		stats.LatencyMS,
		stats.SizeBytes,
		stats.SeenCount,
		stats.NodeID,
	)

	if err != nil {
		return fmt.Errorf("failed to insert message stats: %w", err)
	}

	return nil
}

// SaveMessageStatsMulti saves multiple message stats records in a batch
func (r *ClickHouseRepository) SaveMessageStatsMulti(ctx context.Context, stats []*MessageStats) error {
	if len(stats) == 0 {
		return nil
	}

	batch, err := r.conn.PrepareBatch(ctx, "INSERT INTO message_stats")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, stat := range stats {
		err := batch.Append(
			stat.Slot,
			stat.Topic,
			stat.NodeRegion,
			stat.MessageID,
			stat.SlotStartTime,
			stat.FirstArrivalTime,
			stat.LatencyMS,
			stat.SizeBytes,
			stat.SeenCount,
			stat.NodeID,
		)
		if err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// Close closes the ClickHouse connection
func (r *ClickHouseRepository) Close() error {
	return r.conn.Close()
}
