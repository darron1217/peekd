package repository

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pkg/errors"
)

// clickhouseConfig holds ClickHouse connection configuration
type clickhouseConfig struct {
	Host     string
	Port     uint16
	Database string
	Username string
	Password string
	Secure   bool
}

// ClickHouseRepository implements the Repository interface for ClickHouse
type ClickHouseRepository struct {
	conn driver.Conn
}

// NewClickHouseRepository creates a new ClickHouse repository instance
func NewClickHouseRepository(cfg clickhouseConfig) (*ClickHouseRepository, error) {
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

	if cfg.Secure {
		options.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to ClickHouse")
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, errors.Wrap(err, "failed to ping ClickHouse")
	}

	return &ClickHouseRepository{
		conn: conn,
	}, nil
}

// SaveGeneralMessageHistory saves a general message history record to the database
func (r *ClickHouseRepository) SaveGeneralMessageHistory(ctx context.Context, history *GeneralMessageHistory) error {
	if history == nil {
		return errors.New("history is nil")
	}

	query := `
		INSERT INTO general_message_histories (
			arrival_time,
			fork_version,
			topic,
			node_region,
			node_alias,
			node_peer_count,
			message_id,
			size_bytes
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?
		)
	`

	err := r.conn.Exec(ctx, query,
		history.ArrivalTime,
		history.ForkVersion,
		history.Topic,
		history.NodeRegion,
		history.NodeAlias,
		history.NodePeerCount,
		history.MessageID,
		history.SizeBytes,
	)

	if err != nil {
		return errors.Wrap(err, "failed to save general message history")
	}

	return nil
}

// SaveSlotMessageStatsMulti saves multiple slot message stats records in a batch
func (r *ClickHouseRepository) SaveSlotMessageStatsMulti(ctx context.Context, stats []*SlotMessageStats) error {
	if len(stats) == 0 {
		return nil
	}

	batch, err := r.conn.PrepareBatch(ctx, "INSERT INTO slot_message_stats")
	if err != nil {
		return errors.Wrap(err, "failed to prepare batch")
	}

	for _, stat := range stats {
		err := batch.Append(
			stat.Slot,
			stat.TopicGroup,
			stat.ForkVersion,
			stat.Topic,
			stat.NodeRegion,
			stat.NodeAlias,
			stat.NodePeerCount,
			stat.MessageID,
			stat.SlotStartTime,
			stat.FirstArrivalTime,
			stat.LatencyMS,
			stat.SizeBytes,
			stat.SeenCount,
		)
		if err != nil {
			return errors.Wrap(err, "failed to append message stats")
		}
	}

	if err := batch.Send(); err != nil {
		return errors.Wrap(err, "failed to send batch")
	}

	return nil
}

// Close closes the ClickHouse connection
func (r *ClickHouseRepository) Close() error {
	return r.conn.Close()
}
