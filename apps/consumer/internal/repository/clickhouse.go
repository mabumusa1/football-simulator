package repository

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/mabumusa1/football-simulator/apps/consumer/internal/domain"
)

var (
	// Prometheus metrics for ClickHouse repository
	clickhouseQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "football_simulator",
			Subsystem: "clickhouse",
			Name:      "query_duration_seconds",
			Help:      "Histogram of ClickHouse query latency in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		},
		[]string{"operation"},
	)

	clickhouseQueryErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "football_simulator",
			Subsystem: "clickhouse",
			Name:      "query_errors_total",
			Help:      "Total number of ClickHouse query errors",
		},
		[]string{"operation"},
	)

	clickhouseBatchSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "football_simulator",
			Subsystem: "clickhouse",
			Name:      "batch_size",
			Help:      "Histogram of batch insert sizes",
			Buckets:   []float64{1, 10, 50, 100, 500, 1000, 5000, 10000},
		},
		[]string{},
	)

	clickhouseEventsInserted = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "football_simulator",
			Subsystem: "clickhouse",
			Name:      "events_inserted_total",
			Help:      "Total number of events inserted into ClickHouse",
		},
	)
)

// ClickHouseRepository handles ClickHouse database operations.
type ClickHouseRepository struct {
	conn   driver.Conn
	logger *slog.Logger
}

// NewClickHouseRepository creates a new ClickHouseRepository instance.
func NewClickHouseRepository(conn driver.Conn, logger *slog.Logger) *ClickHouseRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &ClickHouseRepository{
		conn:   conn,
		logger: logger,
	}
}

// Ping performs a health check on the ClickHouse connection.
func (r *ClickHouseRepository) Ping(ctx context.Context) error {
	startTime := time.Now()
	err := r.conn.Ping(ctx)
	duration := time.Since(startTime)

	clickhouseQueryDuration.WithLabelValues("ping").Observe(duration.Seconds())

	if err != nil {
		r.logger.Error("ClickHouse ping failed",
			slog.Duration("duration", duration),
			slog.String("error", err.Error()),
		)
		clickhouseQueryErrors.WithLabelValues("ping").Inc()
		return fmt.Errorf("ClickHouse ping failed: %w", err)
	}

	r.logger.Debug("ClickHouse ping successful",
		slog.Duration("duration", duration),
	)
	return nil
}

// InsertBatch inserts a batch of events into the football_simulator.match_events table.
// Uses ClickHouse batch insert for optimal performance.
func (r *ClickHouseRepository) InsertBatch(ctx context.Context, events []*domain.Event) error {
	if len(events) == 0 {
		return nil
	}

	startTime := time.Now()

	// Prepare batch insert
	batch, err := r.conn.PrepareBatch(ctx, `
		INSERT INTO football_simulator.match_events (
			event_id,
			match_id,
			event_type,
			team_id,
			player_id,
			metadata,
			timestamp
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		r.logger.Error("failed to prepare batch insert",
			slog.String("error", err.Error()),
		)
		clickhouseQueryErrors.WithLabelValues("insert_batch_prepare").Inc()
		return fmt.Errorf("failed to prepare batch insert: %w", err)
	}

	// Append each event to the batch
	for _, event := range events {
		if event == nil {
			continue
		}

		// Convert TeamID to string for ClickHouse schema
		teamIDStr := strconv.Itoa(event.TeamID)

		// Handle nullable player_id - use pointer for nullable string
		var playerID *string
		if event.PlayerID != "" {
			playerID = &event.PlayerID
		}

		// Serialize metadata to JSON string
		metadataJSON := event.MetadataJSON()

		err = batch.Append(
			event.EventID,
			event.MatchID,
			string(event.EventType),
			teamIDStr,
			playerID,
			metadataJSON,
			event.Timestamp,
		)
		if err != nil {
			r.logger.Warn("failed to append event to batch",
				slog.String("event_id", event.EventID.String()),
				slog.String("error", err.Error()),
			)
			// Continue with other events rather than failing the entire batch
			continue
		}
	}

	// Send the batch
	err = batch.Send()
	duration := time.Since(startTime)

	// Record metrics
	clickhouseQueryDuration.WithLabelValues("insert_batch").Observe(duration.Seconds())
	clickhouseBatchSize.WithLabelValues().Observe(float64(len(events)))

	if err != nil {
		r.logger.Error("failed to send batch insert",
			slog.Int("batch_size", len(events)),
			slog.Duration("duration", duration),
			slog.String("error", err.Error()),
		)
		clickhouseQueryErrors.WithLabelValues("insert_batch_send").Inc()
		return fmt.Errorf("failed to send batch insert: %w", err)
	}

	r.logger.Debug("successfully inserted batch",
		slog.Int("batch_size", len(events)),
		slog.Duration("duration", duration),
	)
	clickhouseEventsInserted.Add(float64(len(events)))

	return nil
}

// Close closes the ClickHouse connection.
func (r *ClickHouseRepository) Close() error {
	if r.conn == nil {
		return nil
	}
	return r.conn.Close()
}

// ConnectionConfig holds configuration for ClickHouse connection.
type ConnectionConfig struct {
	Hosts           []string
	Database        string
	Username        string
	Password        string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	Debug           bool
}

// DefaultConnectionConfig returns default ClickHouse connection configuration.
func DefaultConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		Hosts:           []string{"localhost:9000"},
		Database:        "football_simulator",
		Username:        "default",
		Password:        "",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		DialTimeout:     10 * time.Second,
		ReadTimeout:     30 * time.Second,
		Debug:           false,
	}
}

// NewConnection creates a new ClickHouse connection with the given configuration.
func NewConnection(cfg ConnectionConfig) (driver.Conn, error) {
	opts := &clickhouse.Options{
		Addr: cfg.Hosts,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:     cfg.DialTimeout,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		Debug:           cfg.Debug,
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}

	return conn, nil
}
