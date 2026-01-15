package repository

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/mabumusa1/football-simulator/apps/api/internal/domain"
)

var (
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
)

// ClickHouseRepository handles read-only ClickHouse database operations for metrics queries.
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

// GetMatchMetrics retrieves aggregated metrics for a specific match.
func (r *ClickHouseRepository) GetMatchMetrics(ctx context.Context, matchID string) (*domain.MatchMetrics, error) {
	if matchID == "" {
		return nil, fmt.Errorf("matchID cannot be empty")
	}

	startTime := time.Now()
	metrics := domain.NewMatchMetrics(matchID)

	row := r.conn.QueryRow(ctx, `
		SELECT
			count(*) as total_events,
			countIf(event_type = 'goal') as goals,
			countIf(event_type = 'yellow_card') as yellow_cards,
			countIf(event_type = 'red_card') as red_cards,
			min(timestamp) as first_event_at,
			max(timestamp) as last_event_at
		FROM football_simulator.match_events
		WHERE match_id = ?
	`, matchID)

	var totalEvents, goals, yellowCards, redCards uint64
	var firstEventAt, lastEventAt time.Time

	err := row.Scan(&totalEvents, &goals, &yellowCards, &redCards, &firstEventAt, &lastEventAt)
	if err != nil {
		duration := time.Since(startTime)
		r.logger.Error("failed to query match metrics",
			slog.String("match_id", matchID),
			slog.Duration("duration", duration),
			slog.String("error", err.Error()),
		)
		clickhouseQueryErrors.WithLabelValues("get_match_metrics").Inc()
		clickhouseQueryDuration.WithLabelValues("get_match_metrics").Observe(duration.Seconds())
		return nil, fmt.Errorf("failed to query match metrics: %w", err)
	}

	if totalEvents == 0 {
		duration := time.Since(startTime)
		clickhouseQueryDuration.WithLabelValues("get_match_metrics").Observe(duration.Seconds())
		return nil, nil
	}

	metrics.TotalEvents = int64(totalEvents)
	metrics.Goals = int64(goals)
	metrics.YellowCards = int64(yellowCards)
	metrics.RedCards = int64(redCards)

	if !firstEventAt.IsZero() {
		metrics.FirstEventAt = &firstEventAt
	}
	if !lastEventAt.IsZero() {
		metrics.LastEventAt = &lastEventAt
	}

	// Query events by type
	rows, err := r.conn.Query(ctx, `
		SELECT event_type, count(*) as event_count
		FROM football_simulator.match_events
		WHERE match_id = ?
		GROUP BY event_type
		ORDER BY event_count DESC
	`, matchID)
	if err != nil {
		duration := time.Since(startTime)
		r.logger.Error("failed to query events by type",
			slog.String("match_id", matchID),
			slog.Duration("duration", duration),
			slog.String("error", err.Error()),
		)
		clickhouseQueryErrors.WithLabelValues("get_match_metrics_by_type").Inc()
		clickhouseQueryDuration.WithLabelValues("get_match_metrics").Observe(duration.Seconds())
		return nil, fmt.Errorf("failed to query events by type: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var eventType string
		var eventCount uint64
		if err := rows.Scan(&eventType, &eventCount); err != nil {
			r.logger.Warn("failed to scan event type row",
				slog.String("error", err.Error()),
			)
			continue
		}
		metrics.EventsByType[eventType] = int64(eventCount)
	}

	if err := rows.Err(); err != nil {
		duration := time.Since(startTime)
		r.logger.Error("error iterating events by type rows",
			slog.String("match_id", matchID),
			slog.String("error", err.Error()),
		)
		clickhouseQueryErrors.WithLabelValues("get_match_metrics_by_type").Inc()
		clickhouseQueryDuration.WithLabelValues("get_match_metrics").Observe(duration.Seconds())
		return nil, fmt.Errorf("error iterating events by type: %w", err)
	}

	duration := time.Since(startTime)
	clickhouseQueryDuration.WithLabelValues("get_match_metrics").Observe(duration.Seconds())

	r.logger.Debug("successfully retrieved match metrics",
		slog.String("match_id", matchID),
		slog.Int64("total_events", int64(totalEvents)),
		slog.Duration("duration", duration),
	)

	return metrics, nil
}

// GetEventsPerMinute retrieves events aggregated by minute for a specific match.
func (r *ClickHouseRepository) GetEventsPerMinute(ctx context.Context, matchID string) ([]domain.EventsPerMinute, error) {
	if matchID == "" {
		return nil, fmt.Errorf("matchID cannot be empty")
	}

	startTime := time.Now()

	rows, err := r.conn.Query(ctx, `
		SELECT
			toStartOfMinute(timestamp) as minute,
			event_type,
			count(*) as event_count
		FROM football_simulator.match_events
		WHERE match_id = ?
		GROUP BY minute, event_type
		ORDER BY minute ASC, event_type ASC
	`, matchID)
	if err != nil {
		duration := time.Since(startTime)
		r.logger.Error("failed to query events per minute",
			slog.String("match_id", matchID),
			slog.Duration("duration", duration),
			slog.String("error", err.Error()),
		)
		clickhouseQueryErrors.WithLabelValues("get_events_per_minute").Inc()
		clickhouseQueryDuration.WithLabelValues("get_events_per_minute").Observe(duration.Seconds())
		return nil, fmt.Errorf("failed to query events per minute: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var results []domain.EventsPerMinute
	for rows.Next() {
		var minute time.Time
		var eventType string
		var eventCount uint64
		if err := rows.Scan(&minute, &eventType, &eventCount); err != nil {
			r.logger.Warn("failed to scan events per minute row",
				slog.String("error", err.Error()),
			)
			continue
		}
		results = append(results, domain.EventsPerMinute{
			Minute:     minute,
			EventType:  eventType,
			EventCount: int64(eventCount),
		})
	}

	if err := rows.Err(); err != nil {
		duration := time.Since(startTime)
		r.logger.Error("error iterating events per minute rows",
			slog.String("match_id", matchID),
			slog.String("error", err.Error()),
		)
		clickhouseQueryErrors.WithLabelValues("get_events_per_minute").Inc()
		clickhouseQueryDuration.WithLabelValues("get_events_per_minute").Observe(duration.Seconds())
		return nil, fmt.Errorf("error iterating events per minute: %w", err)
	}

	duration := time.Since(startTime)
	clickhouseQueryDuration.WithLabelValues("get_events_per_minute").Observe(duration.Seconds())

	r.logger.Debug("successfully retrieved events per minute",
		slog.String("match_id", matchID),
		slog.Int("result_count", len(results)),
		slog.Duration("duration", duration),
	)

	return results, nil
}
