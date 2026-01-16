package repository

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/mabumusa1/football-simulator/apps/consumer/internal/domain"
)

func getClickHouseConnection(t *testing.T) driver.Conn {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "clickhouse"
	}
	port := os.Getenv("CLICKHOUSE_PORT")
	if port == "" {
		port = "9000"
	}

	opts := &clickhouse.Options{
		Addr: []string{host + ":" + port},
		Auth: clickhouse.Auth{
			Database: "football_simulator",
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 10 * time.Second,
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatalf("failed to open ClickHouse connection: %v", err)
	}

	return conn
}

func newIntegrationTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestNewClickHouseRepository_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()

	repo := NewClickHouseRepository(conn, logger)
	if repo == nil {
		t.Fatal("NewClickHouseRepository returned nil")
	}
	if repo.conn == nil {
		t.Error("expected conn to be set")
	}
	if repo.logger == nil {
		t.Error("expected logger to be set")
	}
}

func TestNewClickHouseRepository_NilLogger_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	repo := NewClickHouseRepository(conn, nil)
	if repo == nil {
		t.Fatal("NewClickHouseRepository returned nil")
	}
	if repo.logger == nil {
		t.Error("expected default logger to be set")
	}
}

func TestClickHouseRepository_Ping_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := repo.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_EmptyBatch_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := repo.InsertBatch(ctx, []*domain.Event{})
	if err != nil {
		t.Fatalf("InsertBatch with empty batch should not error: %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_SingleEvent_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "integration-test-match-" + uuid.New().String()[:8],
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
		PlayerID:  "player-123",
		Metadata:  map[string]interface{}{"minute": float64(45)},
	}

	err := repo.InsertBatch(ctx, []*domain.Event{event})
	if err != nil {
		t.Fatalf("InsertBatch failed: %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_MultipleEvents_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	matchID := "integration-test-batch-" + uuid.New().String()[:8]
	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   matchID,
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
			PlayerID:  "player-1",
		},
		{
			EventID:   uuid.New(),
			MatchID:   matchID,
			EventType: domain.EventTypeShot,
			Timestamp: time.Now(),
			TeamID:    2,
			PlayerID:  "player-2",
		},
		{
			EventID:   uuid.New(),
			MatchID:   matchID,
			EventType: domain.EventTypeFoul,
			Timestamp: time.Now(),
			TeamID:    1,
			PlayerID:  "player-3",
		},
	}

	err := repo.InsertBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertBatch failed: %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_WithNilEvents_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "test-match-nil-" + uuid.New().String()[:8],
			EventType: domain.EventTypePass,
			Timestamp: time.Now(),
			TeamID:    1,
		},
		nil, // Nil event should be skipped
		{
			EventID:   uuid.New(),
			MatchID:   "test-match-nil-" + uuid.New().String()[:8],
			EventType: domain.EventTypeShot,
			Timestamp: time.Now(),
			TeamID:    2,
		},
	}

	err := repo.InsertBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertBatch with nil events should not error: %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_WithEmptyPlayerID_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "test-match-no-player-" + uuid.New().String()[:8],
		EventType: domain.EventTypeHalfTime,
		Timestamp: time.Now(),
		TeamID:    1,
		PlayerID:  "", // Empty player ID
	}

	err := repo.InsertBatch(ctx, []*domain.Event{event})
	if err != nil {
		t.Fatalf("InsertBatch with empty playerID should not error: %v", err)
	}
}

func TestClickHouseRepository_InsertEngagementBatch_EmptyBatch_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := repo.InsertEngagementBatch(ctx, []*domain.EngagementEvent{})
	if err != nil {
		t.Fatalf("InsertEngagementBatch with empty batch should not error: %v", err)
	}
}

func TestClickHouseRepository_InsertEngagementBatch_SingleEvent_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event := &domain.EngagementEvent{
		EventID:        uuid.New(),
		MatchID:        "integration-test-engagement-" + uuid.New().String()[:8],
		UserID:         "user-123",
		SessionID:      "session-456",
		EngagementType: domain.EngagementTypeReaction,
		Timestamp:      time.Now(),
		DeviceType:     "mobile",
		Platform:       "ios",
		CountryCode:    "US",
		Metadata:       map[string]interface{}{"source": "test"},
	}

	err := repo.InsertEngagementBatch(ctx, []*domain.EngagementEvent{event})
	if err != nil {
		t.Fatalf("InsertEngagementBatch failed: %v", err)
	}
}

func TestClickHouseRepository_InsertEngagementBatch_MultipleEvents_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	matchID := "integration-test-engagement-batch-" + uuid.New().String()[:8]
	events := []*domain.EngagementEvent{
		{
			EventID:        uuid.New(),
			MatchID:        matchID,
			UserID:         "user-1",
			SessionID:      "session-1",
			EngagementType: domain.EngagementTypeReaction,
			Timestamp:      time.Now(),
		},
		{
			EventID:        uuid.New(),
			MatchID:        matchID,
			UserID:         "user-2",
			SessionID:      "session-2",
			EngagementType: domain.EngagementTypeShare,
			Timestamp:      time.Now(),
		},
		{
			EventID:        uuid.New(),
			MatchID:        matchID,
			UserID:         "user-3",
			SessionID:      "session-3",
			EngagementType: domain.EngagementTypeComment,
			Timestamp:      time.Now(),
		},
	}

	err := repo.InsertEngagementBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertEngagementBatch failed: %v", err)
	}
}

func TestClickHouseRepository_InsertEngagementBatch_WithNilEvents_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events := []*domain.EngagementEvent{
		{
			EventID:        uuid.New(),
			MatchID:        "test-engagement-nil-" + uuid.New().String()[:8],
			UserID:         "user-1",
			SessionID:      "session-1",
			EngagementType: domain.EngagementTypeReaction,
			Timestamp:      time.Now(),
		},
		nil, // Nil event should be skipped
	}

	err := repo.InsertEngagementBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertEngagementBatch with nil events should not error: %v", err)
	}
}

func TestClickHouseRepository_Close_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	err := repo.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestClickHouseRepository_Close_NilConnection_Integration(t *testing.T) {
	repo := &ClickHouseRepository{
		conn:   nil,
		logger: newIntegrationTestLogger(),
	}

	err := repo.Close()
	if err != nil {
		t.Fatalf("Close with nil connection should not error: %v", err)
	}
}

func TestNewConnection_Integration(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "clickhouse"
	}
	port := os.Getenv("CLICKHOUSE_PORT")
	if port == "" {
		port = "9000"
	}

	cfg := ConnectionConfig{
		Hosts:           []string{host + ":" + port},
		Database:        "football_simulator",
		Username:        "default",
		Password:        "",
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		DialTimeout:     10 * time.Second,
		ReadTimeout:     30 * time.Second,
		Debug:           false,
	}

	conn, err := NewConnection(cfg)
	if err != nil {
		t.Fatalf("NewConnection failed: %v", err)
	}
	defer conn.Close()

	// Verify connection works
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = conn.Ping(ctx)
	if err != nil {
		t.Fatalf("connection ping failed: %v", err)
	}
}

func TestDefaultConnectionConfig_Integration(t *testing.T) {
	cfg := DefaultConnectionConfig()

	if len(cfg.Hosts) != 1 || cfg.Hosts[0] != "localhost:9000" {
		t.Errorf("unexpected default hosts: %v", cfg.Hosts)
	}
	if cfg.Database != "football_simulator" {
		t.Errorf("expected database 'football_simulator', got %s", cfg.Database)
	}
	if cfg.Username != "default" {
		t.Errorf("expected username 'default', got %s", cfg.Username)
	}
	if cfg.MaxOpenConns != 10 {
		t.Errorf("expected MaxOpenConns 10, got %d", cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns != 5 {
		t.Errorf("expected MaxIdleConns 5, got %d", cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime != time.Hour {
		t.Errorf("expected ConnMaxLifetime 1h, got %v", cfg.ConnMaxLifetime)
	}
	if cfg.DialTimeout != 10*time.Second {
		t.Errorf("expected DialTimeout 10s, got %v", cfg.DialTimeout)
	}
	if cfg.ReadTimeout != 30*time.Second {
		t.Errorf("expected ReadTimeout 30s, got %v", cfg.ReadTimeout)
	}
}

func TestClickHouseRepository_LargeBatch_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a batch of 100 events
	matchID := "integration-test-large-batch-" + uuid.New().String()[:8]
	events := make([]*domain.Event, 100)
	for i := 0; i < 100; i++ {
		events[i] = &domain.Event{
			EventID:   uuid.New(),
			MatchID:   matchID,
			EventType: domain.EventTypePass,
			Timestamp: time.Now(),
			TeamID:    (i % 2) + 1,
			PlayerID:  "player-" + uuid.New().String()[:8],
		}
	}

	err := repo.InsertBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertBatch for large batch failed: %v", err)
	}
}

func TestClickHouseRepository_AllEventTypes_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eventTypes := []domain.EventType{
		domain.EventTypePass,
		domain.EventTypeShot,
		domain.EventTypeShotOnTarget,
		domain.EventTypeGoal,
		domain.EventTypeOwnGoal,
		domain.EventTypeFoul,
		domain.EventTypeYellowCard,
		domain.EventTypeRedCard,
		domain.EventTypeSubstitution,
		domain.EventTypeOffside,
		domain.EventTypeCorner,
		domain.EventTypeFreeKick,
		domain.EventTypePenalty,
		domain.EventTypePenaltySaved,
		domain.EventTypeInterception,
		domain.EventTypeTackle,
		domain.EventTypeSave,
		domain.EventTypeInjury,
		domain.EventTypeVARReview,
		domain.EventTypeKickoff,
		domain.EventTypeHalfTime,
		domain.EventTypeSecondHalf,
		domain.EventTypeFullTime,
	}

	matchID := "integration-test-all-types-" + uuid.New().String()[:8]
	events := make([]*domain.Event, len(eventTypes))
	for i, et := range eventTypes {
		events[i] = &domain.Event{
			EventID:   uuid.New(),
			MatchID:   matchID,
			EventType: et,
			Timestamp: time.Now(),
			TeamID:    1,
		}
	}

	err := repo.InsertBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertBatch for all event types failed: %v", err)
	}
}

func TestClickHouseRepository_AllEngagementTypes_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	engagementTypes := []domain.EngagementType{
		domain.EngagementTypeReaction,
		domain.EngagementTypeComment,
		domain.EngagementTypeVideoAction,
		domain.EngagementTypeShare,
		domain.EngagementTypePrediction,
		domain.EngagementTypeClick,
		domain.EngagementTypeSession,
	}

	matchID := "integration-test-all-engagement-types-" + uuid.New().String()[:8]
	events := make([]*domain.EngagementEvent, len(engagementTypes))
	for i, et := range engagementTypes {
		events[i] = &domain.EngagementEvent{
			EventID:        uuid.New(),
			MatchID:        matchID,
			UserID:         "user-" + uuid.New().String()[:8],
			SessionID:      "session-" + uuid.New().String()[:8],
			EngagementType: et,
			Timestamp:      time.Now(),
		}
	}

	err := repo.InsertEngagementBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertEngagementBatch for all engagement types failed: %v", err)
	}
}

func TestClickHouseRepository_ContextCancellation_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	// Create already cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Ping should fail or return quickly
	err := repo.Ping(ctx)
	if err == nil {
		t.Log("Ping succeeded with cancelled context - behavior may vary by driver")
	}
}

func TestClickHouseRepository_MultipleBatches_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newIntegrationTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Insert multiple batches
	for batch := 0; batch < 5; batch++ {
		matchID := "integration-test-multi-batch-" + uuid.New().String()[:8]
		events := make([]*domain.Event, 20)
		for i := 0; i < 20; i++ {
			events[i] = &domain.Event{
				EventID:   uuid.New(),
				MatchID:   matchID,
				EventType: domain.EventTypePass,
				Timestamp: time.Now(),
				TeamID:    1,
			}
		}

		err := repo.InsertBatch(ctx, events)
		if err != nil {
			t.Fatalf("InsertBatch for batch %d failed: %v", batch, err)
		}
	}
}
