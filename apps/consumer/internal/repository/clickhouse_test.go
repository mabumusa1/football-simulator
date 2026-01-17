package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/mabumusa1/football-simulator/apps/consumer/internal/domain"
)

// Test infrastructure helpers
func getClickHouseHost() string {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "clickhouse"
	}
	return host
}

func getClickHousePort() string {
	port := os.Getenv("CLICKHOUSE_PORT")
	if port == "" {
		port = "9000"
	}
	return port
}

func setupTestConnection(t *testing.T) (*ClickHouseRepository, func()) {
	host := getClickHouseHost()
	port := getClickHousePort()

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

	repo := NewClickHouseRepository(conn, nil)

	cleanup := func() {
		_ = conn.Close()
	}

	return repo, cleanup
}

func createTestEvent() *domain.Event {
	return &domain.Event{
		EventID:   uuid.New(),
		MatchID:   "test-match-" + uuid.New().String()[:8],
		EventType: domain.EventTypeGoal,
		Timestamp: time.Now(),
		TeamID:    1,
		PlayerID:  "test-player-" + uuid.New().String()[:8],
		Metadata:  map[string]interface{}{"minute": 45},
	}
}

func createTestEngagementEvent() *domain.EngagementEvent {
	return &domain.EngagementEvent{
		EventID:           uuid.New(),
		MatchID:           "test-match-" + uuid.New().String()[:8],
		UserID:            "test-user-" + uuid.New().String()[:8],
		SessionID:         "test-session-" + uuid.New().String()[:8],
		EngagementType:    domain.EngagementTypeReaction,
		EngagementSubtype: "cheer",
		GameMinute:        45,
		DeviceType:        "mobile",
		Platform:          "ios",
		CountryCode:       "US",
		Timestamp:         time.Now(),
		Metadata:          map[string]interface{}{"source": "test"},
	}
}

// =============================================================================
// Constructor Tests
// =============================================================================

func TestNewClickHouseRepository(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	if repo == nil {
		t.Fatal("expected non-nil repository")
	}
	if repo.conn == nil {
		t.Error("expected conn to be set")
	}
	if repo.logger == nil {
		t.Error("expected default logger to be set")
	}
}

// =============================================================================
// Ping Tests
// =============================================================================

func TestClickHouseRepository_Ping_Success(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := repo.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestClickHouseRepository_Ping_ContextCanceled(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := repo.Ping(ctx)
	if err == nil {
		t.Error("expected error for canceled context")
	}
}

// =============================================================================
// InsertBatch Tests
// =============================================================================

func TestClickHouseRepository_InsertBatch_EmptySlice(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := repo.InsertBatch(ctx, []*domain.Event{})
	if err != nil {
		t.Errorf("expected no error for empty slice, got %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_NilSlice(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := repo.InsertBatch(ctx, nil)
	if err != nil {
		t.Errorf("expected no error for nil slice, got %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_SingleEvent(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	event := createTestEvent()
	events := []*domain.Event{event}

	err := repo.InsertBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertBatch failed: %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_MultipleEvents(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	events := make([]*domain.Event, 10)
	for i := 0; i < 10; i++ {
		events[i] = createTestEvent()
	}

	err := repo.InsertBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertBatch failed: %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_WithNilEvents(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	events := []*domain.Event{
		createTestEvent(),
		nil, // Should be skipped
		createTestEvent(),
	}

	err := repo.InsertBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertBatch failed: %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_AllNil(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events := []*domain.Event{nil, nil, nil}

	err := repo.InsertBatch(ctx, events)
	if err != nil {
		t.Errorf("expected no error for all nil events, got %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_LargeBatch(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	events := make([]*domain.Event, 100)
	for i := 0; i < 100; i++ {
		events[i] = createTestEvent()
	}

	err := repo.InsertBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertBatch failed for large batch: %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_EmptyPlayerID(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	event := createTestEvent()
	event.PlayerID = "" // Empty player ID

	events := []*domain.Event{event}

	err := repo.InsertBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertBatch failed: %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_WithMetadata(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	event := createTestEvent()
	event.Metadata = map[string]interface{}{
		"minute":      45,
		"assist":      "player-789",
		"goal_type":   "header",
		"distance":    12.5,
		"is_overtime": false,
	}

	events := []*domain.Event{event}

	err := repo.InsertBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertBatch failed: %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_AllEventTypes(t *testing.T) {
	eventTypes := []domain.EventType{
		domain.EventTypeGoal,
		domain.EventTypePass,
		domain.EventTypeShot,
		domain.EventTypeShotOnTarget,
		domain.EventTypeTackle,
		domain.EventTypeFoul,
		domain.EventTypeYellowCard,
		domain.EventTypeRedCard,
		domain.EventTypeCorner,
		domain.EventTypeFreeKick,
		domain.EventTypePenalty,
		domain.EventTypeSubstitution,
		domain.EventTypeInjury,
		domain.EventTypeOffside,
		domain.EventTypeKickoff,
		domain.EventTypeHalfTime,
		domain.EventTypeFullTime,
	}

	for _, eventType := range eventTypes {
		t.Run(string(eventType), func(t *testing.T) {
			repo, cleanup := setupTestConnection(t)
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			event := &domain.Event{
				EventID:   uuid.New(),
				MatchID:   "match-123",
				EventType: eventType,
				Timestamp: time.Now(),
				TeamID:    1,
				PlayerID:  "player-456",
			}

			events := []*domain.Event{event}

			err := repo.InsertBatch(ctx, events)
			if err != nil {
				t.Fatalf("InsertBatch failed for event type %s: %v", eventType, err)
			}
		})
	}
}

func TestClickHouseRepository_InsertBatch_ContextCanceled(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	event := createTestEvent()
	events := []*domain.Event{event}

	err := repo.InsertBatch(ctx, events)
	if err == nil {
		t.Error("expected error for canceled context")
	}
}

// =============================================================================
// InsertEngagementBatch Tests
// =============================================================================

func TestClickHouseRepository_InsertEngagementBatch_EmptySlice(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := repo.InsertEngagementBatch(ctx, []*domain.EngagementEvent{})
	if err != nil {
		t.Errorf("expected no error for empty slice, got %v", err)
	}
}

func TestClickHouseRepository_InsertEngagementBatch_NilSlice(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := repo.InsertEngagementBatch(ctx, nil)
	if err != nil {
		t.Errorf("expected no error for nil slice, got %v", err)
	}
}

func TestClickHouseRepository_InsertEngagementBatch_SingleEvent(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	event := createTestEngagementEvent()
	events := []*domain.EngagementEvent{event}

	err := repo.InsertEngagementBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertEngagementBatch failed: %v", err)
	}
}

func TestClickHouseRepository_InsertEngagementBatch_MultipleEvents(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	events := make([]*domain.EngagementEvent, 10)
	for i := 0; i < 10; i++ {
		events[i] = createTestEngagementEvent()
	}

	err := repo.InsertEngagementBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertEngagementBatch failed: %v", err)
	}
}

func TestClickHouseRepository_InsertEngagementBatch_WithNilEvents(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	events := []*domain.EngagementEvent{
		createTestEngagementEvent(),
		nil, // Should be skipped
		createTestEngagementEvent(),
	}

	err := repo.InsertEngagementBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertEngagementBatch failed: %v", err)
	}
}

func TestClickHouseRepository_InsertEngagementBatch_LargeBatch(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	events := make([]*domain.EngagementEvent, 500)
	for i := 0; i < 500; i++ {
		events[i] = createTestEngagementEvent()
	}

	err := repo.InsertEngagementBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertEngagementBatch failed for large batch: %v", err)
	}
}

func TestClickHouseRepository_InsertEngagementBatch_AllFields(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	relatedGameEventID := uuid.New()
	event := &domain.EngagementEvent{
		EventID:            uuid.New(),
		MatchID:            "match-123",
		UserID:             "user-456",
		SessionID:          "session-789",
		EngagementType:     domain.EngagementTypeReaction,
		EngagementSubtype:  "like",
		RelatedGameEventID: &relatedGameEventID,
		GameMinute:         45,
		DeviceType:         "mobile",
		Platform:           "ios",
		CountryCode:        "US",
		Content:            "Great goal!",
		Timestamp:          time.Now(),
		Metadata:           map[string]interface{}{"source": "app"},
	}

	events := []*domain.EngagementEvent{event}

	err := repo.InsertEngagementBatch(ctx, events)
	if err != nil {
		t.Fatalf("InsertEngagementBatch failed: %v", err)
	}
}

func TestClickHouseRepository_InsertEngagementBatch_AllEngagementTypes(t *testing.T) {
	engagementTypes := []domain.EngagementType{
		domain.EngagementTypeReaction,
		domain.EngagementTypeComment,
		domain.EngagementTypeShare,
	}

	for _, engagementType := range engagementTypes {
		t.Run(string(engagementType), func(t *testing.T) {
			repo, cleanup := setupTestConnection(t)
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			event := &domain.EngagementEvent{
				EventID:        uuid.New(),
				MatchID:        "match-123",
				UserID:         "user-456",
				SessionID:      "session-789",
				EngagementType: engagementType,
				Timestamp:      time.Now(),
			}

			events := []*domain.EngagementEvent{event}

			err := repo.InsertEngagementBatch(ctx, events)
			if err != nil {
				t.Fatalf("InsertEngagementBatch failed for engagement type %s: %v", engagementType, err)
			}
		})
	}
}

func TestClickHouseRepository_InsertEngagementBatch_ContextCanceled(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	event := createTestEngagementEvent()
	events := []*domain.EngagementEvent{event}

	err := repo.InsertEngagementBatch(ctx, events)
	if err == nil {
		t.Error("expected error for canceled context")
	}
}

// =============================================================================
// Close Tests
// =============================================================================

func TestClickHouseRepository_Close(t *testing.T) {
	repo, _ := setupTestConnection(t)
	// Don't use cleanup, we'll close manually

	err := repo.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// =============================================================================
// DefaultConnectionConfig Tests
// =============================================================================

func TestDefaultConnectionConfig(t *testing.T) {
	cfg := DefaultConnectionConfig()

	if len(cfg.Hosts) != 1 || cfg.Hosts[0] != "localhost:9000" {
		t.Errorf("expected Hosts ['localhost:9000'], got %v", cfg.Hosts)
	}
	if cfg.Database != "football_simulator" {
		t.Errorf("expected Database 'football_simulator', got %s", cfg.Database)
	}
	if cfg.Username != "default" {
		t.Errorf("expected Username 'default', got %s", cfg.Username)
	}
	if cfg.Password != "" {
		t.Errorf("expected empty Password, got %s", cfg.Password)
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
	if cfg.Debug != false {
		t.Errorf("expected Debug false, got %v", cfg.Debug)
	}
}

// =============================================================================
// Concurrent Tests
// =============================================================================

func TestClickHouseRepository_ConcurrentInserts(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	errChan := make(chan error, 10)

	// Run concurrent inserts
	for i := 0; i < 10; i++ {
		go func() {
			event := createTestEvent()
			events := []*domain.Event{event}
			errChan <- repo.InsertBatch(ctx, events)
		}()
	}

	// Collect results
	for i := 0; i < 10; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Concurrent insert failed: %v", err)
		}
	}
}

func TestClickHouseRepository_ConcurrentEngagementInserts(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	errChan := make(chan error, 10)

	// Run concurrent inserts
	for i := 0; i < 10; i++ {
		go func() {
			event := createTestEngagementEvent()
			events := []*domain.EngagementEvent{event}
			errChan <- repo.InsertEngagementBatch(ctx, events)
		}()
	}

	// Collect results
	for i := 0; i < 10; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Concurrent engagement insert failed: %v", err)
		}
	}
}

// =============================================================================
// Multiple Operations Tests
// =============================================================================

func TestClickHouseRepository_MultipleOperations(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Run multiple operations in sequence
	for i := 0; i < 5; i++ {
		// Ping
		err := repo.Ping(ctx)
		if err != nil {
			t.Fatalf("Ping failed on iteration %d: %v", i, err)
		}

		// Insert events
		events := []*domain.Event{createTestEvent()}
		err = repo.InsertBatch(ctx, events)
		if err != nil {
			t.Fatalf("InsertBatch failed on iteration %d: %v", i, err)
		}

		// Insert engagements
		engagements := []*domain.EngagementEvent{createTestEngagementEvent()}
		err = repo.InsertEngagementBatch(ctx, engagements)
		if err != nil {
			t.Fatalf("InsertEngagementBatch failed on iteration %d: %v", i, err)
		}
	}
}
