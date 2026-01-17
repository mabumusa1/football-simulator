package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
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
// GetMatchMetrics Tests
// =============================================================================

func TestClickHouseRepository_GetMatchMetrics_NonExistentMatch(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use a random match ID that doesn't exist
	matchID := "nonexistent-match-" + uuid.New().String()

	metrics, err := repo.GetMatchMetrics(ctx, matchID)
	if err != nil {
		t.Fatalf("GetMatchMetrics failed: %v", err)
	}

	// For non-existent match, should return nil metrics
	if metrics != nil {
		t.Error("expected nil metrics for non-existent match")
	}
}

func TestClickHouseRepository_GetMatchMetrics_EmptyMatchID(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := repo.GetMatchMetrics(ctx, "")
	if err == nil {
		t.Error("expected error for empty matchID")
	}

	if err.Error() != "matchID cannot be empty" {
		t.Errorf("expected 'matchID cannot be empty', got '%s'", err.Error())
	}
}

func TestClickHouseRepository_GetMatchMetrics_ContextCanceled(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := repo.GetMatchMetrics(ctx, "test-match")
	if err == nil {
		t.Error("expected error for canceled context")
	}
}

// =============================================================================
// GetEventsPerMinute Tests
// =============================================================================

func TestClickHouseRepository_GetEventsPerMinute_NonExistentMatch(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use a random match ID that doesn't exist
	matchID := "nonexistent-match-" + uuid.New().String()

	results, err := repo.GetEventsPerMinute(ctx, matchID)
	if err != nil {
		t.Fatalf("GetEventsPerMinute failed: %v", err)
	}

	// For non-existent match, should return empty slice
	if len(results) != 0 {
		t.Errorf("expected empty results for non-existent match, got %d", len(results))
	}
}

func TestClickHouseRepository_GetEventsPerMinute_EmptyMatchID(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := repo.GetEventsPerMinute(ctx, "")
	if err == nil {
		t.Error("expected error for empty matchID")
	}

	if err.Error() != "matchID cannot be empty" {
		t.Errorf("expected 'matchID cannot be empty', got '%s'", err.Error())
	}
}

func TestClickHouseRepository_GetEventsPerMinute_ContextCanceled(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := repo.GetEventsPerMinute(ctx, "test-match")
	if err == nil {
		t.Error("expected error for canceled context")
	}
}

// =============================================================================
// Connection Tests
// =============================================================================

func TestClickHouseRepository_MultipleQueries(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Run multiple queries in sequence
	for i := 0; i < 5; i++ {
		matchID := "test-match-" + uuid.New().String()

		_, err := repo.GetMatchMetrics(ctx, matchID)
		if err != nil {
			t.Fatalf("GetMatchMetrics failed on iteration %d: %v", i, err)
		}

		_, err = repo.GetEventsPerMinute(ctx, matchID)
		if err != nil {
			t.Fatalf("GetEventsPerMinute failed on iteration %d: %v", i, err)
		}
	}
}

func TestClickHouseRepository_ConcurrentQueries(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	errChan := make(chan error, 10)

	// Run concurrent queries
	for i := 0; i < 10; i++ {
		go func() {
			matchID := "test-match-" + uuid.New().String()
			_, err := repo.GetMatchMetrics(ctx, matchID)
			errChan <- err
		}()
	}

	// Collect results
	for i := 0; i < 10; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Concurrent query failed: %v", err)
		}
	}
}

// =============================================================================
// Timeout Tests
// =============================================================================

func TestClickHouseRepository_Ping_Timeout(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	// Very short timeout that may or may not trigger
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// This may succeed or fail depending on timing
	_ = repo.Ping(ctx)
	// Just verify it doesn't panic
}

func TestClickHouseRepository_Query_Timeout(t *testing.T) {
	repo, cleanup := setupTestConnection(t)
	defer cleanup()

	// Very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// This will likely fail due to timeout
	_, _ = repo.GetMatchMetrics(ctx, "test-match")
	// Just verify it doesn't panic
}
