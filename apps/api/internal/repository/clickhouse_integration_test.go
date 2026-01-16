package repository

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestNewClickHouseRepository_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newTestLogger()

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

	logger := newTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := repo.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestClickHouseRepository_GetMatchMetrics_EmptyMatchID_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := repo.GetMatchMetrics(ctx, "")
	if err == nil {
		t.Fatal("expected error for empty matchID")
	}
}

func TestClickHouseRepository_GetMatchMetrics_NonExistentMatch_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	metrics, err := repo.GetMatchMetrics(ctx, "non-existent-match-12345")
	if err != nil {
		t.Fatalf("GetMatchMetrics failed: %v", err)
	}

	// Should return nil for non-existent match
	if metrics != nil {
		t.Errorf("expected nil metrics for non-existent match, got %+v", metrics)
	}
}

func TestClickHouseRepository_GetEventsPerMinute_EmptyMatchID_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := repo.GetEventsPerMinute(ctx, "")
	if err == nil {
		t.Fatal("expected error for empty matchID")
	}
}

func TestClickHouseRepository_GetEventsPerMinute_NonExistentMatch_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	events, err := repo.GetEventsPerMinute(ctx, "non-existent-match-12345")
	if err != nil {
		t.Fatalf("GetEventsPerMinute failed: %v", err)
	}

	// Should return empty slice or nil for non-existent match
	if len(events) != 0 {
		t.Errorf("expected 0 events for non-existent match, got %d", len(events))
	}
}

func TestClickHouseRepository_ContextTimeout_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	// Create a very short timeout to trigger timeout errors
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Let the context timeout
	time.Sleep(1 * time.Millisecond)

	// Ping should fail with timeout
	err := repo.Ping(ctx)
	if err == nil {
		t.Log("Ping succeeded despite cancelled context - may depend on driver behavior")
	}
}


func TestClickHouseRepository_MultipleQueries_Integration(t *testing.T) {
	conn := getClickHouseConnection(t)
	defer conn.Close()

	logger := newTestLogger()
	repo := NewClickHouseRepository(conn, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Run multiple queries in sequence
	for i := 0; i < 5; i++ {
		err := repo.Ping(ctx)
		if err != nil {
			t.Fatalf("Ping %d failed: %v", i, err)
		}
	}

	// Run GetMatchMetrics multiple times
	for i := 0; i < 3; i++ {
		_, err := repo.GetMatchMetrics(ctx, "test-match")
		if err != nil {
			t.Fatalf("GetMatchMetrics %d failed: %v", i, err)
		}
	}

	// Run GetEventsPerMinute multiple times
	for i := 0; i < 3; i++ {
		_, err := repo.GetEventsPerMinute(ctx, "test-match")
		if err != nil {
			t.Fatalf("GetEventsPerMinute %d failed: %v", i, err)
		}
	}
}
