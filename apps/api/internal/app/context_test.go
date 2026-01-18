package app

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"
)

// Test helper to get ClickHouse host from environment
func getClickHouseHost() string {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "clickhouse"
	}
	return host
}

// Test helper to get ClickHouse port from environment
func getClickHousePort() int {
	return 9000 // Default ClickHouse native port
}

// Test helper to get Kafka broker from environment
func getKafkaBroker() string {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:29092"
	}
	return broker
}

// createTestConfig creates a config for testing
func createTestConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host:         "localhost",
			Port:         8080,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
		Kafka: KafkaConfig{
			BootstrapServers: getKafkaBroker(),
			TopicPrefix:      "test-",
			TopicEvents:      "test-events",
			TopicEngagements: "test-engagements",
			ProducerTimeout:  10 * time.Second,
		},
		ClickHouse: ClickHouseConfig{
			Host:     getClickHouseHost(),
			Port:     getClickHousePort(),
			Database: "football_simulator",
			User:     "default",
			Password: "",
		},
	}
}

// createTestLogger creates a logger for testing
func createTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError, // Only log errors during tests
	}))
}

// =============================================================================
// NewContext Tests
// =============================================================================

func TestNewContext_Success(t *testing.T) {
	cfg := createTestConfig()
	logger := createTestLogger()

	ctx, err := NewContext(cfg, logger)
	if err != nil {
		t.Fatalf("NewContext failed: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ctx.Shutdown(shutdownCtx)
	}()

	if ctx.Config != cfg {
		t.Error("Config not set correctly")
	}
	if ctx.Logger != logger {
		t.Error("Logger not set correctly")
	}
	if ctx.ClickHouse == nil {
		t.Error("ClickHouse connection not initialized")
	}
	if ctx.Producer == nil {
		t.Error("Kafka producer not initialized")
	}
	if ctx.shutdownCh == nil {
		t.Error("Shutdown channel not initialized")
	}
}

func TestNewContext_ClickHouseConnectionFailure(t *testing.T) {
	cfg := createTestConfig()
	// Use invalid host to trigger connection failure
	cfg.ClickHouse.Host = "invalid-host-that-does-not-exist"
	cfg.ClickHouse.Port = 12345
	logger := createTestLogger()

	ctx, err := NewContext(cfg, logger)
	if err == nil {
		// Cleanup if somehow it succeeded
		if ctx != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = ctx.Shutdown(shutdownCtx)
		}
		t.Fatal("Expected error for invalid ClickHouse host, got nil")
	}

	// Verify error message contains expected text
	if ctx != nil {
		t.Error("Expected nil context when initialization fails")
	}
}

func TestNewContext_ClickHouseInvalidAuth(t *testing.T) {
	cfg := createTestConfig()
	// Use invalid credentials
	cfg.ClickHouse.User = "nonexistent_user"
	cfg.ClickHouse.Password = "wrong_password"
	logger := createTestLogger()

	ctx, err := NewContext(cfg, logger)
	// This may or may not fail depending on ClickHouse auth settings
	// In default setup, ClickHouse allows any user
	if err != nil {
		// Expected failure with authentication
		if ctx != nil {
			t.Error("Expected nil context when initialization fails")
		}
		return
	}

	// If it succeeded, cleanup
	if ctx != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ctx.Shutdown(shutdownCtx)
	}
}

// =============================================================================
// Shutdown Tests
// =============================================================================

func TestAppContext_Shutdown_Success(t *testing.T) {
	cfg := createTestConfig()
	logger := createTestLogger()

	appCtx, err := NewContext(cfg, logger)
	if err != nil {
		t.Fatalf("NewContext failed: %v", err)
	}

	// Perform shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = appCtx.Shutdown(shutdownCtx)
	if err != nil {
		t.Errorf("Shutdown returned unexpected error: %v", err)
	}
}

func TestAppContext_Shutdown_WithHTTPServer(t *testing.T) {
	cfg := createTestConfig()
	logger := createTestLogger()

	appCtx, err := NewContext(cfg, logger)
	if err != nil {
		t.Fatalf("NewContext failed: %v", err)
	}

	// Create a test HTTP server
	appCtx.Server = &http.Server{
		Addr:    ":0", // Random port
		Handler: http.NewServeMux(),
	}

	// Start server in background
	go func() {
		_ = appCtx.Server.ListenAndServe()
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Perform shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = appCtx.Shutdown(shutdownCtx)
	if err != nil {
		t.Errorf("Shutdown returned unexpected error: %v", err)
	}
}

func TestAppContext_Shutdown_NilComponents(t *testing.T) {
	// Test shutdown with nil components doesn't panic
	appCtx := &AppContext{
		Logger:     createTestLogger(),
		shutdownCh: make(chan struct{}),
		Server:     nil,
		Producer:   nil,
		ClickHouse: nil,
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := appCtx.Shutdown(shutdownCtx)
	if err != nil {
		t.Errorf("Shutdown with nil components should not error: %v", err)
	}
}

func TestAppContext_Shutdown_ContextCanceled(t *testing.T) {
	cfg := createTestConfig()
	logger := createTestLogger()

	appCtx, err := NewContext(cfg, logger)
	if err != nil {
		t.Fatalf("NewContext failed: %v", err)
	}

	// Create already canceled context
	shutdownCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Shutdown should still attempt to close resources
	// It may return an error due to context cancellation
	_ = appCtx.Shutdown(shutdownCtx)
}

// =============================================================================
// ShutdownChan Tests
// =============================================================================

func TestAppContext_ShutdownChan_ReturnsChannel(t *testing.T) {
	appCtx := &AppContext{
		Logger:     createTestLogger(),
		shutdownCh: make(chan struct{}),
	}

	ch := appCtx.ShutdownChan()
	if ch == nil {
		t.Error("ShutdownChan returned nil")
	}

	// Verify channel is not closed initially
	select {
	case <-ch:
		t.Error("Shutdown channel should not be closed initially")
	default:
		// Expected - channel is not closed
	}
}

func TestAppContext_ShutdownChan_SignalsOnShutdown(t *testing.T) {
	appCtx := &AppContext{
		Logger:     createTestLogger(),
		shutdownCh: make(chan struct{}),
	}

	ch := appCtx.ShutdownChan()

	// Start a goroutine that waits for shutdown
	done := make(chan bool)
	go func() {
		<-ch
		done <- true
	}()

	// Perform shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = appCtx.Shutdown(shutdownCtx)

	// Verify shutdown signal was received
	select {
	case <-done:
		// Success - shutdown was signaled
	case <-time.After(2 * time.Second):
		t.Error("Shutdown signal not received within timeout")
	}
}

// =============================================================================
// initClickHouse Tests
// =============================================================================

func TestAppContext_initClickHouse_Success(t *testing.T) {
	cfg := createTestConfig()
	appCtx := &AppContext{
		Config: cfg,
		Logger: createTestLogger(),
	}

	err := appCtx.initClickHouse()
	if err != nil {
		t.Fatalf("initClickHouse failed: %v", err)
	}
	defer func() {
		if appCtx.ClickHouse != nil {
			_ = appCtx.ClickHouse.Close()
		}
	}()

	if appCtx.ClickHouse == nil {
		t.Error("ClickHouse connection not set after init")
	}

	// Verify connection works
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = appCtx.ClickHouse.Ping(ctx)
	if err != nil {
		t.Errorf("ClickHouse ping failed: %v", err)
	}
}

func TestAppContext_initClickHouse_InvalidHost(t *testing.T) {
	cfg := createTestConfig()
	cfg.ClickHouse.Host = "nonexistent-host-12345"
	cfg.ClickHouse.Port = 9999

	appCtx := &AppContext{
		Config: cfg,
		Logger: createTestLogger(),
	}

	err := appCtx.initClickHouse()
	if err == nil {
		// Cleanup if somehow it succeeded
		if appCtx.ClickHouse != nil {
			_ = appCtx.ClickHouse.Close()
		}
		t.Error("Expected error for invalid ClickHouse host")
	}
}

// =============================================================================
// initKafkaProducer Tests
// =============================================================================

func TestAppContext_initKafkaProducer(t *testing.T) {
	cfg := createTestConfig()
	appCtx := &AppContext{
		Config: cfg,
		Logger: createTestLogger(),
	}

	appCtx.initKafkaProducer()

	if appCtx.Producer == nil {
		t.Error("Producer not initialized")
	}

	// Verify producer configuration
	if appCtx.Producer.Topic != cfg.Kafka.TopicEvents {
		t.Errorf("Expected topic %s, got %s", cfg.Kafka.TopicEvents, appCtx.Producer.Topic)
	}

	// Cleanup
	if appCtx.Producer != nil {
		_ = appCtx.Producer.Close()
	}
}

func TestAppContext_initKafkaProducer_CustomTimeout(t *testing.T) {
	cfg := createTestConfig()
	cfg.Kafka.ProducerTimeout = 30 * time.Second

	appCtx := &AppContext{
		Config: cfg,
		Logger: createTestLogger(),
	}

	appCtx.initKafkaProducer()

	if appCtx.Producer == nil {
		t.Error("Producer not initialized")
	}

	// Verify custom timeout is applied
	if appCtx.Producer.WriteTimeout != 30*time.Second {
		t.Errorf("Expected WriteTimeout 30s, got %v", appCtx.Producer.WriteTimeout)
	}

	// Cleanup
	if appCtx.Producer != nil {
		_ = appCtx.Producer.Close()
	}
}

// =============================================================================
// Integration Tests
// =============================================================================

func TestAppContext_FullLifecycle(t *testing.T) {
	cfg := createTestConfig()
	logger := createTestLogger()

	// Create context
	appCtx, err := NewContext(cfg, logger)
	if err != nil {
		t.Fatalf("NewContext failed: %v", err)
	}

	// Verify all components initialized
	if appCtx.ClickHouse == nil {
		t.Error("ClickHouse not initialized")
	}
	if appCtx.Producer == nil {
		t.Error("Producer not initialized")
	}

	// Test ClickHouse connection
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := appCtx.ClickHouse.Ping(pingCtx); err != nil {
		t.Errorf("ClickHouse ping failed: %v", err)
	}

	// Perform shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = appCtx.Shutdown(shutdownCtx)
	if err != nil {
		t.Errorf("Shutdown failed: %v", err)
	}

	// Verify shutdown channel is closed
	select {
	case <-appCtx.ShutdownChan():
		// Expected - channel is closed
	default:
		t.Error("Shutdown channel should be closed after shutdown")
	}
}
