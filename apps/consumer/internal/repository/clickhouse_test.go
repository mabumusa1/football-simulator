package repository

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/mabumusa1/football-simulator/apps/consumer/internal/domain"
)

// MockConn implements driver.Conn for testing
type MockConn struct {
	PingFunc         func(ctx context.Context) error
	PrepareBatchFunc func(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
	CloseFunc        func() error
}

func (m *MockConn) Ping(ctx context.Context) error {
	if m.PingFunc != nil {
		return m.PingFunc(ctx)
	}
	return nil
}

func (m *MockConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if m.PrepareBatchFunc != nil {
		return m.PrepareBatchFunc(ctx, query, opts...)
	}
	return nil, errors.New("PrepareBatch not implemented")
}

func (m *MockConn) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

// Implement remaining driver.Conn interface methods
func (m *MockConn) Stats() driver.Stats            { return driver.Stats{} }
func (m *MockConn) Contributors() []string         { return nil }
func (m *MockConn) ServerVersion() (*driver.ServerVersion, error) {
	return &driver.ServerVersion{}, nil
}
func (m *MockConn) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return nil
}
func (m *MockConn) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return nil, nil
}
func (m *MockConn) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	return nil
}
func (m *MockConn) Exec(ctx context.Context, query string, args ...interface{}) error {
	return nil
}
func (m *MockConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...interface{}) error {
	return nil
}

// MockBatch implements driver.Batch for testing
type MockBatch struct {
	AppendFunc       func(v ...interface{}) error
	AppendStructFunc func(v interface{}) error
	SendFunc         func() error
	AbortFunc        func() error
	FlushFunc        func() error
	ColumnFunc       func(int) driver.BatchColumn
	IsSentFunc       func() bool
	RowsFunc         func() int
	AppendCount      int
}

func (m *MockBatch) Append(v ...interface{}) error {
	m.AppendCount++
	if m.AppendFunc != nil {
		return m.AppendFunc(v...)
	}
	return nil
}

func (m *MockBatch) AppendStruct(v interface{}) error {
	if m.AppendStructFunc != nil {
		return m.AppendStructFunc(v)
	}
	return nil
}

func (m *MockBatch) Send() error {
	if m.SendFunc != nil {
		return m.SendFunc()
	}
	return nil
}

func (m *MockBatch) Abort() error {
	if m.AbortFunc != nil {
		return m.AbortFunc()
	}
	return nil
}

func (m *MockBatch) Flush() error {
	if m.FlushFunc != nil {
		return m.FlushFunc()
	}
	return nil
}

func (m *MockBatch) Column(i int) driver.BatchColumn {
	if m.ColumnFunc != nil {
		return m.ColumnFunc(i)
	}
	return nil
}

func (m *MockBatch) IsSent() bool {
	if m.IsSentFunc != nil {
		return m.IsSentFunc()
	}
	return false
}

func (m *MockBatch) Rows() int {
	if m.RowsFunc != nil {
		return m.RowsFunc()
	}
	return m.AppendCount
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestNewClickHouseRepository(t *testing.T) {
	mockConn := &MockConn{}
	logger := newTestLogger()

	repo := NewClickHouseRepository(mockConn, logger)

	if repo == nil {
		t.Fatal("expected non-nil repository")
	}
	if repo.conn != mockConn {
		t.Error("expected conn to be set")
	}
	if repo.logger != logger {
		t.Error("expected logger to be set")
	}
}

func TestNewClickHouseRepository_NilLogger(t *testing.T) {
	mockConn := &MockConn{}

	repo := NewClickHouseRepository(mockConn, nil)

	if repo == nil {
		t.Fatal("expected non-nil repository")
	}
	if repo.logger == nil {
		t.Error("expected default logger to be set")
	}
}

func TestClickHouseRepository_Ping_Success(t *testing.T) {
	mockConn := &MockConn{
		PingFunc: func(ctx context.Context) error {
			return nil
		},
	}
	repo := NewClickHouseRepository(mockConn, newTestLogger())

	err := repo.Ping(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestClickHouseRepository_Ping_Error(t *testing.T) {
	expectedErr := errors.New("connection failed")
	mockConn := &MockConn{
		PingFunc: func(ctx context.Context) error {
			return expectedErr
		},
	}
	repo := NewClickHouseRepository(mockConn, newTestLogger())

	err := repo.Ping(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to wrap %v, got %v", expectedErr, err)
	}
}

func TestClickHouseRepository_InsertBatch_EmptySlice(t *testing.T) {
	mockConn := &MockConn{}
	repo := NewClickHouseRepository(mockConn, newTestLogger())

	err := repo.InsertBatch(context.Background(), []*domain.Event{})
	if err != nil {
		t.Errorf("expected no error for empty slice, got %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_NilSlice(t *testing.T) {
	mockConn := &MockConn{}
	repo := NewClickHouseRepository(mockConn, newTestLogger())

	err := repo.InsertBatch(context.Background(), nil)
	if err != nil {
		t.Errorf("expected no error for nil slice, got %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_PrepareBatchError(t *testing.T) {
	expectedErr := errors.New("prepare batch failed")
	mockConn := &MockConn{
		PrepareBatchFunc: func(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
			return nil, expectedErr
		},
	}
	repo := NewClickHouseRepository(mockConn, newTestLogger())

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
			PlayerID:  "player-456",
		},
	}

	err := repo.InsertBatch(context.Background(), events)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestClickHouseRepository_InsertBatch_Success(t *testing.T) {
	mockBatch := &MockBatch{}
	mockConn := &MockConn{
		PrepareBatchFunc: func(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
			return mockBatch, nil
		},
	}
	repo := NewClickHouseRepository(mockConn, newTestLogger())

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
			PlayerID:  "player-456",
			Metadata:  map[string]interface{}{"minute": 45},
		},
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypePass,
			Timestamp: time.Now(),
			TeamID:    2,
			PlayerID:  "player-789",
		},
	}

	err := repo.InsertBatch(context.Background(), events)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if mockBatch.AppendCount != 2 {
		t.Errorf("expected 2 appends, got %d", mockBatch.AppendCount)
	}
}

func TestClickHouseRepository_InsertBatch_SendError(t *testing.T) {
	expectedErr := errors.New("send failed")
	mockBatch := &MockBatch{
		SendFunc: func() error {
			return expectedErr
		},
	}
	mockConn := &MockConn{
		PrepareBatchFunc: func(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
			return mockBatch, nil
		},
	}
	repo := NewClickHouseRepository(mockConn, newTestLogger())

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
		},
	}

	err := repo.InsertBatch(context.Background(), events)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestClickHouseRepository_InsertBatch_NilEventSkipped(t *testing.T) {
	mockBatch := &MockBatch{}
	mockConn := &MockConn{
		PrepareBatchFunc: func(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
			return mockBatch, nil
		},
	}
	repo := NewClickHouseRepository(mockConn, newTestLogger())

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
		},
		nil, // This should be skipped
		{
			EventID:   uuid.New(),
			MatchID:   "match-456",
			EventType: domain.EventTypePass,
			Timestamp: time.Now(),
			TeamID:    2,
		},
	}

	err := repo.InsertBatch(context.Background(), events)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	// Only 2 events should be appended (nil is skipped)
	if mockBatch.AppendCount != 2 {
		t.Errorf("expected 2 appends (nil skipped), got %d", mockBatch.AppendCount)
	}
}

func TestClickHouseRepository_InsertBatch_AppendError(t *testing.T) {
	appendCalls := 0
	mockBatch := &MockBatch{
		AppendFunc: func(v ...interface{}) error {
			appendCalls++
			if appendCalls == 1 {
				return errors.New("append failed")
			}
			return nil
		},
	}
	mockConn := &MockConn{
		PrepareBatchFunc: func(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
			return mockBatch, nil
		},
	}
	repo := NewClickHouseRepository(mockConn, newTestLogger())

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
		},
		{
			EventID:   uuid.New(),
			MatchID:   "match-456",
			EventType: domain.EventTypePass,
			Timestamp: time.Now(),
			TeamID:    2,
		},
	}

	// Append error should not fail the entire batch
	err := repo.InsertBatch(context.Background(), events)
	if err != nil {
		t.Errorf("expected no error (append errors are logged and skipped), got %v", err)
	}
}

func TestClickHouseRepository_InsertBatch_EmptyPlayerID(t *testing.T) {
	var capturedArgs []interface{}
	mockBatch := &MockBatch{
		AppendFunc: func(v ...interface{}) error {
			capturedArgs = v
			return nil
		},
	}
	mockConn := &MockConn{
		PrepareBatchFunc: func(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
			return mockBatch, nil
		},
	}
	repo := NewClickHouseRepository(mockConn, newTestLogger())

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
			PlayerID:  "", // Empty player ID should result in nil pointer
		},
	}

	err := repo.InsertBatch(context.Background(), events)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	// Check that playerID was passed as nil pointer
	// Args: event_id, match_id, event_type, team_id, player_id, metadata, timestamp
	if len(capturedArgs) >= 5 {
		playerIDPtr, ok := capturedArgs[4].(*string)
		if !ok {
			t.Error("expected player_id to be *string type")
		} else if playerIDPtr != nil {
			t.Error("expected nil player_id pointer for empty string")
		}
	}
}

func TestClickHouseRepository_InsertBatch_WithMetadata(t *testing.T) {
	var capturedArgs []interface{}
	mockBatch := &MockBatch{
		AppendFunc: func(v ...interface{}) error {
			capturedArgs = v
			return nil
		},
	}
	mockConn := &MockConn{
		PrepareBatchFunc: func(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
			return mockBatch, nil
		},
	}
	repo := NewClickHouseRepository(mockConn, newTestLogger())

	events := []*domain.Event{
		{
			EventID:   uuid.New(),
			MatchID:   "match-123",
			EventType: domain.EventTypeGoal,
			Timestamp: time.Now(),
			TeamID:    1,
			Metadata:  map[string]interface{}{"minute": 45, "assist": "player-789"},
		},
	}

	err := repo.InsertBatch(context.Background(), events)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	// Verify metadata was serialized
	if len(capturedArgs) >= 6 {
		metadata, ok := capturedArgs[5].(string)
		if !ok {
			t.Error("expected metadata to be string")
		}
		if metadata == "{}" {
			t.Error("expected non-empty metadata JSON")
		}
	}
}

func TestClickHouseRepository_Close_Success(t *testing.T) {
	closeCalled := false
	mockConn := &MockConn{
		CloseFunc: func() error {
			closeCalled = true
			return nil
		},
	}
	repo := NewClickHouseRepository(mockConn, newTestLogger())

	err := repo.Close()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !closeCalled {
		t.Error("expected Close to be called on connection")
	}
}

func TestClickHouseRepository_Close_NilConn(t *testing.T) {
	repo := &ClickHouseRepository{
		conn:   nil,
		logger: newTestLogger(),
	}

	err := repo.Close()
	if err != nil {
		t.Errorf("expected no error for nil conn, got %v", err)
	}
}

func TestClickHouseRepository_Close_Error(t *testing.T) {
	expectedErr := errors.New("close failed")
	mockConn := &MockConn{
		CloseFunc: func() error {
			return expectedErr
		},
	}
	repo := NewClickHouseRepository(mockConn, newTestLogger())

	err := repo.Close()
	if err != expectedErr {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}

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

func TestConnectionConfig_CustomValues(t *testing.T) {
	cfg := ConnectionConfig{
		Hosts:           []string{"host1:9000", "host2:9000"},
		Database:        "custom_db",
		Username:        "admin",
		Password:        "secret",
		MaxOpenConns:    20,
		MaxIdleConns:    10,
		ConnMaxLifetime: 2 * time.Hour,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     60 * time.Second,
		Debug:           true,
	}

	if len(cfg.Hosts) != 2 {
		t.Errorf("expected 2 hosts, got %d", len(cfg.Hosts))
	}
	if cfg.Database != "custom_db" {
		t.Errorf("expected Database 'custom_db', got %s", cfg.Database)
	}
	if cfg.MaxOpenConns != 20 {
		t.Errorf("expected MaxOpenConns 20, got %d", cfg.MaxOpenConns)
	}
}
