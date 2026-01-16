package repository

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// MockRow implements driver.Row for testing.
type MockRow struct {
	scanFunc func(dest ...interface{}) error
}

func (m *MockRow) Scan(dest ...interface{}) error {
	if m.scanFunc != nil {
		return m.scanFunc(dest...)
	}
	return nil
}

func (m *MockRow) ScanStruct(dest interface{}) error {
	return nil
}

func (m *MockRow) Err() error {
	return nil
}

// MockRows implements driver.Rows for testing.
type MockRows struct {
	current  int
	data     [][]interface{}
	scanFunc func(dest ...interface{}) error
	errFunc  func() error
	columns  []string
}

func (m *MockRows) Next() bool {
	m.current++
	return m.current <= len(m.data)
}

func (m *MockRows) Scan(dest ...interface{}) error {
	if m.scanFunc != nil {
		return m.scanFunc(dest...)
	}
	if m.current > 0 && m.current <= len(m.data) {
		row := m.data[m.current-1]
		for i, v := range row {
			if i < len(dest) {
				switch d := dest[i].(type) {
				case *string:
					if s, ok := v.(string); ok {
						*d = s
					}
				case *uint64:
					if n, ok := v.(uint64); ok {
						*d = n
					}
				case *int64:
					if n, ok := v.(int64); ok {
						*d = n
					}
				case *time.Time:
					if t, ok := v.(time.Time); ok {
						*d = t
					}
				}
			}
		}
	}
	return nil
}

func (m *MockRows) ScanStruct(dest interface{}) error {
	return nil
}

func (m *MockRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (m *MockRows) Totals(dest ...interface{}) error {
	return nil
}

func (m *MockRows) Columns() []string {
	return m.columns
}

func (m *MockRows) Close() error {
	return nil
}

func (m *MockRows) Err() error {
	if m.errFunc != nil {
		return m.errFunc()
	}
	return nil
}

// MockConn implements driver.Conn for testing.
type MockConn struct {
	pingFunc     func(ctx context.Context) error
	queryRowFunc func(ctx context.Context, query string, args ...interface{}) driver.Row
	queryFunc    func(ctx context.Context, query string, args ...interface{}) (driver.Rows, error)
	closeFunc    func() error
}

func (m *MockConn) Contributors() []string {
	return nil
}

func (m *MockConn) ServerVersion() (*driver.ServerVersion, error) {
	return &driver.ServerVersion{}, nil
}

func (m *MockConn) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return nil
}

func (m *MockConn) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, query, args...)
	}
	return &MockRows{}, nil
}

func (m *MockConn) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	if m.queryRowFunc != nil {
		return m.queryRowFunc(ctx, query, args...)
	}
	return &MockRow{}
}

func (m *MockConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, nil
}

func (m *MockConn) Exec(ctx context.Context, query string, args ...interface{}) error {
	return nil
}

func (m *MockConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...interface{}) error {
	return nil
}

func (m *MockConn) Ping(ctx context.Context) error {
	if m.pingFunc != nil {
		return m.pingFunc(ctx)
	}
	return nil
}

func (m *MockConn) Stats() driver.Stats {
	return driver.Stats{}
}

func (m *MockConn) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func TestNewClickHouseRepository(t *testing.T) {
	conn := &MockConn{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	repo := NewClickHouseRepository(conn, logger)

	if repo == nil {
		t.Fatal("NewClickHouseRepository returned nil")
	}
	if repo.conn != conn {
		t.Error("NewClickHouseRepository did not set conn correctly")
	}
	if repo.logger != logger {
		t.Error("NewClickHouseRepository did not set logger correctly")
	}
}

func TestNewClickHouseRepository_NilLogger(t *testing.T) {
	conn := &MockConn{}

	repo := NewClickHouseRepository(conn, nil)

	if repo == nil {
		t.Fatal("NewClickHouseRepository returned nil")
	}
	if repo.logger == nil {
		t.Error("NewClickHouseRepository should use default logger when nil is passed")
	}
}

func TestClickHouseRepository_Ping_Success(t *testing.T) {
	pingCalled := false
	conn := &MockConn{
		pingFunc: func(ctx context.Context) error {
			pingCalled = true
			return nil
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	repo := NewClickHouseRepository(conn, logger)

	err := repo.Ping(context.Background())

	if err != nil {
		t.Errorf("Ping returned error: %v", err)
	}
	if !pingCalled {
		t.Error("Ping did not call conn.Ping")
	}
}

func TestClickHouseRepository_Ping_Error(t *testing.T) {
	expectedErr := errors.New("connection failed")
	conn := &MockConn{
		pingFunc: func(ctx context.Context) error {
			return expectedErr
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	repo := NewClickHouseRepository(conn, logger)

	err := repo.Ping(context.Background())

	if err == nil {
		t.Error("Ping should return error when conn.Ping fails")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("Ping error should wrap original error, got: %v", err)
	}
}

func TestClickHouseRepository_GetMatchMetrics_EmptyMatchID(t *testing.T) {
	conn := &MockConn{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	repo := NewClickHouseRepository(conn, logger)

	_, err := repo.GetMatchMetrics(context.Background(), "")

	if err == nil {
		t.Error("GetMatchMetrics should return error for empty matchID")
	}
}

func TestClickHouseRepository_GetMatchMetrics_Success(t *testing.T) {
	firstEvent := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	lastEvent := time.Date(2024, 1, 1, 11, 45, 0, 0, time.UTC)

	conn := &MockConn{
		queryRowFunc: func(ctx context.Context, query string, args ...interface{}) driver.Row {
			return &MockRow{
				scanFunc: func(dest ...interface{}) error {
					// total_events, goals, yellow_cards, red_cards, first_event_at, last_event_at
					if len(dest) >= 6 {
						*dest[0].(*uint64) = 100
						*dest[1].(*uint64) = 3
						*dest[2].(*uint64) = 4
						*dest[3].(*uint64) = 1
						*dest[4].(*time.Time) = firstEvent
						*dest[5].(*time.Time) = lastEvent
					}
					return nil
				},
			}
		},
		queryFunc: func(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
			return &MockRows{
				data: [][]interface{}{
					{"goal", uint64(3)},
					{"yellow_card", uint64(4)},
					{"red_card", uint64(1)},
				},
			}, nil
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	repo := NewClickHouseRepository(conn, logger)

	metrics, err := repo.GetMatchMetrics(context.Background(), "match-123")

	if err != nil {
		t.Errorf("GetMatchMetrics returned error: %v", err)
	}
	if metrics == nil {
		t.Fatal("GetMatchMetrics returned nil metrics")
	}
	if metrics.MatchID != "match-123" {
		t.Errorf("GetMatchMetrics MatchID = %q, want %q", metrics.MatchID, "match-123")
	}
	if metrics.TotalEvents != 100 {
		t.Errorf("GetMatchMetrics TotalEvents = %d, want %d", metrics.TotalEvents, 100)
	}
	if metrics.Goals != 3 {
		t.Errorf("GetMatchMetrics Goals = %d, want %d", metrics.Goals, 3)
	}
	if metrics.YellowCards != 4 {
		t.Errorf("GetMatchMetrics YellowCards = %d, want %d", metrics.YellowCards, 4)
	}
	if metrics.RedCards != 1 {
		t.Errorf("GetMatchMetrics RedCards = %d, want %d", metrics.RedCards, 1)
	}
}

func TestClickHouseRepository_GetMatchMetrics_NoEvents(t *testing.T) {
	conn := &MockConn{
		queryRowFunc: func(ctx context.Context, query string, args ...interface{}) driver.Row {
			return &MockRow{
				scanFunc: func(dest ...interface{}) error {
					// All zeros
					if len(dest) >= 6 {
						*dest[0].(*uint64) = 0
						*dest[1].(*uint64) = 0
						*dest[2].(*uint64) = 0
						*dest[3].(*uint64) = 0
						*dest[4].(*time.Time) = time.Time{}
						*dest[5].(*time.Time) = time.Time{}
					}
					return nil
				},
			}
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	repo := NewClickHouseRepository(conn, logger)

	metrics, err := repo.GetMatchMetrics(context.Background(), "match-123")

	if err != nil {
		t.Errorf("GetMatchMetrics returned error: %v", err)
	}
	if metrics != nil {
		t.Error("GetMatchMetrics should return nil for match with no events")
	}
}

func TestClickHouseRepository_GetMatchMetrics_QueryError(t *testing.T) {
	conn := &MockConn{
		queryRowFunc: func(ctx context.Context, query string, args ...interface{}) driver.Row {
			return &MockRow{
				scanFunc: func(dest ...interface{}) error {
					return errors.New("query failed")
				},
			}
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	repo := NewClickHouseRepository(conn, logger)

	_, err := repo.GetMatchMetrics(context.Background(), "match-123")

	if err == nil {
		t.Error("GetMatchMetrics should return error when query fails")
	}
}

func TestClickHouseRepository_GetMatchMetrics_EventsByTypeQueryError(t *testing.T) {
	conn := &MockConn{
		queryRowFunc: func(ctx context.Context, query string, args ...interface{}) driver.Row {
			return &MockRow{
				scanFunc: func(dest ...interface{}) error {
					if len(dest) >= 6 {
						*dest[0].(*uint64) = 100
						*dest[1].(*uint64) = 3
						*dest[2].(*uint64) = 4
						*dest[3].(*uint64) = 1
						*dest[4].(*time.Time) = time.Now()
						*dest[5].(*time.Time) = time.Now()
					}
					return nil
				},
			}
		},
		queryFunc: func(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
			return nil, errors.New("events by type query failed")
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	repo := NewClickHouseRepository(conn, logger)

	_, err := repo.GetMatchMetrics(context.Background(), "match-123")

	if err == nil {
		t.Error("GetMatchMetrics should return error when events by type query fails")
	}
}

func TestClickHouseRepository_GetEventsPerMinute_EmptyMatchID(t *testing.T) {
	conn := &MockConn{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	repo := NewClickHouseRepository(conn, logger)

	_, err := repo.GetEventsPerMinute(context.Background(), "")

	if err == nil {
		t.Error("GetEventsPerMinute should return error for empty matchID")
	}
}

func TestClickHouseRepository_GetEventsPerMinute_Success(t *testing.T) {
	minute1 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	minute2 := time.Date(2024, 1, 1, 10, 1, 0, 0, time.UTC)

	conn := &MockConn{
		queryFunc: func(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
			return &MockRows{
				data: [][]interface{}{
					{minute1, "goal", uint64(2)},
					{minute1, "pass", uint64(50)},
					{minute2, "shot", uint64(5)},
				},
			}, nil
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	repo := NewClickHouseRepository(conn, logger)

	results, err := repo.GetEventsPerMinute(context.Background(), "match-123")

	if err != nil {
		t.Errorf("GetEventsPerMinute returned error: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("GetEventsPerMinute returned %d results, want %d", len(results), 3)
	}
}

func TestClickHouseRepository_GetEventsPerMinute_QueryError(t *testing.T) {
	conn := &MockConn{
		queryFunc: func(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
			return nil, errors.New("query failed")
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	repo := NewClickHouseRepository(conn, logger)

	_, err := repo.GetEventsPerMinute(context.Background(), "match-123")

	if err == nil {
		t.Error("GetEventsPerMinute should return error when query fails")
	}
}

func TestClickHouseRepository_GetEventsPerMinute_RowsError(t *testing.T) {
	conn := &MockConn{
		queryFunc: func(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
			return &MockRows{
				errFunc: func() error {
					return errors.New("rows error")
				},
			}, nil
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	repo := NewClickHouseRepository(conn, logger)

	_, err := repo.GetEventsPerMinute(context.Background(), "match-123")

	if err == nil {
		t.Error("GetEventsPerMinute should return error when rows.Err() returns error")
	}
}

func TestClickHouseRepository_GetEventsPerMinute_EmptyResult(t *testing.T) {
	conn := &MockConn{
		queryFunc: func(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
			return &MockRows{
				data: [][]interface{}{},
			}, nil
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	repo := NewClickHouseRepository(conn, logger)

	results, err := repo.GetEventsPerMinute(context.Background(), "match-123")

	if err != nil {
		t.Errorf("GetEventsPerMinute returned error: %v", err)
	}
	// Results can be nil when no data is returned - this is acceptable
	if len(results) != 0 {
		t.Errorf("GetEventsPerMinute returned %d results, want %d", len(results), 0)
	}
}
