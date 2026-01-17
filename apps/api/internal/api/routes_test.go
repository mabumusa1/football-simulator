package api

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestNewRouter_CreatesRouter(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	apiKey := "test-api-key"
	openAPISpec := []byte("openapi: 3.0.0")

	router := NewRouter(eventProducer, engagementProducer, repo, logger, apiKey, openAPISpec)

	if router == nil {
		t.Fatal("NewRouter returned nil")
	}
}

func TestNewRouter_RegistersHealthEndpoint(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	router := NewRouter(eventProducer, engagementProducer, repo, logger, "api-key", nil)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("GET /health: got status %d, want %d", rr.Code, http.StatusOK)
	}
}

func TestNewRouter_RegistersReadyEndpoint(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	router := NewRouter(eventProducer, engagementProducer, repo, logger, "api-key", nil)

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("GET /ready: got status %d, want %d", rr.Code, http.StatusOK)
	}
}

func TestNewRouter_RegistersMetricsEndpoint(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	router := NewRouter(eventProducer, engagementProducer, repo, logger, "api-key", nil)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	// Prometheus metrics endpoint should return 200
	if rr.Code != http.StatusOK {
		t.Errorf("GET /metrics: got status %d, want %d", rr.Code, http.StatusOK)
	}
}

func TestNewRouter_RegistersSwaggerUI(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	router := NewRouter(eventProducer, engagementProducer, repo, logger, "api-key", nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("GET /: got status %d, want %d", rr.Code, http.StatusOK)
	}

	contentType := rr.Header().Get("Content-Type")
	if contentType != "text/html; charset=utf-8" {
		t.Errorf("GET /: got Content-Type %q, want %q", contentType, "text/html; charset=utf-8")
	}
}

func TestNewRouter_RegistersOpenAPISpec(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	spec := []byte("openapi: 3.0.0\ninfo:\n  title: Test API")

	router := NewRouter(eventProducer, engagementProducer, repo, logger, "api-key", spec)

	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("GET /openapi.yaml: got status %d, want %d", rr.Code, http.StatusOK)
	}

	contentType := rr.Header().Get("Content-Type")
	if contentType != "application/x-yaml" {
		t.Errorf("GET /openapi.yaml: got Content-Type %q, want %q", contentType, "application/x-yaml")
	}

	if rr.Body.String() != string(spec) {
		t.Errorf("GET /openapi.yaml: got body %q, want %q", rr.Body.String(), string(spec))
	}
}

func TestNewRouter_ProtectsAPIRoutes_WithoutAPIKey(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	router := NewRouter(eventProducer, engagementProducer, repo, logger, "secret-key", nil)

	// Try to access /api/events without API key
	req := httptest.NewRequest(http.MethodPost, "/api/events", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("POST /api/events without API key: got status %d, want %d", rr.Code, http.StatusUnauthorized)
	}
}

func TestNewRouter_ProtectsAPIRoutes_WithWrongAPIKey(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	router := NewRouter(eventProducer, engagementProducer, repo, logger, "secret-key", nil)

	req := httptest.NewRequest(http.MethodPost, "/api/events", nil)
	req.Header.Set("X-API-Key", "wrong-key")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("POST /api/events with wrong API key: got status %d, want %d", rr.Code, http.StatusUnauthorized)
	}
}

func TestNewRouter_AllowsAPIRoutes_WithCorrectAPIKey(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	router := NewRouter(eventProducer, engagementProducer, repo, logger, "secret-key", nil)

	// POST /api/events with correct API key (will return 400 due to missing body, but not 401)
	req := httptest.NewRequest(http.MethodPost, "/api/events", nil)
	req.Header.Set("X-API-Key", "secret-key")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	// Should not be unauthorized
	if rr.Code == http.StatusUnauthorized {
		t.Error("POST /api/events with correct API key should not return 401")
	}
}

func TestNewRouter_RegistersEngagementsEndpoint(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	router := NewRouter(eventProducer, engagementProducer, repo, logger, "secret-key", nil)

	// POST /api/engagements with correct API key
	req := httptest.NewRequest(http.MethodPost, "/api/engagements", nil)
	req.Header.Set("X-API-Key", "secret-key")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	// Should not be unauthorized or 404
	if rr.Code == http.StatusUnauthorized {
		t.Error("POST /api/engagements with correct API key should not return 401")
	}
	if rr.Code == http.StatusNotFound {
		t.Error("POST /api/engagements should be registered, not 404")
	}
}

func TestNewRouter_RegistersMatchMetricsEndpoint(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	router := NewRouter(eventProducer, engagementProducer, repo, logger, "secret-key", nil)

	// GET /api/matches/{matchId}/metrics
	req := httptest.NewRequest(http.MethodGet, "/api/matches/test-match-123/metrics", nil)
	req.Header.Set("X-API-Key", "secret-key")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	// Should not be unauthorized
	if rr.Code == http.StatusUnauthorized {
		t.Error("GET /api/matches/{matchId}/metrics with correct API key should not return 401")
	}
}

func TestNewServer_CreatesServer(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	apiKey := "test-api-key"
	openAPISpec := []byte("openapi: 3.0.0")

	server := NewServer(":8080", eventProducer, engagementProducer, repo, logger, apiKey, openAPISpec)

	if server == nil {
		t.Fatal("NewServer returned nil")
	}
}

func TestNewServer_ConfiguresAddress(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server := NewServer(":9090", eventProducer, engagementProducer, repo, logger, "key", nil)

	if server.Addr != ":9090" {
		t.Errorf("NewServer Addr = %q, want %q", server.Addr, ":9090")
	}
}

func TestNewServer_ConfiguresTimeouts(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server := NewServer(":8080", eventProducer, engagementProducer, repo, logger, "key", nil)

	if server.ReadTimeout != 15*1e9 {
		t.Errorf("NewServer ReadTimeout = %v, want 15s", server.ReadTimeout)
	}
	if server.WriteTimeout != 30*1e9 {
		t.Errorf("NewServer WriteTimeout = %v, want 30s", server.WriteTimeout)
	}
	if server.IdleTimeout != 60*1e9 {
		t.Errorf("NewServer IdleTimeout = %v, want 60s", server.IdleTimeout)
	}
}

func TestNewServer_HasHandler(t *testing.T) {
	eventProducer, engagementProducer, cleanupProducers := setupTestProducers(t)
	defer cleanupProducers()

	repo, cleanupRepo := setupTestRepository(t)
	defer cleanupRepo()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server := NewServer(":8080", eventProducer, engagementProducer, repo, logger, "key", nil)

	if server.Handler == nil {
		t.Error("NewServer Handler should not be nil")
	}
}
