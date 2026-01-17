package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/mabumusa1/football-simulator/apps/api/internal/domain"
)

// EventProducer defines the interface for producing events to Kafka.
type EventProducer interface {
	Produce(ctx context.Context, event *domain.Event) error
	Ping(ctx context.Context) error
}

// EngagementProducer defines the interface for producing engagement events to Kafka.
type EngagementProducer interface {
	Produce(ctx context.Context, event *domain.EngagementEvent) error
	ProduceBatch(ctx context.Context, events []*domain.EngagementEvent) error
	Ping(ctx context.Context) error
}

// MetricsRepository defines the interface for querying metrics from ClickHouse (read-only).
type MetricsRepository interface {
	GetMatchMetrics(ctx context.Context, matchID string) (*domain.MatchMetrics, error)
	GetEventsPerMinute(ctx context.Context, matchID string) ([]domain.EventsPerMinute, error)
	Ping(ctx context.Context) error
}

// Handler handles HTTP requests for the API.
type Handler struct {
	producer           EventProducer
	engagementProducer EngagementProducer
	repository         MetricsRepository
}

// NewHandler creates a new Handler with the given producer and repository.
func NewHandler(producer EventProducer, repository MetricsRepository) *Handler {
	return &Handler{
		producer:   producer,
		repository: repository,
	}
}

// NewHandlerWithEngagement creates a new Handler with engagement producer support.
func NewHandlerWithEngagement(producer EventProducer, engagementProducer EngagementProducer, repository MetricsRepository) *Handler {
	return &Handler{
		producer:           producer,
		engagementProducer: engagementProducer,
		repository:         repository,
	}
}

// IngestEventResponse represents the response for a successful event ingestion.
type IngestEventResponse struct {
	EventID   string    `json:"eventId"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// IngestEvent handles POST /api/events.
// It validates the incoming event, produces it to Kafka, and returns 202 Accepted.
func (h *Handler) IngestEvent(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// Parse JSON body
	var req domain.EventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid JSON body", err.Error())
		return
	}

	// Validate and convert to domain Event
	event, err := req.ToEvent()
	if err != nil {
		if ve := domain.AsValidationError(err); ve != nil {
			respondErrorWithField(w, http.StatusBadRequest, ve.Message, ve.Field)
			return
		}
		respondError(w, http.StatusBadRequest, "validation failed", err.Error())
		return
	}

	// Produce to Kafka
	ctx := r.Context()
	if err := h.producer.Produce(ctx, event); err != nil {
		RecordKafkaProduceError()
		respondError(w, http.StatusServiceUnavailable, "failed to queue event", "")
		return
	}

	// Record metrics
	duration := time.Since(start)
	RecordEventIngested(string(event.EventType))
	RecordEventIngestDuration(duration)
	RecordEventResponseTime(duration)

	// Return 202 Accepted
	response := IngestEventResponse{
		EventID:   event.EventID.String(),
		Status:    "accepted",
		Timestamp: time.Now().UTC(),
	}
	respondJSON(w, http.StatusAccepted, response)
}

// GetMatchMetrics handles GET /api/matches/{matchId}/metrics.
// It queries the repository for match metrics and returns them.
func (h *Handler) GetMatchMetrics(w http.ResponseWriter, r *http.Request) {
	matchID := chi.URLParam(r, "matchId")
	if matchID == "" {
		respondError(w, http.StatusBadRequest, "matchId is required", "")
		return
	}

	ctx := r.Context()

	// Get base metrics
	metrics, err := h.repository.GetMatchMetrics(ctx, matchID)
	if err != nil {
		RecordClickHouseQueryError()
		respondError(w, http.StatusInternalServerError, "failed to fetch metrics", "")
		return
	}

	if metrics == nil || metrics.TotalEvents == 0 {
		respondError(w, http.StatusNotFound, "match not found", "")
		return
	}

	// Get events per minute to calculate peak engagement
	eventsPerMinute, err := h.repository.GetEventsPerMinute(ctx, matchID)
	if err != nil {
		RecordClickHouseQueryError()
		// Continue without peak engagement data
		respondJSON(w, http.StatusOK, metrics)
		return
	}

	// Calculate peak engagement
	var peak *domain.PeakEngagement
	if len(eventsPerMinute) > 0 {
		// Aggregate events per minute across all event types
		minuteTotals := make(map[time.Time]int64)
		for _, epm := range eventsPerMinute {
			minuteTotals[epm.Minute] += epm.EventCount
		}

		// Find the peak minute
		var peakMinute time.Time
		var peakCount int64
		for minute, count := range minuteTotals {
			if count > peakCount {
				peakCount = count
				peakMinute = minute
			}
		}

		if peakCount > 0 {
			peak = &domain.PeakEngagement{
				Minute:     peakMinute,
				EventCount: peakCount,
			}
		}
	}

	// Update metrics with peak engagement
	metrics.PeakMinute = peak

	// Add response time percentiles
	metrics.ResponseTimePercentiles = GetEventResponseTimePercentiles()

	respondJSON(w, http.StatusOK, metrics)
}

// HealthResponse represents the response for health check endpoints.
type HealthResponse struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// HealthCheck handles GET /health.
// It returns a simple health status without checking dependencies.
func (h *Handler) HealthCheck(w http.ResponseWriter, _ *http.Request) {
	response := HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now().UTC(),
	}
	respondJSON(w, http.StatusOK, response)
}

// ReadinessResponse represents the response for readiness check.
type ReadinessResponse struct {
	Status    string            `json:"status"`
	Timestamp time.Time         `json:"timestamp"`
	Checks    map[string]string `json:"checks,omitempty"`
}

// ReadinessCheck handles GET /ready.
// It verifies that dependencies (repository and producer) are available.
func (h *Handler) ReadinessCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	checks := make(map[string]string)
	healthy := true

	// Check ClickHouse connectivity
	if err := h.repository.Ping(ctx); err != nil {
		checks["clickhouse"] = "unhealthy: " + err.Error()
		healthy = false
	} else {
		checks["clickhouse"] = "healthy"
	}

	// Check Kafka connectivity
	if err := h.producer.Ping(ctx); err != nil {
		checks["kafka"] = "unhealthy: " + err.Error()
		healthy = false
	} else {
		checks["kafka"] = "healthy"
	}

	status := "ready"
	statusCode := http.StatusOK
	if !healthy {
		status = "not ready"
		statusCode = http.StatusServiceUnavailable
	}

	response := ReadinessResponse{
		Status:    status,
		Timestamp: time.Now().UTC(),
		Checks:    checks,
	}
	respondJSON(w, statusCode, response)
}

// SwaggerUI handles GET / and serves the Swagger UI documentation.
func SwaggerUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(swaggerUIHTML))
}

// ServeOpenAPISpec handles GET /openapi.yaml and serves the OpenAPI specification.
func ServeOpenAPISpec(spec []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-yaml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(spec)
	}
}

// IngestEngagementsResponse represents the response for batch engagement ingestion.
type IngestEngagementsResponse struct {
	Accepted  int       `json:"accepted"`
	Rejected  int       `json:"rejected"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// IngestEngagements handles POST /api/engagements.
// It accepts a batch of engagement events and produces them to Kafka.
func (h *Handler) IngestEngagements(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// Check if engagement producer is configured
	if h.engagementProducer == nil {
		respondError(w, http.StatusServiceUnavailable, "engagement ingestion not configured", "")
		return
	}

	// Parse JSON body
	var req domain.EngagementBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid JSON body", err.Error())
		return
	}

	if len(req.Events) == 0 {
		respondError(w, http.StatusBadRequest, "no events provided", "")
		return
	}

	// Validate and convert events
	validEvents := make([]*domain.EngagementEvent, 0, len(req.Events))
	rejected := 0

	for _, eventReq := range req.Events {
		event, err := eventReq.ToEngagementEvent()
		if err != nil {
			rejected++
			continue
		}
		validEvents = append(validEvents, event)
	}

	// Produce to Kafka in batch
	ctx := r.Context()
	if err := h.engagementProducer.ProduceBatch(ctx, validEvents); err != nil {
		RecordKafkaProduceError()
		respondError(w, http.StatusServiceUnavailable, "failed to queue engagements", "")
		return
	}

	// Record metrics
	duration := time.Since(start)
	for _, event := range validEvents {
		RecordEngagementIngested(string(event.EngagementType))
	}
	RecordEngagementIngestDuration(duration)

	response := IngestEngagementsResponse{
		Accepted:  len(validEvents),
		Rejected:  rejected,
		Status:    "accepted",
		Timestamp: time.Now().UTC(),
	}
	respondJSON(w, http.StatusAccepted, response)
}

const swaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Football Simulator Events API</title>
    <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
    <style>
        html { box-sizing: border-box; overflow-y: scroll; }
        *, *:before, *:after { box-sizing: inherit; }
        body { margin: 0; background: #fafafa; }
        .swagger-ui .topbar { display: none; }
    </style>
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
        window.onload = function() {
            SwaggerUIBundle({
                url: "/openapi.yaml",
                dom_id: '#swagger-ui',
                deepLinking: true,
                presets: [
                    SwaggerUIBundle.presets.apis,
                    SwaggerUIBundle.SwaggerUIStandalonePreset
                ],
                layout: "BaseLayout"
            });
        };
    </script>
</body>
</html>`
