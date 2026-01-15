package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRouter creates and configures a new chi router with all routes and middleware.
func NewRouter(producer EventProducer, engagementProducer EngagementProducer, repository MetricsRepository, logger *slog.Logger, apiKey string, openAPISpec []byte) *chi.Mux {
	r := chi.NewRouter()

	// Apply middleware stack
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(RequestLogger(logger))
	r.Use(PrometheusMiddleware)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	// Create handler with engagement producer support
	h := NewHandlerWithEngagement(producer, engagementProducer, repository)

	// Swagger UI at root
	r.Get("/", SwaggerUI)
	r.Get("/openapi.yaml", ServeOpenAPISpec(openAPISpec))

	// Health check endpoints (outside /api prefix)
	r.Get("/health", h.HealthCheck)
	r.Get("/ready", h.ReadinessCheck)

	// Prometheus metrics endpoint
	r.Handle("/metrics", promhttp.Handler())

	// API routes (protected by API key authentication)
	r.Route("/api", func(r chi.Router) {
		// Apply API key authentication middleware to all /api routes
		r.Use(APIKeyAuth(apiKey))

		// Event ingestion (game events on the field)
		r.Post("/events", h.IngestEvent)

		// Engagement ingestion (viewer engagement events)
		r.Post("/engagements", h.IngestEngagements)

		// Match metrics
		r.Get("/matches/{matchId}/metrics", h.GetMatchMetrics)
	})

	return r
}

// NewServer creates a new HTTP server with the configured router.
func NewServer(addr string, producer EventProducer, engagementProducer EngagementProducer, repository MetricsRepository, logger *slog.Logger, apiKey string, openAPISpec []byte) *http.Server {
	router := NewRouter(producer, engagementProducer, repository, logger, apiKey, openAPISpec)

	return &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
}
