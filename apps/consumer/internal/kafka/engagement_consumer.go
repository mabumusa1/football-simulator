package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"

	"github.com/mabumusa1/football-simulator/apps/consumer/internal/domain"
)

var (
	kafkaEngagementConsumerLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "football_simulator",
			Subsystem: "kafka_engagement_consumer",
			Name:      "lag",
			Help:      "Current engagement consumer lag",
		},
		[]string{"topic", "partition"},
	)

	kafkaEngagementBatchesProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "football_simulator",
			Subsystem: "kafka_engagement_consumer",
			Name:      "batches_processed_total",
			Help:      "Total number of engagement batches processed",
		},
		[]string{"status"},
	)

	kafkaEngagementsConsumed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "football_simulator",
			Subsystem: "kafka_engagement_consumer",
			Name:      "events_consumed_total",
			Help:      "Total number of engagement events consumed from Kafka",
		},
		[]string{"status"},
	)

	kafkaEngagementConsumeDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "football_simulator",
			Subsystem: "kafka_engagement_consumer",
			Name:      "consume_duration_seconds",
			Help:      "Histogram of engagement batch processing duration in seconds",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		},
		[]string{"operation"},
	)
)

// EngagementRepository defines the interface for engagement batch insertion.
type EngagementRepository interface {
	InsertEngagementBatch(ctx context.Context, events []*domain.EngagementEvent) error
}

// EngagementConsumer consumes engagement events from Kafka and batch inserts them into ClickHouse.
type EngagementConsumer struct {
	reader        *kafka.Reader
	repository    EngagementRepository
	batchSize     int
	flushInterval time.Duration
	workerCount   int
	logger        *slog.Logger

	batch     []*domain.EngagementEvent
	messages  []kafka.Message
	batchLock sync.Mutex
	ticker    *time.Ticker
	done      chan struct{}
	wg        sync.WaitGroup
}

// EngagementConsumerConfig holds configuration for the engagement consumer.
type EngagementConsumerConfig struct {
	Reader        *kafka.Reader
	Repository    EngagementRepository
	BatchSize     int
	FlushInterval time.Duration
	WorkerCount   int
	Logger        *slog.Logger
}

// NewEngagementConsumer creates a new EngagementConsumer instance.
func NewEngagementConsumer(cfg EngagementConsumerConfig) *EngagementConsumer {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 2000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 500 * time.Millisecond
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 8
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &EngagementConsumer{
		reader:        cfg.Reader,
		repository:    cfg.Repository,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		workerCount:   cfg.WorkerCount,
		logger:        cfg.Logger,
		batch:         make([]*domain.EngagementEvent, 0, cfg.BatchSize),
		messages:      make([]kafka.Message, 0, cfg.BatchSize),
		done:          make(chan struct{}),
	}
}

// Start begins consuming engagement messages from Kafka.
func (c *EngagementConsumer) Start(ctx context.Context) {
	c.logger.Info("starting engagement consumer",
		slog.Int("batch_size", c.batchSize),
		slog.Duration("flush_interval", c.flushInterval),
		slog.Int("worker_count", c.workerCount),
	)

	c.ticker = time.NewTicker(c.flushInterval)

	// Start the flusher goroutine
	c.wg.Add(1)
	go c.flusher(ctx)

	// Start worker goroutines
	for i := 0; i < c.workerCount; i++ {
		c.wg.Add(1)
		go c.worker(ctx, i)
	}

	// Wait for shutdown
	<-ctx.Done()
	c.logger.Info("context cancelled, stopping engagement workers")

	// Signal done and wait for workers
	close(c.done)
}

// worker fetches and processes engagement messages from Kafka.
func (c *EngagementConsumer) worker(ctx context.Context, workerID int) {
	defer c.wg.Done()

	c.logger.Info("engagement worker started", slog.Int("worker_id", workerID))

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("engagement worker stopping", slog.Int("worker_id", workerID))
			return
		case <-c.done:
			c.logger.Info("engagement worker stopping", slog.Int("worker_id", workerID))
			return
		default:
			fetchCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			msg, err := c.reader.FetchMessage(fetchCtx)
			cancel()

			if err != nil {
				if ctx.Err() != nil {
					continue
				}
				if fetchCtx.Err() == context.DeadlineExceeded {
					continue
				}
				c.logger.Error("failed to fetch engagement message",
					slog.Int("worker_id", workerID),
					slog.String("error", err.Error()),
				)
				continue
			}

			// Update consumer lag metric
			c.updateLagMetric(msg)

			// Parse the message
			event, err := domain.EngagementEventFromKafkaMessage(msg.Value)
			if err != nil {
				c.logger.Error("failed to parse engagement message",
					slog.String("error", err.Error()),
					slog.Int64("offset", msg.Offset),
					slog.Int("partition", msg.Partition),
				)
				kafkaEngagementsConsumed.WithLabelValues("parse_error").Inc()
				if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
					c.logger.Error("failed to commit message after parse error",
						slog.String("error", commitErr.Error()),
					)
				}
				continue
			}

			// Add to batch
			c.batchLock.Lock()
			c.batch = append(c.batch, event)
			c.messages = append(c.messages, msg)
			batchLen := len(c.batch)
			c.batchLock.Unlock()

			c.logger.Debug("engagement message added to batch",
				slog.Int("worker_id", workerID),
				slog.String("event_id", event.EventID.String()),
				slog.Int("batch_size", batchLen),
			)

			// Flush if batch is full
			if batchLen >= c.batchSize {
				c.flushWithContext(ctx)
			}
		}
	}
}

// flusher periodically flushes the engagement batch.
func (c *EngagementConsumer) flusher(ctx context.Context) {
	defer c.wg.Done()
	defer c.ticker.Stop()

	c.logger.Info("engagement flusher started")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("engagement flusher stopping, final flush")
			c.flushWithContext(context.Background())
			return
		case <-c.done:
			c.logger.Info("engagement flusher stopping, final flush")
			c.flushWithContext(context.Background())
			return
		case <-c.ticker.C:
			c.flushWithContext(ctx)
		}
	}
}

// updateLagMetric updates the consumer lag Prometheus metric.
func (c *EngagementConsumer) updateLagMetric(msg kafka.Message) {
	stats := c.reader.Stats()
	kafkaEngagementConsumerLag.WithLabelValues(
		stats.Topic,
		fmt.Sprintf("%d", msg.Partition),
	).Set(float64(stats.Lag))
}

// flushWithContext flushes the current batch to the repository.
func (c *EngagementConsumer) flushWithContext(ctx context.Context) {
	c.batchLock.Lock()
	if len(c.batch) == 0 {
		c.batchLock.Unlock()
		return
	}

	events := c.batch
	messages := c.messages
	c.batch = make([]*domain.EngagementEvent, 0, c.batchSize)
	c.messages = make([]kafka.Message, 0, c.batchSize)
	c.batchLock.Unlock()

	startTime := time.Now()
	c.logger.Debug("flushing engagement batch",
		slog.Int("batch_size", len(events)),
	)

	err := c.repository.InsertEngagementBatch(ctx, events)
	duration := time.Since(startTime)
	kafkaEngagementConsumeDuration.WithLabelValues("insert_batch").Observe(duration.Seconds())

	if err != nil {
		c.logger.Error("failed to insert engagement batch",
			slog.Int("batch_size", len(events)),
			slog.Duration("duration", duration),
			slog.String("error", err.Error()),
		)
		kafkaEngagementBatchesProcessed.WithLabelValues("error").Inc()
		return
	}

	// Commit messages after successful insert
	if len(messages) > 0 {
		if err := c.reader.CommitMessages(ctx, messages...); err != nil {
			c.logger.Error("failed to commit engagement messages",
				slog.Int("message_count", len(messages)),
				slog.String("error", err.Error()),
			)
		}
	}

	c.logger.Info("engagement batch flushed successfully",
		slog.Int("batch_size", len(events)),
		slog.Duration("duration", duration),
	)
	kafkaEngagementBatchesProcessed.WithLabelValues("success").Inc()
	kafkaEngagementsConsumed.WithLabelValues("success").Add(float64(len(events)))
}

// Stop signals the consumer to stop and waits for it to finish.
func (c *EngagementConsumer) Stop() {
	c.logger.Info("stopping engagement consumer")
	close(c.done)
	c.wg.Wait()
	c.logger.Info("engagement consumer stopped")
}
