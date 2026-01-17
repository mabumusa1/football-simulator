package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
)

// BatchRepository defines the interface for batch event insertion.
type BatchRepository[T any] interface {
	InsertBatch(ctx context.Context, events []*T) error
}

// MessageParser is a function that parses Kafka message bytes into an event.
type MessageParser[T any] func([]byte) (*T, error)

// GenericBatchConsumer is a generic consumer that batches events and inserts them.
type GenericBatchConsumer[T any] struct {
	reader        *kafka.Reader
	repository    BatchRepository[T]
	parser        MessageParser[T]
	batchSize     int
	flushInterval time.Duration
	workerCount   int
	logger        *slog.Logger
	consumerName  string

	// Metrics
	lagMetric        *prometheus.GaugeVec
	batchesProcessed *prometheus.CounterVec
	eventsConsumed   *prometheus.CounterVec
	consumeDuration  *prometheus.HistogramVec

	batch     []*T
	messages  []kafka.Message
	batchLock sync.Mutex
	ticker    *time.Ticker
	done      chan struct{}
	wg        sync.WaitGroup
}

// GenericBatchConsumerConfig holds configuration for the generic batch consumer.
type GenericBatchConsumerConfig[T any] struct {
	Reader        *kafka.Reader
	Repository    BatchRepository[T]
	Parser        MessageParser[T]
	BatchSize     int
	FlushInterval time.Duration
	WorkerCount   int
	Logger        *slog.Logger
	ConsumerName  string

	// Metrics
	LagMetric        *prometheus.GaugeVec
	BatchesProcessed *prometheus.CounterVec
	EventsConsumed   *prometheus.CounterVec
	ConsumeDuration  *prometheus.HistogramVec
}

// NewGenericBatchConsumer creates a new GenericBatchConsumer instance.
func NewGenericBatchConsumer[T any](cfg GenericBatchConsumerConfig[T]) *GenericBatchConsumer[T] {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 1 * time.Second
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 4
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ConsumerName == "" {
		cfg.ConsumerName = "generic"
	}

	return &GenericBatchConsumer[T]{
		reader:           cfg.Reader,
		repository:       cfg.Repository,
		parser:           cfg.Parser,
		batchSize:        cfg.BatchSize,
		flushInterval:    cfg.FlushInterval,
		workerCount:      cfg.WorkerCount,
		logger:           cfg.Logger,
		consumerName:     cfg.ConsumerName,
		lagMetric:        cfg.LagMetric,
		batchesProcessed: cfg.BatchesProcessed,
		eventsConsumed:   cfg.EventsConsumed,
		consumeDuration:  cfg.ConsumeDuration,
		batch:            make([]*T, 0, cfg.BatchSize),
		messages:         make([]kafka.Message, 0, cfg.BatchSize),
		done:             make(chan struct{}),
	}
}

// Start begins consuming messages from Kafka.
func (c *GenericBatchConsumer[T]) Start(ctx context.Context) {
	c.logger.Info(fmt.Sprintf("starting %s batch consumer", c.consumerName),
		slog.Int("batch_size", c.batchSize),
		slog.Duration("flush_interval", c.flushInterval),
		slog.Int("worker_count", c.workerCount),
	)

	c.ticker = time.NewTicker(c.flushInterval)

	c.wg.Add(1)
	go c.flusher(ctx)

	for i := 0; i < c.workerCount; i++ {
		c.wg.Add(1)
		go c.worker(ctx, i)
	}

	<-ctx.Done()
	c.logger.Info(fmt.Sprintf("context cancelled, stopping %s workers", c.consumerName))

	close(c.done)
}

// worker fetches and processes messages from Kafka.
func (c *GenericBatchConsumer[T]) worker(ctx context.Context, workerID int) {
	defer c.wg.Done()

	c.logger.Info(fmt.Sprintf("%s worker started", c.consumerName), slog.Int("worker_id", workerID))

	for {
		select {
		case <-ctx.Done():
			c.logger.Info(fmt.Sprintf("%s worker stopping", c.consumerName), slog.Int("worker_id", workerID))
			return
		case <-c.done:
			c.logger.Info(fmt.Sprintf("%s worker stopping", c.consumerName), slog.Int("worker_id", workerID))
			return
		default:
			c.processMessage(ctx, workerID)
		}
	}
}

// processMessage fetches and processes a single message.
func (c *GenericBatchConsumer[T]) processMessage(ctx context.Context, workerID int) {
	fetchCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	msg, err := c.reader.FetchMessage(fetchCtx)
	cancel()

	if err != nil {
		if ctx.Err() != nil || fetchCtx.Err() == context.DeadlineExceeded {
			return
		}
		c.logger.Error(fmt.Sprintf("failed to fetch %s message", c.consumerName),
			slog.Int("worker_id", workerID),
			slog.String("error", err.Error()),
		)
		return
	}

	c.updateLagMetric(msg)

	event, err := c.parser(msg.Value)
	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to parse %s message", c.consumerName),
			slog.String("error", err.Error()),
			slog.Int64("offset", msg.Offset),
			slog.Int("partition", msg.Partition),
		)
		if c.eventsConsumed != nil {
			c.eventsConsumed.WithLabelValues("parse_error").Inc()
		}
		if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
			c.logger.Error("failed to commit message after parse error",
				slog.String("error", commitErr.Error()),
			)
		}
		return
	}

	c.batchLock.Lock()
	c.batch = append(c.batch, event)
	c.messages = append(c.messages, msg)
	batchLen := len(c.batch)
	c.batchLock.Unlock()

	c.logger.Debug(fmt.Sprintf("%s message added to batch", c.consumerName),
		slog.Int("worker_id", workerID),
		slog.Int("batch_size", batchLen),
	)

	if batchLen >= c.batchSize {
		c.flushWithContext(ctx)
	}
}

// flusher periodically flushes the batch.
func (c *GenericBatchConsumer[T]) flusher(ctx context.Context) {
	defer c.wg.Done()
	defer c.ticker.Stop()

	c.logger.Info(fmt.Sprintf("%s flusher started", c.consumerName))

	for {
		select {
		case <-ctx.Done():
			c.logger.Info(fmt.Sprintf("%s flusher stopping, final flush", c.consumerName))
			c.flushWithContext(context.Background())
			return
		case <-c.done:
			c.logger.Info(fmt.Sprintf("%s flusher stopping, final flush", c.consumerName))
			c.flushWithContext(context.Background())
			return
		case <-c.ticker.C:
			c.flushWithContext(ctx)
		}
	}
}

// updateLagMetric updates the consumer lag Prometheus metric.
func (c *GenericBatchConsumer[T]) updateLagMetric(msg kafka.Message) {
	if c.lagMetric == nil {
		return
	}
	stats := c.reader.Stats()
	c.lagMetric.WithLabelValues(
		stats.Topic,
		fmt.Sprintf("%d", msg.Partition),
	).Set(float64(stats.Lag))
}

// flushWithContext flushes the current batch to the repository.
func (c *GenericBatchConsumer[T]) flushWithContext(ctx context.Context) {
	c.batchLock.Lock()
	if len(c.batch) == 0 {
		c.batchLock.Unlock()
		return
	}

	events := c.batch
	messages := c.messages
	c.batch = make([]*T, 0, c.batchSize)
	c.messages = make([]kafka.Message, 0, c.batchSize)
	c.batchLock.Unlock()

	startTime := time.Now()
	c.logger.Debug(fmt.Sprintf("flushing %s batch", c.consumerName),
		slog.Int("batch_size", len(events)),
	)

	err := c.repository.InsertBatch(ctx, events)
	duration := time.Since(startTime)
	if c.consumeDuration != nil {
		c.consumeDuration.WithLabelValues("insert_batch").Observe(duration.Seconds())
	}

	if err != nil {
		c.logger.Error(fmt.Sprintf("failed to insert %s batch", c.consumerName),
			slog.Int("batch_size", len(events)),
			slog.Duration("duration", duration),
			slog.String("error", err.Error()),
		)
		if c.batchesProcessed != nil {
			c.batchesProcessed.WithLabelValues("error").Inc()
		}
		return
	}

	if len(messages) > 0 {
		if err := c.reader.CommitMessages(ctx, messages...); err != nil {
			c.logger.Error(fmt.Sprintf("failed to commit %s messages", c.consumerName),
				slog.Int("message_count", len(messages)),
				slog.String("error", err.Error()),
			)
		}
	}

	c.logger.Info(fmt.Sprintf("%s batch flushed successfully", c.consumerName),
		slog.Int("batch_size", len(events)),
		slog.Duration("duration", duration),
	)
	if c.batchesProcessed != nil {
		c.batchesProcessed.WithLabelValues("success").Inc()
	}
	if c.eventsConsumed != nil {
		c.eventsConsumed.WithLabelValues("success").Add(float64(len(events)))
	}
}

// Stop signals the consumer to stop and waits for it to finish.
func (c *GenericBatchConsumer[T]) Stop() {
	c.logger.Info(fmt.Sprintf("stopping %s batch consumer", c.consumerName))
	close(c.done)
	c.wg.Wait()
	c.logger.Info(fmt.Sprintf("%s batch consumer stopped", c.consumerName))
}
