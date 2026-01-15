package app

import (
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration.
type Config struct {
	Kafka      KafkaConfig
	ClickHouse ClickHouseConfig
	Consumer   ConsumerConfig
}

// KafkaConfig holds Kafka connection and topic settings.
type KafkaConfig struct {
	BootstrapServers string
	TopicPrefix      string
	TopicEvents      string
	TopicRetry       string
	TopicDead        string
}

// ClickHouseConfig holds ClickHouse connection settings.
type ClickHouseConfig struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
}

// ConsumerConfig holds Kafka consumer and batch processing settings.
type ConsumerConfig struct {
	BatchSize     int
	FlushInterval time.Duration
	MaxRetries    int
	RetryBackoff  time.Duration
	ConsumerGroup string
}

// LoadConfig reads configuration from environment variables with sensible defaults.
func LoadConfig() *Config {
	return &Config{
		Kafka: KafkaConfig{
			BootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
			TopicPrefix:      getEnv("KAFKA_TOPIC_PREFIX", "fanfinity"),
			TopicEvents:      getEnv("KAFKA_TOPIC_EVENTS", "fanfinity.events"),
			TopicRetry:       getEnv("KAFKA_TOPIC_RETRY", "fanfinity.retry"),
			TopicDead:        getEnv("KAFKA_TOPIC_DEAD", "fanfinity.dead"),
		},
		ClickHouse: ClickHouseConfig{
			Host:     getEnv("CLICKHOUSE_HOST", "clickhouse"),
			Port:     getEnvInt("CLICKHOUSE_PORT", 9000),
			Database: getEnv("CLICKHOUSE_DATABASE", "fanfinity"),
			User:     getEnv("CLICKHOUSE_USER", "default"),
			Password: getEnv("CLICKHOUSE_PASSWORD", ""),
		},
		Consumer: ConsumerConfig{
			BatchSize:     getEnvInt("CONSUMER_BATCH_SIZE", 1000),
			FlushInterval: getEnvDuration("CONSUMER_FLUSH_INTERVAL", 5*time.Second),
			MaxRetries:    getEnvInt("CONSUMER_MAX_RETRIES", 3),
			RetryBackoff:  getEnvDuration("CONSUMER_RETRY_BACKOFF", 1*time.Second),
			ConsumerGroup: getEnv("CONSUMER_GROUP", "fanfinity-consumers"),
		},
	}
}

// getEnv retrieves an environment variable or returns a default value.
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// getEnvInt retrieves an environment variable as an integer or returns a default value.
func getEnvInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

// getEnvDuration retrieves an environment variable as a duration or returns a default value.
// Accepts formats like "10s", "5m", "1h".
func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value, exists := os.LookupEnv(key); exists {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
