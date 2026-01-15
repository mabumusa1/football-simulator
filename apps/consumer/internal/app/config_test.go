package app

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig_DefaultValues(t *testing.T) {
	// Clear any existing environment variables
	envVars := []string{
		"KAFKA_BOOTSTRAP_SERVERS",
		"KAFKA_TOPIC_PREFIX",
		"KAFKA_TOPIC_EVENTS",
		"KAFKA_TOPIC_RETRY",
		"KAFKA_TOPIC_DEAD",
		"CLICKHOUSE_HOST",
		"CLICKHOUSE_PORT",
		"CLICKHOUSE_DATABASE",
		"CLICKHOUSE_USER",
		"CLICKHOUSE_PASSWORD",
		"CONSUMER_BATCH_SIZE",
		"CONSUMER_FLUSH_INTERVAL",
		"CONSUMER_MAX_RETRIES",
		"CONSUMER_RETRY_BACKOFF",
		"CONSUMER_GROUP",
	}
	for _, env := range envVars {
		os.Unsetenv(env)
	}

	cfg := LoadConfig()

	// Test Kafka defaults
	if cfg.Kafka.BootstrapServers != "kafka:29092" {
		t.Errorf("expected default BootstrapServers 'kafka:29092', got %s", cfg.Kafka.BootstrapServers)
	}
	if cfg.Kafka.TopicPrefix != "football_simulator" {
		t.Errorf("expected default TopicPrefix 'football_simulator', got %s", cfg.Kafka.TopicPrefix)
	}
	if cfg.Kafka.TopicEvents != "football_simulator.events" {
		t.Errorf("expected default TopicEvents 'football_simulator.events', got %s", cfg.Kafka.TopicEvents)
	}
	if cfg.Kafka.TopicRetry != "football_simulator.retry" {
		t.Errorf("expected default TopicRetry 'football_simulator.retry', got %s", cfg.Kafka.TopicRetry)
	}
	if cfg.Kafka.TopicDead != "football_simulator.dead" {
		t.Errorf("expected default TopicDead 'football_simulator.dead', got %s", cfg.Kafka.TopicDead)
	}

	// Test ClickHouse defaults
	if cfg.ClickHouse.Host != "clickhouse" {
		t.Errorf("expected default Host 'clickhouse', got %s", cfg.ClickHouse.Host)
	}
	if cfg.ClickHouse.Port != 9000 {
		t.Errorf("expected default Port 9000, got %d", cfg.ClickHouse.Port)
	}
	if cfg.ClickHouse.Database != "football_simulator" {
		t.Errorf("expected default Database 'football_simulator', got %s", cfg.ClickHouse.Database)
	}
	if cfg.ClickHouse.User != "default" {
		t.Errorf("expected default User 'default', got %s", cfg.ClickHouse.User)
	}
	if cfg.ClickHouse.Password != "" {
		t.Errorf("expected default Password '', got %s", cfg.ClickHouse.Password)
	}

	// Test Consumer defaults
	if cfg.Consumer.BatchSize != 1000 {
		t.Errorf("expected default BatchSize 1000, got %d", cfg.Consumer.BatchSize)
	}
	if cfg.Consumer.FlushInterval != 5*time.Second {
		t.Errorf("expected default FlushInterval 5s, got %v", cfg.Consumer.FlushInterval)
	}
	if cfg.Consumer.MaxRetries != 3 {
		t.Errorf("expected default MaxRetries 3, got %d", cfg.Consumer.MaxRetries)
	}
	if cfg.Consumer.RetryBackoff != 1*time.Second {
		t.Errorf("expected default RetryBackoff 1s, got %v", cfg.Consumer.RetryBackoff)
	}
	if cfg.Consumer.ConsumerGroup != "football_simulator-consumers" {
		t.Errorf("expected default ConsumerGroup 'football_simulator-consumers', got %s", cfg.Consumer.ConsumerGroup)
	}
}

func TestLoadConfig_CustomKafkaValues(t *testing.T) {
	// Set custom Kafka environment variables
	os.Setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	os.Setenv("KAFKA_TOPIC_PREFIX", "custom_prefix")
	os.Setenv("KAFKA_TOPIC_EVENTS", "custom.events")
	os.Setenv("KAFKA_TOPIC_RETRY", "custom.retry")
	os.Setenv("KAFKA_TOPIC_DEAD", "custom.dead")
	defer func() {
		os.Unsetenv("KAFKA_BOOTSTRAP_SERVERS")
		os.Unsetenv("KAFKA_TOPIC_PREFIX")
		os.Unsetenv("KAFKA_TOPIC_EVENTS")
		os.Unsetenv("KAFKA_TOPIC_RETRY")
		os.Unsetenv("KAFKA_TOPIC_DEAD")
	}()

	cfg := LoadConfig()

	if cfg.Kafka.BootstrapServers != "localhost:9092" {
		t.Errorf("expected BootstrapServers 'localhost:9092', got %s", cfg.Kafka.BootstrapServers)
	}
	if cfg.Kafka.TopicPrefix != "custom_prefix" {
		t.Errorf("expected TopicPrefix 'custom_prefix', got %s", cfg.Kafka.TopicPrefix)
	}
	if cfg.Kafka.TopicEvents != "custom.events" {
		t.Errorf("expected TopicEvents 'custom.events', got %s", cfg.Kafka.TopicEvents)
	}
	if cfg.Kafka.TopicRetry != "custom.retry" {
		t.Errorf("expected TopicRetry 'custom.retry', got %s", cfg.Kafka.TopicRetry)
	}
	if cfg.Kafka.TopicDead != "custom.dead" {
		t.Errorf("expected TopicDead 'custom.dead', got %s", cfg.Kafka.TopicDead)
	}
}

func TestLoadConfig_CustomClickHouseValues(t *testing.T) {
	// Set custom ClickHouse environment variables
	os.Setenv("CLICKHOUSE_HOST", "localhost")
	os.Setenv("CLICKHOUSE_PORT", "9001")
	os.Setenv("CLICKHOUSE_DATABASE", "test_db")
	os.Setenv("CLICKHOUSE_USER", "admin")
	os.Setenv("CLICKHOUSE_PASSWORD", "secret123")
	defer func() {
		os.Unsetenv("CLICKHOUSE_HOST")
		os.Unsetenv("CLICKHOUSE_PORT")
		os.Unsetenv("CLICKHOUSE_DATABASE")
		os.Unsetenv("CLICKHOUSE_USER")
		os.Unsetenv("CLICKHOUSE_PASSWORD")
	}()

	cfg := LoadConfig()

	if cfg.ClickHouse.Host != "localhost" {
		t.Errorf("expected Host 'localhost', got %s", cfg.ClickHouse.Host)
	}
	if cfg.ClickHouse.Port != 9001 {
		t.Errorf("expected Port 9001, got %d", cfg.ClickHouse.Port)
	}
	if cfg.ClickHouse.Database != "test_db" {
		t.Errorf("expected Database 'test_db', got %s", cfg.ClickHouse.Database)
	}
	if cfg.ClickHouse.User != "admin" {
		t.Errorf("expected User 'admin', got %s", cfg.ClickHouse.User)
	}
	if cfg.ClickHouse.Password != "secret123" {
		t.Errorf("expected Password 'secret123', got %s", cfg.ClickHouse.Password)
	}
}

func TestLoadConfig_CustomConsumerValues(t *testing.T) {
	// Set custom Consumer environment variables
	os.Setenv("CONSUMER_BATCH_SIZE", "500")
	os.Setenv("CONSUMER_FLUSH_INTERVAL", "10s")
	os.Setenv("CONSUMER_MAX_RETRIES", "5")
	os.Setenv("CONSUMER_RETRY_BACKOFF", "2s")
	os.Setenv("CONSUMER_GROUP", "custom-group")
	defer func() {
		os.Unsetenv("CONSUMER_BATCH_SIZE")
		os.Unsetenv("CONSUMER_FLUSH_INTERVAL")
		os.Unsetenv("CONSUMER_MAX_RETRIES")
		os.Unsetenv("CONSUMER_RETRY_BACKOFF")
		os.Unsetenv("CONSUMER_GROUP")
	}()

	cfg := LoadConfig()

	if cfg.Consumer.BatchSize != 500 {
		t.Errorf("expected BatchSize 500, got %d", cfg.Consumer.BatchSize)
	}
	if cfg.Consumer.FlushInterval != 10*time.Second {
		t.Errorf("expected FlushInterval 10s, got %v", cfg.Consumer.FlushInterval)
	}
	if cfg.Consumer.MaxRetries != 5 {
		t.Errorf("expected MaxRetries 5, got %d", cfg.Consumer.MaxRetries)
	}
	if cfg.Consumer.RetryBackoff != 2*time.Second {
		t.Errorf("expected RetryBackoff 2s, got %v", cfg.Consumer.RetryBackoff)
	}
	if cfg.Consumer.ConsumerGroup != "custom-group" {
		t.Errorf("expected ConsumerGroup 'custom-group', got %s", cfg.Consumer.ConsumerGroup)
	}
}

func TestGetEnv_ExistingVariable(t *testing.T) {
	os.Setenv("TEST_VAR", "test_value")
	defer os.Unsetenv("TEST_VAR")

	result := getEnv("TEST_VAR", "default")
	if result != "test_value" {
		t.Errorf("expected 'test_value', got %s", result)
	}
}

func TestGetEnv_NonExistingVariable(t *testing.T) {
	os.Unsetenv("NON_EXISTING_VAR")

	result := getEnv("NON_EXISTING_VAR", "default_value")
	if result != "default_value" {
		t.Errorf("expected 'default_value', got %s", result)
	}
}

func TestGetEnv_EmptyValue(t *testing.T) {
	os.Setenv("EMPTY_VAR", "")
	defer os.Unsetenv("EMPTY_VAR")

	result := getEnv("EMPTY_VAR", "default")
	if result != "" {
		t.Errorf("expected empty string, got %s", result)
	}
}

func TestGetEnvInt_ValidInteger(t *testing.T) {
	os.Setenv("INT_VAR", "42")
	defer os.Unsetenv("INT_VAR")

	result := getEnvInt("INT_VAR", 10)
	if result != 42 {
		t.Errorf("expected 42, got %d", result)
	}
}

func TestGetEnvInt_InvalidInteger(t *testing.T) {
	os.Setenv("INT_VAR", "not_a_number")
	defer os.Unsetenv("INT_VAR")

	result := getEnvInt("INT_VAR", 10)
	if result != 10 {
		t.Errorf("expected default 10, got %d", result)
	}
}

func TestGetEnvInt_NonExistingVariable(t *testing.T) {
	os.Unsetenv("NON_EXISTING_INT")

	result := getEnvInt("NON_EXISTING_INT", 99)
	if result != 99 {
		t.Errorf("expected default 99, got %d", result)
	}
}

func TestGetEnvInt_NegativeInteger(t *testing.T) {
	os.Setenv("INT_VAR", "-5")
	defer os.Unsetenv("INT_VAR")

	result := getEnvInt("INT_VAR", 10)
	if result != -5 {
		t.Errorf("expected -5, got %d", result)
	}
}

func TestGetEnvInt_Zero(t *testing.T) {
	os.Setenv("INT_VAR", "0")
	defer os.Unsetenv("INT_VAR")

	result := getEnvInt("INT_VAR", 10)
	if result != 0 {
		t.Errorf("expected 0, got %d", result)
	}
}

func TestGetEnvDuration_ValidDuration(t *testing.T) {
	testCases := []struct {
		input    string
		expected time.Duration
	}{
		{"10s", 10 * time.Second},
		{"5m", 5 * time.Minute},
		{"1h", 1 * time.Hour},
		{"100ms", 100 * time.Millisecond},
		{"2h30m", 2*time.Hour + 30*time.Minute},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			os.Setenv("DURATION_VAR", tc.input)
			defer os.Unsetenv("DURATION_VAR")

			result := getEnvDuration("DURATION_VAR", time.Second)
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestGetEnvDuration_InvalidDuration(t *testing.T) {
	os.Setenv("DURATION_VAR", "not_a_duration")
	defer os.Unsetenv("DURATION_VAR")

	result := getEnvDuration("DURATION_VAR", 5*time.Second)
	if result != 5*time.Second {
		t.Errorf("expected default 5s, got %v", result)
	}
}

func TestGetEnvDuration_NonExistingVariable(t *testing.T) {
	os.Unsetenv("NON_EXISTING_DURATION")

	result := getEnvDuration("NON_EXISTING_DURATION", 30*time.Second)
	if result != 30*time.Second {
		t.Errorf("expected default 30s, got %v", result)
	}
}

func TestGetEnvDuration_EmptyValue(t *testing.T) {
	os.Setenv("DURATION_VAR", "")
	defer os.Unsetenv("DURATION_VAR")

	result := getEnvDuration("DURATION_VAR", 5*time.Second)
	if result != 5*time.Second {
		t.Errorf("expected default 5s, got %v", result)
	}
}

func TestLoadConfig_PartialOverride(t *testing.T) {
	// Only set some values, others should use defaults
	os.Setenv("KAFKA_BOOTSTRAP_SERVERS", "custom-kafka:9092")
	os.Setenv("CONSUMER_BATCH_SIZE", "2000")
	defer func() {
		os.Unsetenv("KAFKA_BOOTSTRAP_SERVERS")
		os.Unsetenv("CONSUMER_BATCH_SIZE")
	}()

	cfg := LoadConfig()

	// Custom values
	if cfg.Kafka.BootstrapServers != "custom-kafka:9092" {
		t.Errorf("expected BootstrapServers 'custom-kafka:9092', got %s", cfg.Kafka.BootstrapServers)
	}
	if cfg.Consumer.BatchSize != 2000 {
		t.Errorf("expected BatchSize 2000, got %d", cfg.Consumer.BatchSize)
	}

	// Default values should still work
	if cfg.Kafka.TopicEvents != "football_simulator.events" {
		t.Errorf("expected default TopicEvents, got %s", cfg.Kafka.TopicEvents)
	}
	if cfg.Consumer.MaxRetries != 3 {
		t.Errorf("expected default MaxRetries 3, got %d", cfg.Consumer.MaxRetries)
	}
}
