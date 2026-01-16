package app

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig_DefaultValues(t *testing.T) {
	// Clear any existing environment variables
	envVars := []string{
		"SERVER_HOST",
		"SERVER_PORT",
		"SERVER_READ_TIMEOUT",
		"SERVER_WRITE_TIMEOUT",
		"SERVER_IDLE_TIMEOUT",
		"KAFKA_BOOTSTRAP_SERVERS",
		"KAFKA_TOPIC_PREFIX",
		"KAFKA_TOPIC_EVENTS",
		"KAFKA_TOPIC_ENGAGEMENTS",
		"KAFKA_TOPIC_RETRY",
		"KAFKA_TOPIC_DEAD",
		"KAFKA_PRODUCER_TIMEOUT",
		"CLICKHOUSE_HOST",
		"CLICKHOUSE_PORT",
		"CLICKHOUSE_DATABASE",
		"CLICKHOUSE_USER",
		"CLICKHOUSE_PASSWORD",
		"API_KEY",
	}
	for _, env := range envVars {
		_ = os.Unsetenv(env)
	}

	cfg := LoadConfig()

	// Test Server defaults
	if cfg.Server.Host != "0.0.0.0" {
		t.Errorf("expected default Server.Host '0.0.0.0', got %s", cfg.Server.Host)
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("expected default Server.Port 8080, got %d", cfg.Server.Port)
	}
	if cfg.Server.ReadTimeout != 10*time.Second {
		t.Errorf("expected default Server.ReadTimeout 10s, got %v", cfg.Server.ReadTimeout)
	}
	if cfg.Server.WriteTimeout != 10*time.Second {
		t.Errorf("expected default Server.WriteTimeout 10s, got %v", cfg.Server.WriteTimeout)
	}
	if cfg.Server.IdleTimeout != 60*time.Second {
		t.Errorf("expected default Server.IdleTimeout 60s, got %v", cfg.Server.IdleTimeout)
	}

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
	if cfg.Kafka.TopicEngagements != "football_simulator.engagements" {
		t.Errorf("expected default TopicEngagements 'football_simulator.engagements', got %s", cfg.Kafka.TopicEngagements)
	}
	if cfg.Kafka.TopicRetry != "football_simulator.retry" {
		t.Errorf("expected default TopicRetry 'football_simulator.retry', got %s", cfg.Kafka.TopicRetry)
	}
	if cfg.Kafka.TopicDead != "football_simulator.dead" {
		t.Errorf("expected default TopicDead 'football_simulator.dead', got %s", cfg.Kafka.TopicDead)
	}
	if cfg.Kafka.ProducerTimeout != 10*time.Second {
		t.Errorf("expected default ProducerTimeout 10s, got %v", cfg.Kafka.ProducerTimeout)
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

	// Test API Key default
	if cfg.APIKey != "" {
		t.Errorf("expected default APIKey '', got %s", cfg.APIKey)
	}
}

func TestLoadConfig_CustomServerValues(t *testing.T) {
	t.Setenv("SERVER_HOST", "127.0.0.1")
	t.Setenv("SERVER_PORT", "3000")
	t.Setenv("SERVER_READ_TIMEOUT", "30s")
	t.Setenv("SERVER_WRITE_TIMEOUT", "45s")
	t.Setenv("SERVER_IDLE_TIMEOUT", "120s")

	cfg := LoadConfig()

	if cfg.Server.Host != "127.0.0.1" {
		t.Errorf("expected Server.Host '127.0.0.1', got %s", cfg.Server.Host)
	}
	if cfg.Server.Port != 3000 {
		t.Errorf("expected Server.Port 3000, got %d", cfg.Server.Port)
	}
	if cfg.Server.ReadTimeout != 30*time.Second {
		t.Errorf("expected Server.ReadTimeout 30s, got %v", cfg.Server.ReadTimeout)
	}
	if cfg.Server.WriteTimeout != 45*time.Second {
		t.Errorf("expected Server.WriteTimeout 45s, got %v", cfg.Server.WriteTimeout)
	}
	if cfg.Server.IdleTimeout != 120*time.Second {
		t.Errorf("expected Server.IdleTimeout 120s, got %v", cfg.Server.IdleTimeout)
	}
}

func TestLoadConfig_CustomKafkaValues(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	t.Setenv("KAFKA_TOPIC_PREFIX", "custom_prefix")
	t.Setenv("KAFKA_TOPIC_EVENTS", "custom.events")
	t.Setenv("KAFKA_TOPIC_ENGAGEMENTS", "custom.engagements")
	t.Setenv("KAFKA_TOPIC_RETRY", "custom.retry")
	t.Setenv("KAFKA_TOPIC_DEAD", "custom.dead")
	t.Setenv("KAFKA_PRODUCER_TIMEOUT", "20s")

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
	if cfg.Kafka.TopicEngagements != "custom.engagements" {
		t.Errorf("expected TopicEngagements 'custom.engagements', got %s", cfg.Kafka.TopicEngagements)
	}
	if cfg.Kafka.TopicRetry != "custom.retry" {
		t.Errorf("expected TopicRetry 'custom.retry', got %s", cfg.Kafka.TopicRetry)
	}
	if cfg.Kafka.TopicDead != "custom.dead" {
		t.Errorf("expected TopicDead 'custom.dead', got %s", cfg.Kafka.TopicDead)
	}
	if cfg.Kafka.ProducerTimeout != 20*time.Second {
		t.Errorf("expected ProducerTimeout 20s, got %v", cfg.Kafka.ProducerTimeout)
	}
}

func TestLoadConfig_CustomClickHouseValues(t *testing.T) {
	t.Setenv("CLICKHOUSE_HOST", "localhost")
	t.Setenv("CLICKHOUSE_PORT", "9001")
	t.Setenv("CLICKHOUSE_DATABASE", "test_db")
	t.Setenv("CLICKHOUSE_USER", "admin")
	t.Setenv("CLICKHOUSE_PASSWORD", "secret123")

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

func TestLoadConfig_CustomAPIKey(t *testing.T) {
	t.Setenv("API_KEY", "my-secret-api-key")

	cfg := LoadConfig()

	if cfg.APIKey != "my-secret-api-key" {
		t.Errorf("expected APIKey 'my-secret-api-key', got %s", cfg.APIKey)
	}
}

func TestGetEnv_ExistingVariable(t *testing.T) {
	t.Setenv("TEST_VAR", "test_value")

	result := getEnv("TEST_VAR", "default")
	if result != "test_value" {
		t.Errorf("expected 'test_value', got %s", result)
	}
}

func TestGetEnv_NonExistingVariable(t *testing.T) {
	_ = os.Unsetenv("NON_EXISTING_VAR")

	result := getEnv("NON_EXISTING_VAR", "default_value")
	if result != "default_value" {
		t.Errorf("expected 'default_value', got %s", result)
	}
}

func TestGetEnv_EmptyValue(t *testing.T) {
	t.Setenv("EMPTY_VAR", "")

	result := getEnv("EMPTY_VAR", "default")
	if result != "" {
		t.Errorf("expected empty string, got %s", result)
	}
}

func TestGetEnvInt_ValidInteger(t *testing.T) {
	t.Setenv("INT_VAR", "42")

	result := getEnvInt("INT_VAR", 10)
	if result != 42 {
		t.Errorf("expected 42, got %d", result)
	}
}

func TestGetEnvInt_InvalidInteger(t *testing.T) {
	t.Setenv("INT_VAR", "not_a_number")

	result := getEnvInt("INT_VAR", 10)
	if result != 10 {
		t.Errorf("expected default 10, got %d", result)
	}
}

func TestGetEnvInt_NonExistingVariable(t *testing.T) {
	_ = os.Unsetenv("NON_EXISTING_INT")

	result := getEnvInt("NON_EXISTING_INT", 99)
	if result != 99 {
		t.Errorf("expected default 99, got %d", result)
	}
}

func TestGetEnvInt_NegativeInteger(t *testing.T) {
	t.Setenv("INT_VAR", "-5")

	result := getEnvInt("INT_VAR", 10)
	if result != -5 {
		t.Errorf("expected -5, got %d", result)
	}
}

func TestGetEnvInt_Zero(t *testing.T) {
	t.Setenv("INT_VAR", "0")

	result := getEnvInt("INT_VAR", 10)
	if result != 0 {
		t.Errorf("expected 0, got %d", result)
	}
}

func TestGetEnvInt_FloatValue(t *testing.T) {
	t.Setenv("INT_VAR", "3.14")

	result := getEnvInt("INT_VAR", 10)
	if result != 10 {
		t.Errorf("expected default 10 for float input, got %d", result)
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
		{"500us", 500 * time.Microsecond},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			t.Setenv("DURATION_VAR", tc.input)

			result := getEnvDuration("DURATION_VAR", time.Second)
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestGetEnvDuration_InvalidDuration(t *testing.T) {
	t.Setenv("DURATION_VAR", "not_a_duration")

	result := getEnvDuration("DURATION_VAR", 5*time.Second)
	if result != 5*time.Second {
		t.Errorf("expected default 5s, got %v", result)
	}
}

func TestGetEnvDuration_NonExistingVariable(t *testing.T) {
	_ = os.Unsetenv("NON_EXISTING_DURATION")

	result := getEnvDuration("NON_EXISTING_DURATION", 30*time.Second)
	if result != 30*time.Second {
		t.Errorf("expected default 30s, got %v", result)
	}
}

func TestGetEnvDuration_EmptyValue(t *testing.T) {
	t.Setenv("DURATION_VAR", "")

	result := getEnvDuration("DURATION_VAR", 5*time.Second)
	if result != 5*time.Second {
		t.Errorf("expected default 5s, got %v", result)
	}
}

func TestGetEnvDuration_NumericWithoutUnit(t *testing.T) {
	t.Setenv("DURATION_VAR", "100")

	result := getEnvDuration("DURATION_VAR", 5*time.Second)
	// "100" without unit is invalid, should return default
	if result != 5*time.Second {
		t.Errorf("expected default 5s for numeric without unit, got %v", result)
	}
}

func TestLoadConfig_PartialOverride(t *testing.T) {
	// Clear all first
	_ = os.Unsetenv("SERVER_HOST")
	_ = os.Unsetenv("SERVER_PORT")

	// Only set some values, others should use defaults
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "custom-kafka:9092")
	t.Setenv("SERVER_PORT", "9000")

	cfg := LoadConfig()

	// Custom values
	if cfg.Kafka.BootstrapServers != "custom-kafka:9092" {
		t.Errorf("expected BootstrapServers 'custom-kafka:9092', got %s", cfg.Kafka.BootstrapServers)
	}
	if cfg.Server.Port != 9000 {
		t.Errorf("expected Server.Port 9000, got %d", cfg.Server.Port)
	}

	// Default values should still work
	if cfg.Server.Host != "0.0.0.0" {
		t.Errorf("expected default Server.Host '0.0.0.0', got %s", cfg.Server.Host)
	}
	if cfg.Kafka.TopicEvents != "football_simulator.events" {
		t.Errorf("expected default TopicEvents, got %s", cfg.Kafka.TopicEvents)
	}
}

func TestConfig_Structs(t *testing.T) {
	// Test that Config struct has all expected fields
	cfg := &Config{
		Server: ServerConfig{
			Host:         "localhost",
			Port:         8080,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
		Kafka: KafkaConfig{
			BootstrapServers: "kafka:9092",
			TopicPrefix:      "prefix",
			TopicEvents:      "events",
			TopicEngagements: "engagements",
			TopicRetry:       "retry",
			TopicDead:        "dead",
			ProducerTimeout:  10 * time.Second,
		},
		ClickHouse: ClickHouseConfig{
			Host:     "clickhouse",
			Port:     9000,
			Database: "db",
			User:     "user",
			Password: "pass",
		},
		APIKey: "key",
	}

	// Verify values are set correctly
	if cfg.Server.Host != "localhost" {
		t.Error("Config struct Server.Host not set correctly")
	}
	if cfg.Kafka.BootstrapServers != "kafka:9092" {
		t.Error("Config struct Kafka.BootstrapServers not set correctly")
	}
	if cfg.ClickHouse.Host != "clickhouse" {
		t.Error("Config struct ClickHouse.Host not set correctly")
	}
	if cfg.APIKey != "key" {
		t.Error("Config struct APIKey not set correctly")
	}
}
