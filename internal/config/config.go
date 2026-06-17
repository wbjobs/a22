package config

import (
	"os"
)

type Config struct {
	GatewayPort      string
	QueryAPIPort     string
	FunctionAURL     string
	FunctionBURL     string
	FunctionCURL     string
	KafkaBrokers     string
	KafkaTopic       string
	KafkaGroupID     string
	ElasticsearchURL string
	ESIndex          string
}

func Load() *Config {
	return &Config{
		GatewayPort:      getEnv("GATEWAY_PORT", "8080"),
		QueryAPIPort:     getEnv("QUERY_API_PORT", "8081"),
		FunctionAURL:     getEnv("FUNCTION_A_URL", "http://function-a:8082/order"),
		FunctionBURL:     getEnv("FUNCTION_B_URL", "http://function-b:8083/payment"),
		FunctionCURL:     getEnv("FUNCTION_C_URL", "http://function-c:8084/notify"),
		KafkaBrokers:     getEnv("KAFKA_BROKERS", "kafka:9092"),
		KafkaTopic:       getEnv("KAFKA_TOPIC", "trace-spans"),
		KafkaGroupID:     getEnv("KAFKA_GROUP_ID", "trace-consumer-group"),
		ElasticsearchURL: getEnv("ELASTICSEARCH_URL", "http://elasticsearch:9200"),
		ESIndex:          getEnv("ES_INDEX", "trace-spans"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
