package natsutil

const (
	DefaultStreamName     = "TRACES"
	DefaultSubjectPrefix  = "trace.spans"
	DefaultConsumerName   = "es-consumer"
	DefaultMaxMsgs        = 100000
	DefaultMaxBytes       = 1 * 1024 * 1024 * 1024
	DefaultReplicas       = 1
	DefaultRetentionHours = 168
)

type NATSConfig struct {
	URLs           []string
	StreamName     string
	SubjectPrefix  string
	ConsumerName   string
	MaxMsgs        int64
	MaxBytes       int64
	Replicas       int
	RetentionHours int
}

func DefaultConfig() *NATSConfig {
	return &NATSConfig{
		URLs:           []string{"nats://nats:4222"},
		StreamName:     DefaultStreamName,
		SubjectPrefix:  DefaultSubjectPrefix,
		ConsumerName:   DefaultConsumerName,
		MaxMsgs:        DefaultMaxMsgs,
		MaxBytes:       DefaultMaxBytes,
		Replicas:       DefaultReplicas,
		RetentionHours: DefaultRetentionHours,
	}
}

func ConfigFromEnv() *NATSConfig {
	cfg := DefaultConfig()
	return cfg
}
