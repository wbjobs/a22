package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"ebpf-serverless-tracing/internal/config"
	"ebpf-serverless-tracing/internal/model"
)

type App struct {
	cfg        *config.Config
	writer     *kafka.Writer
	mu         sync.Mutex
	stats      ProducerStats
}

type ProducerStats struct {
	MessagesSent     int64     `json:"messages_sent"`
	MessagesFailed   int64     `json:"messages_failed"`
	LastMessageTime  time.Time `json:"last_message_time"`
	LastRequestID    string    `json:"last_request_id"`
}

func main() {
	cfg := config.Load()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(cfg.KafkaBrokers, ",")...),
		Topic:        cfg.KafkaTopic,
		Balancer:     &kafka.Hash{},
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireOne,
		Async:        true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				log.Printf("[KafkaProducer] Async write error: %v", err)
			}
		},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	app := &App{
		cfg:    cfg,
		writer: writer,
	}

	r := gin.Default()

	r.POST("/v1/spans", app.handleSpan)
	r.POST("/v1/spans/batch", app.handleBatchSpans)
	r.GET("/health", app.healthCheck)
	r.GET("/stats", app.getStats)

	port := getEnv("KAFKA_PRODUCER_PORT", "8085")

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

	go func() {
		log.Printf("Kafka Producer Service starting on port %s, topic: %s, brokers: %s",
			port, cfg.KafkaTopic, cfg.KafkaBrokers)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	go app.simulateDirectCapture(cfg)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Kafka Producer Service...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}

	log.Println("Kafka Producer Service stopped")
}

func (a *App) handleSpan(c *gin.Context) {
	var span model.TraceSpan
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read body"})
		return
	}

	if err := json.Unmarshal(body, &span); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON: " + err.Error()})
		return
	}

	if span.RequestID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing request_id"})
		return
	}

	if span.SpanID == "" {
		span.SpanID = uuid.New().String()
	}
	if span.Timestamp.IsZero() {
		span.Timestamp = time.Now()
	}
	if span.StartTime.IsZero() {
		span.StartTime = span.Timestamp
	}
	if span.EndTime.IsZero() {
		span.EndTime = span.StartTime.Add(time.Duration(span.DurationMs) * time.Millisecond)
	}

	if err := a.sendSpan(context.Background(), &span); err != nil {
		a.incrementFailed()
		log.Printf("[KafkaProducer] Failed to send span req=%s: %v", span.RequestID, err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Failed to send to Kafka: " + err.Error()})
		return
	}

	a.incrementSent(span.RequestID)
	c.JSON(http.StatusAccepted, gin.H{
		"status":     "accepted",
		"request_id": span.RequestID,
		"span_id":    span.SpanID,
	})
}

func (a *App) handleBatchSpans(c *gin.Context) {
	var spans []model.TraceSpan
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read body"})
		return
	}

	if err := json.Unmarshal(body, &spans); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON: " + err.Error()})
		return
	}

	if len(spans) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Empty batch"})
		return
	}

	ctx := context.Background()
	var sent, failed int
	var lastReqID string

	for i := range spans {
		span := &spans[i]
		if span.RequestID == "" {
			continue
		}
		if span.SpanID == "" {
			span.SpanID = uuid.New().String()
		}
		if span.Timestamp.IsZero() {
			span.Timestamp = time.Now()
		}

		if err := a.sendSpan(ctx, span); err != nil {
			failed++
			log.Printf("[KafkaProducer] Batch failed span req=%s: %v", span.RequestID, err)
		} else {
			sent++
			lastReqID = span.RequestID
		}
	}

	a.mu.Lock()
	a.stats.MessagesSent += int64(sent)
	a.stats.MessagesFailed += int64(failed)
	if lastReqID != "" {
		a.stats.LastRequestID = lastReqID
		a.stats.LastMessageTime = time.Now()
	}
	a.mu.Unlock()

	c.JSON(http.StatusAccepted, gin.H{
		"status":       "processed",
		"total":        len(spans),
		"sent":         sent,
		"failed":       failed,
		"last_request": lastReqID,
	})
}

func (a *App) sendSpan(ctx context.Context, span *model.TraceSpan) error {
	spanBytes, err := json.Marshal(span)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(span.RequestID),
		Value: spanBytes,
		Time:  span.Timestamp,
		Headers: []kafka.Header{
			{Key: "request_id", Value: []byte(span.RequestID)},
			{Key: "trace_id", Value: []byte(span.TraceID)},
			{Key: "function_name", Value: []byte(span.FunctionName)},
		},
	}

	return a.writer.WriteMessages(ctx, msg)
}

func (a *App) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"service": "kafka-producer",
		"status":  "healthy",
		"time":    time.Now().UTC(),
		"topic":   a.cfg.KafkaTopic,
		"brokers": a.cfg.KafkaBrokers,
	})
}

func (a *App) getStats(c *gin.Context) {
	a.mu.Lock()
	stats := a.stats
	a.mu.Unlock()
	c.JSON(http.StatusOK, gin.H{
		"service": "kafka-producer",
		"stats":   stats,
	})
}

func (a *App) incrementSent(reqID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stats.MessagesSent++
	a.stats.LastMessageTime = time.Now()
	a.stats.LastRequestID = reqID
}

func (a *App) incrementFailed() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stats.MessagesFailed++
}

func (a *App) simulateDirectCapture(cfg *config.Config) {
	time.Sleep(10 * time.Second)
	log.Println("[KafkaProducer] Starting direct HTTP capture fallback mode (simulates eBPF)")

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		a.mu.Lock()
		stats := a.stats
		a.mu.Unlock()
		log.Printf("[KafkaProducer] Stats: sent=%d failed=%d last=%s",
			stats.MessagesSent, stats.MessagesFailed, stats.LastRequestID)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
