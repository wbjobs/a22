package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	"ebpf-serverless-tracing/internal/config"
	"ebpf-serverless-tracing/internal/model"
	"ebpf-serverless-tracing/internal/natsutil"
)

type App struct {
	cfg         *config.Config
	consumer    *natsutil.NATSTraceConsumer
	es          *elasticsearch.Client
	mu          sync.Mutex
	stats       ConsumerStats
	batchBuffer []model.TraceSpan
	batchSize   int
	batchTimer  *time.Timer
	flushLock   sync.Mutex
}

type ConsumerStats struct {
	MessagesConsumed int64     `json:"messages_consumed"`
	MessagesIndexed  int64     `json:"messages_indexed"`
	MessagesFailed   int64     `json:"messages_failed"`
	BatchesProcessed int64     `json:"batches_processed"`
	LastIndexTime    time.Time `json:"last_index_time"`
	LastRequestID    string    `json:"last_request_id"`
	Uptime           time.Time `json:"uptime"`
	NATSLag          int64     `json:"nats_lag"`
}

func main() {
	cfg := config.Load()

	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: strings.Split(cfg.ElasticsearchURL, ","),
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   32,
			ResponseHeaderTimeout: 30 * time.Second,
			IdleConnTimeout:       90 * time.Second,
			ForceAttemptHTTP2:     true,
		},
		RetryOnStatus: []int{502, 503, 504, 429},
		MaxRetries:    5,
		RetryBackoff: func(i int) time.Duration { return time.Duration(i) * 200 * time.Millisecond },
	})
	if err != nil {
		log.Fatalf("[NATS Consumer] Failed to create ES client: %v", err)
	}

	app := &App{
		cfg:         cfg,
		es:          es,
		stats:       ConsumerStats{Uptime: time.Now()},
		batchBuffer: make([]model.TraceSpan, 0, 500),
		batchSize:   500,
	}

	go app.ensureESIndex()

	natsURLs := strings.Split(getEnv("NATS_URLS", "nats://nats:4222"), ",")
	natsCfg := &natsutil.NATSConfig{
		URLs:           natsURLs,
		StreamName:     getEnv("NATS_STREAM", "TRACES"),
		SubjectPrefix:  "trace.spans",
		ConsumerName:   getEnv("NATS_CONSUMER", "es-consumer"),
		MaxMsgs:        100000,
		MaxBytes:       2 * 1024 * 1024 * 1024,
		Replicas:       1,
		RetentionHours: 168,
	}

	consumer, err := natsutil.NewNATSConsumer(natsCfg, app.handleSpan)
	if err != nil {
		log.Fatalf("[NATS Consumer] Failed to create NATS consumer: %v", err)
	}
	app.consumer = consumer

	go app.startHealthServer()

	app.batchTimer = time.AfterFunc(500*time.Millisecond, app.flushBatch)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		log.Printf("[NATS Consumer] Starting consumption from stream=%s, urls=%v",
			natsCfg.StreamName, natsURLs)
		if err := consumer.Consume(ctx); err != nil {
			log.Printf("[NATS Consumer] Consumption error: %v", err)
		}
	}()

	go app.statsReporter()

	<-quit
	log.Println("[NATS Consumer] Shutting down...")
	cancel()

	app.flushBatch()
	consumer.Close()

	log.Println("[NATS Consumer] Stopped")
}

func (a *App) handleSpan(ctx context.Context, span *model.TraceSpan) error {
	if span == nil || span.RequestID == "" {
		return nil
	}

	a.mu.Lock()
	a.stats.MessagesConsumed++
	a.stats.LastRequestID = span.RequestID
	a.batchBuffer = append(a.batchBuffer, *span)
	shouldFlush := len(a.batchBuffer) >= a.batchSize
	a.mu.Unlock()

	if shouldFlush {
		a.flushBatch()
	}

	return nil
}

func (a *App) flushBatch() {
	a.flushLock.Lock()
	defer a.flushLock.Unlock()

	a.mu.Lock()
	buffer := a.batchBuffer
	a.batchBuffer = make([]model.TraceSpan, 0, a.batchSize)
	a.mu.Unlock()

	if len(buffer) == 0 {
		if a.batchTimer != nil {
			a.batchTimer.Reset(500 * time.Millisecond)
		}
		return
	}

	indexed, failed, err := a.indexSpans(buffer)
	if err != nil {
		log.Printf("[NATS Consumer] ES index error: %v (%d/%d failed)", err, failed, len(buffer))
		a.mu.Lock()
		a.stats.MessagesFailed += int64(failed)
		a.stats.MessagesIndexed += int64(indexed)
		a.mu.Unlock()
	} else {
		a.mu.Lock()
		a.stats.MessagesIndexed += int64(indexed)
		a.stats.BatchesProcessed++
		a.stats.LastIndexTime = time.Now()
		a.mu.Unlock()

		if len(buffer) >= 50 {
			log.Printf("[NATS Consumer] Indexed %d spans to ES (total: %d)",
				len(buffer), a.stats.MessagesIndexed)
		}
	}

	if a.batchTimer != nil {
		a.batchTimer.Reset(500 * time.Millisecond)
	}
}

func (a *App) indexSpans(spans []model.TraceSpan) (int, int, error) {
	var buf bytes.Buffer

	for _, span := range spans {
		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": a.cfg.ESIndex,
				"_id":    span.SpanID,
			},
		}
		metaJSON, _ := json.Marshal(meta)
		buf.Write(metaJSON)
		buf.WriteByte('\n')

		spanJSON, _ := json.Marshal(span)
		buf.Write(spanJSON)
		buf.WriteByte('\n')
	}

	req := esapi.BulkRequest{
		Body:    &buf,
		Refresh: "false",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	resp, err := req.Do(ctx, a.es)
	if err != nil {
		return 0, len(spans), fmt.Errorf("bulk request: %w", err)
	}
	defer resp.Body.Close()

	if resp.IsError() {
		var errBody map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&errBody)
		return 0, len(spans), fmt.Errorf("ES returned %d: %v", resp.StatusCode, errBody)
	}

	var bulkResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&bulkResp)

	errors, _ := bulkResp["errors"].(bool)
	failed := 0

	if errors {
		items, _ := bulkResp["items"].([]interface{})
		for _, item := range items {
			itemMap, _ := item.(map[string]interface{})
			indexData, _ := itemMap["index"].(map[string]interface{})
			if status, ok := indexData["status"].(int); ok && status >= 300 {
				failed++
			}
		}
	}

	return len(spans) - failed, failed, nil
}

func (a *App) ensureESIndex() {
	for i := 0; i < 60; i++ {
		time.Sleep(2 * time.Second)

		existsReq := esapi.IndicesExistsRequest{Index: []string{a.cfg.ESIndex}}
		existsResp, err := existsReq.Do(context.Background(), a.es)
		if err != nil {
			log.Printf("[NATS Consumer] ES waiting: %v", err)
			continue
		}
		existsResp.Body.Close()

		if existsResp.StatusCode == 200 {
			log.Printf("[NATS Consumer] ES index '%s' exists", a.cfg.ESIndex)
			return
		}

		mapping := `{
			"settings": {
				"number_of_shards": 3,
				"number_of_replicas": 0,
				"refresh_interval": "5s",
				"index.translog.durability": "async",
				"index.translog.sync_interval": "5s"
			},
			"mappings": {
				"properties": {
					"request_id": {"type": "keyword"},
					"trace_id": {"type": "keyword"},
					"span_id": {"type": "keyword"},
					"parent_span_id": {"type": "keyword"},
					"function_name": {"type": "keyword"},
					"service_name": {"type": "keyword"},
					"start_time": {"type": "date"},
					"end_time": {"type": "date"},
					"timestamp": {"type": "date"},
					"duration_ms": {"type": "long"},
					"status_code": {"type": "integer"},
					"method": {"type": "keyword"},
					"path": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
					"source_ip": {"type": "ip"},
					"dest_ip": {"type": "ip"},
					"source_port": {"type": "integer"},
					"dest_port": {"type": "integer"},
					"protocol": {"type": "keyword"}
				}
			}
		}`

		createReq := esapi.IndicesCreateRequest{
			Index: a.cfg.ESIndex,
			Body:  strings.NewReader(mapping),
		}
		createResp, err := createReq.Do(context.Background(), a.es)
		if err != nil {
			log.Printf("[NATS Consumer] Create index error: %v", err)
			continue
		}
		createResp.Body.Close()

		if createResp.StatusCode == 400 {
			log.Printf("[NATS Consumer] ES index '%s' already exists", a.cfg.ESIndex)
			return
		}

		if !createResp.IsError() {
			log.Printf("[NATS Consumer] ES index '%s' created", a.cfg.ESIndex)
			return
		}
	}

	log.Printf("[NATS Consumer] WARNING: Could not create ES index")
}

func (a *App) statsReporter() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		a.mu.Lock()
		stats := a.stats
		a.mu.Unlock()

		natsStats := a.consumer.Stats()
		log.Printf("[NATS Consumer] Stats: consumed=%d indexed=%d failed=%d batches=%d | NATS: consumed=%d processed=%d errors=%d pending=%d",
			stats.MessagesConsumed, stats.MessagesIndexed, stats.MessagesFailed, stats.BatchesProcessed,
			natsStats.Consumed, natsStats.Processed, natsStats.Errors, natsStats.Pending)
	}
}

func (a *App) startHealthServer() {
	port := getEnv("NATS_CONSUMER_PORT", "8086")

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		esHealthReq := esapi.ClusterHealthRequest{
			Timeout: 5 * time.Second,
		}
		esResp, err := esHealthReq.Do(context.Background(), a.es)
		esStatus := "unknown"
		if err == nil {
			defer esResp.Body.Close()
			var health map[string]interface{}
			json.NewDecoder(esResp.Body).Decode(&health)
			if s, ok := health["status"].(string); ok {
				esStatus = s
			}
		}

		natsStatus := "disconnected"
		if a.consumer != nil && a.consumer.Connected() {
			natsStatus = "connected"
		}

		a.mu.Lock()
		stats := a.stats
		a.mu.Unlock()

		resp := map[string]interface{}{
			"service":    "nats-trace-consumer",
			"status":     "healthy",
			"es_status":  esStatus,
			"nats_status": natsStatus,
			"es_url":     a.cfg.ElasticsearchURL,
			"stream":     getEnv("NATS_STREAM", "TRACES"),
			"stats":      stats,
			"uptime_seconds": int64(time.Since(stats.Uptime).Seconds()),
			"time":       time.Now().UTC(),
		}
		json.NewEncoder(w).Encode(resp)
	})

	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		stats := a.stats
		a.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"service": "nats-trace-consumer",
			"stats":   stats,
			"nats":    a.consumer.Stats(),
		})
	})

	http.HandleFunc("/flush", func(w http.ResponseWriter, r *http.Request) {
		a.flushBatch()
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "flushed"})
	})

	log.Printf("[NATS Consumer] Health server on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Printf("[NATS Consumer] Health server error: %v", err)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
