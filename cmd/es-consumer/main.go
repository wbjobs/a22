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
	"github.com/segmentio/kafka-go"

	"ebpf-serverless-tracing/internal/config"
	"ebpf-serverless-tracing/internal/model"
)

type App struct {
	cfg         *config.Config
	reader      *kafka.Reader
	es          *elasticsearch.Client
	mu          sync.Mutex
	stats       ConsumerStats
}

type ConsumerStats struct {
	MessagesConsumed int64     `json:"messages_consumed"`
	MessagesIndexed  int64     `json:"messages_indexed"`
	MessagesFailed   int64     `json:"messages_failed"`
	LastIndexTime    time.Time `json:"last_index_time"`
	LastRequestID    string    `json:"last_request_id"`
}

func main() {
	cfg := config.Load()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(cfg.KafkaBrokers, ","),
		Topic:          cfg.KafkaTopic,
		GroupID:        cfg.KafkaGroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		MaxWait:        1 * time.Second,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.LastOffset,
	})
	defer reader.Close()

	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: strings.Split(cfg.ElasticsearchURL, ","),
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: 10 * time.Second,
		},
		RetryOnStatus: []int{502, 503, 504, 429},
		MaxRetries:    5,
		RetryBackoff: func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },
	})
	if err != nil {
		log.Fatalf("Failed to create Elasticsearch client: %v", err)
	}

	app := &App{
		cfg:    cfg,
		reader: reader,
		es:     es,
	}

	go app.ensureESIndex()

	go app.startHealthServer()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go app.consumeLoop(ctx)

	log.Printf("ES Consumer Service started, topic: %s, group: %s, ES: %s",
		cfg.KafkaTopic, cfg.KafkaGroupID, cfg.ElasticsearchURL)

	<-quit
	log.Println("Shutting down ES Consumer Service...")
}

func (a *App) ensureESIndex() {
	for i := 0; i < 30; i++ {
		time.Sleep(2 * time.Second)

		req := esapi.IndicesExistsRequest{
			Index: []string{a.cfg.ESIndex},
		}
		existsResp, err := req.Do(context.Background(), a.es)
		if err != nil {
			log.Printf("[ES] Waiting for Elasticsearch: %v", err)
			continue
		}
		defer existsResp.Body.Close()

		if existsResp.StatusCode == 200 {
			log.Printf("[ES] Index '%s' already exists", a.cfg.ESIndex)
			return
		}

		mapping := `{
			"settings": {
				"number_of_shards": 3,
				"number_of_replicas": 1,
				"refresh_interval": "1s"
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
			log.Printf("[ES] Failed to create index: %v", err)
			continue
		}
		defer createResp.Body.Close()

		if createResp.IsError() {
			log.Printf("[ES] Index creation returned %d", createResp.StatusCode)
			if createResp.StatusCode == 400 {
				return
			}
			continue
		}

		log.Printf("[ES] Index '%s' created successfully", a.cfg.ESIndex)
		return
	}
	log.Printf("[ES] WARNING: Could not create index after retries")
}

func (a *App) consumeLoop(ctx context.Context) {
	batch := make([]model.TraceSpan, 0, 100)
	batchTimer := time.NewTimer(500 * time.Millisecond)

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				a.indexBatch(batch)
			}
			return
		case <-batchTimer.C:
			if len(batch) > 0 {
				a.indexBatch(batch)
				batch = batch[:0]
			}
			batchTimer.Reset(500 * time.Millisecond)
		default:
			readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
			msg, err := a.reader.ReadMessage(readCtx)
			readCancel()

			if err != nil {
				if strings.Contains(err.Error(), "context deadline exceeded") ||
					strings.Contains(err.Error(), "context canceled") {
					continue
				}
				log.Printf("[KafkaConsumer] Read error: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			var span model.TraceSpan
			if err := json.Unmarshal(msg.Value, &span); err != nil {
				log.Printf("[KafkaConsumer] Parse error: %v, value: %s", err, string(msg.Value))
				continue
			}

			batch = append(batch, span)

			if len(batch) >= 100 {
				a.indexBatch(batch)
				batch = batch[:0]
				batchTimer.Reset(500 * time.Millisecond)
			}
		}
	}
}

func (a *App) indexBatch(spans []model.TraceSpan) {
	var buf bytes.Buffer
	var lastReqID string

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

		lastReqID = span.RequestID
	}

	req := esapi.BulkRequest{
		Body:    &buf,
		Refresh: "true",
	}

	resp, err := req.Do(context.Background(), a.es)
	if err != nil {
		log.Printf("[ES] Bulk index error: %v", err)
		a.mu.Lock()
		a.stats.MessagesFailed += int64(len(spans))
		a.mu.Unlock()
		return
	}
	defer resp.Body.Close()

	if resp.IsError() {
		log.Printf("[ES] Bulk index returned %d", resp.StatusCode)
		a.mu.Lock()
		a.stats.MessagesFailed += int64(len(spans))
		a.mu.Unlock()
		return
	}

	var bulkResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&bulkResp)

	errors, _ := bulkResp["errors"].(bool)
	if errors {
		items, _ := bulkResp["items"].([]interface{})
		failed := 0
		for _, item := range items {
			itemMap, _ := item.(map[string]interface{})
			indexData, _ := itemMap["index"].(map[string]interface{})
			if status, ok := indexData["status"].(int); ok {
				if status >= 300 {
					failed++
				}
			}
		}
		a.mu.Lock()
		a.stats.MessagesIndexed += int64(len(spans) - failed)
		a.stats.MessagesFailed += int64(failed)
		a.mu.Unlock()
		log.Printf("[ES] Bulk: %d indexed, %d failed", len(spans)-failed, failed)
	} else {
		a.mu.Lock()
		a.stats.MessagesIndexed += int64(len(spans))
		a.stats.MessagesConsumed += int64(len(spans))
		a.stats.LastIndexTime = time.Now()
		a.stats.LastRequestID = lastReqID
		a.mu.Unlock()
		log.Printf("[ES] Indexed %d spans (last req=%s)", len(spans), lastReqID)
	}
}

func (a *App) startHealthServer() {
	port := getEnv("ES_CONSUMER_PORT", "8086")

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

		a.mu.Lock()
		stats := a.stats
		a.mu.Unlock()

		resp := map[string]interface{}{
			"service":    "es-consumer",
			"status":     "healthy",
			"es_status":  esStatus,
			"es_url":     a.cfg.ElasticsearchURL,
			"kafka_topic": a.cfg.KafkaTopic,
			"stats":      stats,
			"time":       time.Now().UTC(),
		}
		json.NewEncoder(w).Encode(resp)
	})
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		stats := a.stats
		a.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"service": "es-consumer",
			"stats":   stats,
		})
	})

	log.Printf("ES Consumer health server on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Printf("Health server error: %v", err)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
