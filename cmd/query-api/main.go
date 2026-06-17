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
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"ebpf-serverless-tracing/internal/config"
	"ebpf-serverless-tracing/internal/model"
)

type App struct {
	cfg *config.Config
	es  *elasticsearch.Client
}

func main() {
	cfg := config.Load()

	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: strings.Split(cfg.ElasticsearchURL, ","),
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: 10 * time.Second,
		},
		RetryOnStatus: []int{502, 503, 504, 429},
		MaxRetries:    3,
		RetryBackoff: func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },
	})
	if err != nil {
		log.Fatalf("Failed to create ES client: %v", err)
	}

	app := &App{
		cfg: cfg,
		es:  es,
	}

	r := gin.Default()

	r.Static("/web", "./web")

	r.Use(func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}
		c.Header("X-Request-ID", requestID)
		start := time.Now()
		c.Next()
		log.Printf("[QueryAPI] request_id=%s method=%s path=%s status=%d duration=%v",
			requestID, c.Request.Method, c.Request.URL.Path, c.Writer.Status(), time.Since(start))
	})

	api := r.Group("/api")
	{
		api.GET("/trace/:request_id", app.getTraceByRequestID)
		api.GET("/trace/trace/:trace_id", app.getTraceByTraceID)
		api.GET("/spans/:request_id", app.getSpansByRequestID)
		api.GET("/search", app.searchTraces)
		api.GET("/services", app.listServices)
		api.GET("/functions", app.listFunctions)
		api.GET("/stats", app.getStats)
		api.GET("/waterfall/:request_id", app.getWaterfallData)
		api.GET("/topology", app.getServiceTopology)
		api.GET("/health/overview", app.getHealthOverview)
		api.GET("/health/:service", app.getServiceHealth)
	}

	r.GET("/health", app.healthCheck)

	port := cfg.QueryAPIPort
	srv := &http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

	go func() {
		log.Printf("Query API Service starting on port %s, ES: %s", port, cfg.ElasticsearchURL)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Query API Service...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown failed: %v", err)
	}
	log.Println("Query API Service stopped")
}

func (a *App) healthCheck(c *gin.Context) {
	esHealth := "unknown"
	if health, err := a.checkESHealth(); err == nil {
		esHealth = health
	}

	c.JSON(http.StatusOK, gin.H{
		"service":   "query-api",
		"status":    "healthy",
		"es_status": esHealth,
		"es_url":    a.cfg.ElasticsearchURL,
		"es_index":  a.cfg.ESIndex,
		"time":      time.Now().UTC(),
	})
}

func (a *App) checkESHealth() (string, error) {
	req := esapi.ClusterHealthRequest{
		Timeout: 3 * time.Second,
	}
	resp, err := req.Do(context.Background(), a.es)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var health map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&health)
	if status, ok := health["status"].(string); ok {
		return status, nil
	}
	return "unknown", nil
}

func (a *App) getTraceByRequestID(c *gin.Context) {
	requestID := c.Param("request_id")
	if requestID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request_id is required"})
		return
	}

	spans, err := a.querySpansByRequestID(requestID)
	if err != nil {
		log.Printf("[QueryAPI] ES query error for req=%s: %v", requestID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query traces: " + err.Error()})
		return
	}

	if len(spans) == 0 {
		c.JSON(http.StatusNotFound, gin.H{
			"error":      "No trace data found",
			"request_id": requestID,
			"message":    "Trace data may still be processing or request_id is invalid",
		})
		return
	}

	result := a.buildTraceResult(spans, requestID)
	c.JSON(http.StatusOK, result)
}

func (a *App) getTraceByTraceID(c *gin.Context) {
	traceID := c.Param("trace_id")
	if traceID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "trace_id is required"})
		return
	}

	spans, err := a.querySpansByTraceID(traceID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to query: " + err.Error()})
		return
	}

	if len(spans) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "No trace data found", "trace_id": traceID})
		return
	}

	requestID := spans[0].RequestID
	result := a.buildTraceResult(spans, requestID)
	result.TraceID = traceID
	c.JSON(http.StatusOK, result)
}

func (a *App) getSpansByRequestID(c *gin.Context) {
	requestID := c.Param("request_id")
	spans, err := a.querySpansByRequestID(requestID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"request_id": requestID,
		"count":      len(spans),
		"spans":      spans,
	})
}

func (a *App) searchTraces(c *gin.Context) {
	functionName := c.Query("function")
	serviceName := c.Query("service")
	statusCode := c.Query("status")
	minDuration := c.Query("min_duration")
	fromTime := c.Query("from")
	toTime := c.Query("to")
	limitStr := c.DefaultQuery("limit", "50")

	limit := 50
	fmt.Sscanf(limitStr, "%d", &limit)
	if limit > 500 {
		limit = 500
	}

	var mustClauses []map[string]interface{}

	if functionName != "" {
		mustClauses = append(mustClauses, map[string]interface{}{
			"term": map[string]interface{}{"function_name": functionName},
		})
	}
	if serviceName != "" {
		mustClauses = append(mustClauses, map[string]interface{}{
			"term": map[string]interface{}{"service_name": serviceName},
		})
	}
	if statusCode != "" {
		mustClauses = append(mustClauses, map[string]interface{}{
			"term": map[string]interface{}{"status_code": statusCode},
		})
	}
	if minDuration != "" {
		var md int64
		fmt.Sscanf(minDuration, "%d", &md)
		mustClauses = append(mustClauses, map[string]interface{}{
			"range": map[string]interface{}{
				"duration_ms": map[string]interface{}{"gte": md},
			},
		})
	}

	var filterClauses []map[string]interface{}
	if fromTime != "" || toTime != "" {
		timeRange := map[string]interface{}{}
		if fromTime != "" {
			timeRange["gte"] = fromTime
		}
		if toTime != "" {
			timeRange["lte"] = toTime
		}
		filterClauses = append(filterClauses, map[string]interface{}{
			"range": map[string]interface{}{"timestamp": timeRange},
		})
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{},
		},
		"size": limit,
		"sort": []map[string]interface{}{
			{"timestamp": map[string]interface{}{"order": "desc"}},
		},
	}

	boolQ := query["query"].(map[string]interface{})["bool"].(map[string]interface{})
	if len(mustClauses) > 0 {
		boolQ["must"] = mustClauses
	}
	if len(filterClauses) > 0 {
		boolQ["filter"] = filterClauses
	}

	body, _ := json.Marshal(query)
	req := esapi.SearchRequest{
		Index: []string{a.cfg.ESIndex},
		Body:  bytes.NewReader(body),
	}

	resp, err := req.Do(context.Background(), a.es)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	if resp.IsError() {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "ES query failed"})
		return
	}

	var esResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&esResp)

	hits, _ := esResp["hits"].(map[string]interface{})
	total, _ := hits["total"].(map[string]interface{})
	hitList, _ := hits["hits"].([]interface{})

	spans := make([]model.TraceSpan, 0, len(hitList))
	requestIDSet := map[string]bool{}

	for _, h := range hitList {
		hit, _ := h.(map[string]interface{})
		source, _ := hit["_source"].(map[string]interface{})
		span, _ := a.parseSpanFromSource(source)
		spans = append(spans, span)
		requestIDSet[span.RequestID] = true
	}

	requestIDs := make([]string, 0, len(requestIDSet))
	for id := range requestIDSet {
		requestIDs = append(requestIDs, id)
	}

	c.JSON(http.StatusOK, gin.H{
		"total":       total,
		"count":       len(spans),
		"spans":       spans,
		"request_ids": requestIDs,
	})
}

func (a *App) listServices(c *gin.Context) {
	result, err := a.doTermsAgg("service_name")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

func (a *App) listFunctions(c *gin.Context) {
	result, err := a.doTermsAgg("function_name")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

func (a *App) doTermsAgg(field string) (map[string]interface{}, error) {
	query := map[string]interface{}{
		"size": 0,
		"aggs": map[string]interface{}{
			field + "_count": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": field,
					"size":  100,
				},
			},
		},
	}
	body, _ := json.Marshal(query)
	req := esapi.SearchRequest{
		Index: []string{a.cfg.ESIndex},
		Body:  bytes.NewReader(body),
	}
	resp, err := req.Do(context.Background(), a.es)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var esResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&esResp)

	aggs, _ := esResp["aggregations"].(map[string]interface{})
	fieldAgg, _ := aggs[field+"_count"].(map[string]interface{})
	buckets, _ := fieldAgg["buckets"].([]interface{})

	items := make([]map[string]interface{}, 0, len(buckets))
	for _, b := range buckets {
		bucket, _ := b.(map[string]interface{})
		items = append(items, map[string]interface{}{
			"name":  bucket["key"],
			"count": bucket["doc_count"],
		})
	}

	return map[string]interface{}{
		"field": field,
		"items": items,
	}, nil
}

func (a *App) getStats(c *gin.Context) {
	query := map[string]interface{}{
		"size": 0,
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				"timestamp": map[string]interface{}{
					"gte": "now-1h",
				},
			},
		},
		"aggs": map[string]interface{}{
			"total_spans": map[string]interface{}{"value_count": map[string]interface{}{"field": "span_id"}},
			"avg_duration": map[string]interface{}{"avg": map[string]interface{}{"field": "duration_ms"}},
			"max_duration": map[string]interface{}{"max": map[string]interface{}{"field": "duration_ms"}},
			"unique_requests": map[string]interface{}{
				"cardinality": map[string]interface{}{"field": "request_id"},
			},
			"error_count": map[string]interface{}{
				"filter": map[string]interface{}{
					"range": map[string]interface{}{
						"status_code": map[string]interface{}{"gte": 500},
					},
				},
			},
			"per_service": map[string]interface{}{
				"terms": map[string]interface{}{"field": "service_name", "size": 20},
				"aggs": map[string]interface{}{
					"avg_dur": map[string]interface{}{"avg": map[string]interface{}{"field": "duration_ms"}},
				},
			},
		},
	}

	body, _ := json.Marshal(query)
	req := esapi.SearchRequest{
		Index: []string{a.cfg.ESIndex},
		Body:  bytes.NewReader(body),
	}
	resp, err := req.Do(context.Background(), a.es)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	var esResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&esResp)

	aggs, _ := esResp["aggregations"].(map[string]interface{})

	getAggValue := func(name string) float64 {
		a, _ := aggs[name].(map[string]interface{})
		v, _ := a["value"].(float64)
		return v
	}

	perService, _ := aggs["per_service"].(map[string]interface{})
	buckets, _ := perService["buckets"].([]interface{})
	services := make([]map[string]interface{}, 0, len(buckets))
	for _, b := range buckets {
		bucket, _ := b.(map[string]interface{})
		avgDur, _ := bucket["avg_dur"].(map[string]interface{})
		services = append(services, map[string]interface{}{
			"service":        bucket["key"],
			"span_count":     bucket["doc_count"],
			"avg_duration_ms": avgDur["value"],
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"time_range_hours": 1,
		"total_spans":      getAggValue("total_spans"),
		"unique_requests":  getAggValue("unique_requests"),
		"avg_duration_ms":  getAggValue("avg_duration"),
		"max_duration_ms":  getAggValue("max_duration"),
		"error_count_5xx":  getAggValue("error_count"),
		"per_service":      services,
	})
}

func (a *App) getWaterfallData(c *gin.Context) {
	requestID := c.Param("request_id")
	spans, err := a.querySpansByRequestID(requestID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if len(spans) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "No data"})
		return
	}

	sort.Slice(spans, func(i, j int) bool {
		return spans[i].StartTime.Before(spans[j].StartTime)
	})

	baseTime := spans[0].StartTime
	items := make([]map[string]interface{}, 0, len(spans))

	for _, s := range spans {
		items = append(items, map[string]interface{}{
			"id":           s.SpanID,
			"name":         s.FunctionName,
			"service":      s.ServiceName,
			"start_offset": s.StartTime.Sub(baseTime).Milliseconds(),
			"duration_ms":  s.DurationMs,
			"status_code":  s.StatusCode,
			"status":       getStatusLabel(s.StatusCode),
			"method":       s.Method,
			"path":         s.Path,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"request_id":  requestID,
		"trace_id":    spans[0].TraceID,
		"start_time":  baseTime,
		"total_spans": len(spans),
		"waterfall":   items,
	})
}

func (a *App) querySpansByRequestID(requestID string) ([]model.TraceSpan, error) {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{"request_id": requestID},
		},
		"size": 100,
		"sort": []map[string]interface{}{
			{"start_time": map[string]interface{}{"order": "asc"}},
		},
	}
	return a.executeSpanQuery(query)
}

func (a *App) querySpansByTraceID(traceID string) ([]model.TraceSpan, error) {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{"trace_id": traceID},
		},
		"size": 100,
		"sort": []map[string]interface{}{
			{"start_time": map[string]interface{}{"order": "asc"}},
		},
	}
	return a.executeSpanQuery(query)
}

func (a *App) executeSpanQuery(query map[string]interface{}) ([]model.TraceSpan, error) {
	body, _ := json.Marshal(query)
	req := esapi.SearchRequest{
		Index: []string{a.cfg.ESIndex},
		Body:  bytes.NewReader(body),
	}
	resp, err := req.Do(context.Background(), a.es)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return nil, fmt.Errorf("ES returned status %d", resp.StatusCode)
	}

	var esResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&esResp)

	hits, _ := esResp["hits"].(map[string]interface{})
	hitList, _ := hits["hits"].([]interface{})

	spans := make([]model.TraceSpan, 0, len(hitList))
	for _, h := range hitList {
		hit, _ := h.(map[string]interface{})
		source, _ := hit["_source"].(map[string]interface{})
		span, err := a.parseSpanFromSource(source)
		if err == nil {
			spans = append(spans, span)
		}
	}

	return spans, nil
}

func (a *App) parseSpanFromSource(source map[string]interface{}) (model.TraceSpan, error) {
	var span model.TraceSpan

	getStr := func(key string) string {
		v, _ := source[key].(string)
		return v
	}
	getInt := func(key string) int {
		switch v := source[key].(type) {
		case float64:
			return int(v)
		case int:
			return v
		}
		return 0
	}
	getInt64 := func(key string) int64 {
		switch v := source[key].(type) {
		case float64:
			return int64(v)
		case int64:
			return v
		}
		return 0
	}
	getTime := func(key string) time.Time {
		v, _ := source[key].(string)
		if v == "" {
			return time.Time{}
		}
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			t, _ = time.Parse(time.RFC3339, v)
		}
		return t
	}

	span.RequestID = getStr("request_id")
	span.TraceID = getStr("trace_id")
	span.SpanID = getStr("span_id")
	span.ParentSpanID = getStr("parent_span_id")
	span.FunctionName = getStr("function_name")
	span.ServiceName = getStr("service_name")
	span.Method = getStr("method")
	span.Path = getStr("path")
	span.Protocol = getStr("protocol")
	span.SourceIP = getStr("source_ip")
	span.DestIP = getStr("dest_ip")
	span.StatusCode = getInt("status_code")
	span.SourcePort = uint16(getInt("source_port"))
	span.DestPort = uint16(getInt("dest_port"))
	span.DurationMs = getInt64("duration_ms")
	span.StartTime = getTime("start_time")
	span.EndTime = getTime("end_time")
	span.Timestamp = getTime("timestamp")

	return span, nil
}

func (a *App) buildTraceResult(spans []model.TraceSpan, requestID string) *model.TraceResult {
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].StartTime.Before(spans[j].StartTime)
	})

	traceID := ""
	if len(spans) > 0 {
		traceID = spans[0].TraceID
	}

	var startTime, endTime time.Time
	for i, s := range spans {
		if i == 0 || s.StartTime.Before(startTime) {
			startTime = s.StartTime
		}
		if s.EndTime.After(endTime) {
			endTime = s.EndTime
		}
	}

	totalDuration := int64(0)
	if !startTime.IsZero() && !endTime.IsZero() {
		totalDuration = endTime.Sub(startTime).Milliseconds()
	}

	callChain := a.buildCallTree(spans)

	return &model.TraceResult{
		RequestID:       requestID,
		TraceID:         traceID,
		TotalSpans:      len(spans),
		StartTime:       startTime,
		EndTime:         endTime,
		TotalDurationMs: totalDuration,
		Spans:           spans,
		CallChain:       callChain,
	}
}

func (a *App) buildCallTree(spans []model.TraceSpan) []model.CallNode {
	spanMap := map[string]*model.CallNode{}
	var roots []model.CallNode
	var allNodes []model.CallNode

	for _, s := range spans {
		node := model.CallNode{
			SpanID:       s.SpanID,
			FunctionName: s.FunctionName,
			ServiceName:  s.ServiceName,
			StatusCode:   s.StatusCode,
			DurationMs:   s.DurationMs,
			StartTime:    s.StartTime,
			Children:     []model.CallNode{},
		}
		spanMap[s.SpanID] = &node
		allNodes = append(allNodes, node)
	}

	for _, s := range spans {
		node, exists := spanMap[s.SpanID]
		if !exists {
			continue
		}

		if s.ParentSpanID != "" {
			if parent, ok := spanMap[s.ParentSpanID]; ok {
				parent.Children = append(parent.Children, *node)
			} else {
				roots = append(roots, *node)
			}
		} else {
			roots = append(roots, *node)
		}
	}

	if len(roots) == 0 && len(allNodes) > 0 {
		roots = allNodes
	}

	sortCallNodes(roots)
	return roots
}

func sortCallNodes(nodes []model.CallNode) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].StartTime.Before(nodes[j].StartTime)
	})
	for i := range nodes {
		if len(nodes[i].Children) > 0 {
			sortCallNodes(nodes[i].Children)
		}
	}
}

func getStatusLabel(code int) string {
	switch {
	case code == 0:
		return "pending"
	case code >= 200 && code < 300:
		return "success"
	case code >= 300 && code < 400:
		return "redirect"
	case code >= 400 && code < 500:
		return "client_error"
	case code >= 500:
		return "server_error"
	default:
		return "unknown"
	}
}

type ServiceHealth struct {
	Service        string  `json:"service"`
	Status         string  `json:"status"`
	HealthScore    float64 `json:"health_score"`
	RequestCount   int64   `json:"request_count"`
	ErrorCount     int64   `json:"error_count"`
	ErrorRate      float64 `json:"error_rate"`
	AvgLatencyMs   float64 `json:"avg_latency_ms"`
	P95LatencyMs   float64 `json:"p95_latency_ms"`
	TimeoutCount   int64   `json:"timeout_count"`
	TimeoutRate    float64 `json:"timeout_rate"`
	LastUpdated    string  `json:"last_updated"`
	ChaosInjected  bool    `json:"chaos_injected"`
	ChaosType      string  `json:"chaos_type,omitempty"`
}

func calcHealthScore(errorRate, timeoutRate, avgLatencyMs float64) (float64, string) {
	score := 100.0
	score -= errorRate * 500
	score -= timeoutRate * 300
	if avgLatencyMs > 1000 {
		score -= 20
	} else if avgLatencyMs > 500 {
		score -= 10
	}
	if score < 0 {
		score = 0
	}
	status := "healthy"
	if score < 50 {
		status = "critical"
	} else if score < 75 {
		status = "warning"
	} else if score < 90 {
		status = "degraded"
	}
	return score, status
}

func (a *App) getServiceTopology(c *gin.Context) {
	hoursStr := c.DefaultQuery("hours", "1")
	var hours int
	fmt.Sscanf(hoursStr, "%d", &hours)
	if hours <= 0 {
		hours = 1
	}

	query := map[string]interface{}{
		"size": 5000,
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				"timestamp": map[string]interface{}{
					"gte": fmt.Sprintf("now-%dh", hours),
				},
			},
		},
		"sort": []map[string]interface{}{
			{"timestamp": map[string]interface{}{"order": "desc"}},
		},
	}

	spans, err := a.executeSpanQuery(query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	nodes := make(map[string]map[string]interface{})
	edges := make(map[string]map[string]interface{})
	parentChildMap := make(map[string]map[string]bool)

	for _, s := range spans {
		svc := s.ServiceName
		if svc == "" {
			svc = s.FunctionName
		}
		if svc == "" {
			continue
		}
		if _, exists := nodes[svc]; !exists {
			nodes[svc] = map[string]interface{}{
				"id": svc,
				"name": svc,
				"request_count": int64(0),
				"error_count": int64(0),
				"total_latency": int64(0),
				"avg_latency": float64(0),
			}
		}
		n := nodes[svc]
		n["request_count"] = n["request_count"].(int64) + 1
		if s.StatusCode >= 500 {
			n["error_count"] = n["error_count"].(int64) + 1
		}
		n["total_latency"] = n["total_latency"].(int64) + s.DurationMs

		if s.ParentSpanID != "" && s.SpanID != "" {
			parentChildMap[s.ParentSpanID] = map[string]bool{s.SpanID: true}
		}
	}

	for _, n := range nodes {
		reqCount := n["request_count"].(int64)
		if reqCount > 0 {
			n["avg_latency"] = float64(n["total_latency"].(int64)) / float64(reqCount)
		}
		errCount := n["error_count"].(int64)
		errRate := float64(0)
		if reqCount > 0 {
			errRate = float64(errCount) / float64(reqCount)
		}
		score, status := calcHealthScore(errRate, 0, n["avg_latency"].(float64))
		n["error_rate"] = errRate
		n["health_score"] = score
		n["status"] = status
		delete(n, "total_latency")
	}

	spanServiceMap := make(map[string]string)
	for _, s := range spans {
		svc := s.ServiceName
		if svc == "" {
			svc = s.FunctionName
		}
		spanServiceMap[s.SpanID] = svc
	}

	for parentSpanID, children := range parentChildMap {
		parentSvc := spanServiceMap[parentSpanID]
		if parentSvc == "" {
			continue
		}
		for childSpanID := range children {
			childSvc := spanServiceMap[childSpanID]
			if childSvc == "" || parentSvc == childSvc {
				continue
			}
			edgeKey := parentSvc + "->" + childSvc
			if _, exists := edges[edgeKey]; !exists {
				edges[edgeKey] = map[string]interface{}{
					"id": edgeKey,
					"source": parentSvc,
					"target": childSvc,
					"call_count": int64(0),
				}
			}
			edges[edgeKey]["call_count"] = edges[edgeKey]["call_count"].(int64) + 1
		}
	}

	nodeList := make([]map[string]interface{}, 0, len(nodes))
	for _, n := range nodes {
		nodeList = append(nodeList, n)
	}
	edgeList := make([]map[string]interface{}, 0, len(edges))
	for _, e := range edges {
		edgeList = append(edgeList, e)
	}

	c.JSON(http.StatusOK, gin.H{
		"window_hours": hours,
		"nodes": nodeList,
		"edges": edgeList,
		"generated_at": time.Now().UTC().Format(time.RFC3339),
	})
}

func (a *App) getHealthOverview(c *gin.Context) {
	hoursStr := c.DefaultQuery("hours", "1")
	var hours int
	fmt.Sscanf(hoursStr, "%d", &hours)
	if hours <= 0 {
		hours = 1
	}

	query := map[string]interface{}{
		"size": 0,
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				"timestamp": map[string]interface{}{
					"gte": fmt.Sprintf("now-%dh", hours),
				},
			},
		},
		"aggs": map[string]interface{}{
			"per_service": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "service_name",
					"size": 50,
				},
				"aggs": map[string]interface{}{
					"avg_dur":  map[string]interface{}{"avg": map[string]interface{}{"field": "duration_ms"}},
					"p95_dur":  map[string]interface{}{"percentiles": map[string]interface{}{"field": "duration_ms", "percents": []int{95}}},
					"err_5xx":  map[string]interface{}{"filter": map[string]interface{}{"range": map[string]interface{}{"status_code": map[string]interface{}{"gte": 500}}}},
					"timeouts": map[string]interface{}{"filter": map[string]interface{}{"range": map[string]interface{}{"duration_ms": map[string]interface{}{"gte": 1000}}}},
				},
			},
		},
	}

	body, _ := json.Marshal(query)
	req := esapi.SearchRequest{
		Index: []string{a.cfg.ESIndex},
		Body:  bytes.NewReader(body),
	}
	resp, err := req.Do(context.Background(), a.es)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	var esResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&esResp)

	aggs, _ := esResp["aggregations"].(map[string]interface{})
	perSvc, _ := aggs["per_service"].(map[string]interface{})
	buckets, _ := perSvc["buckets"].([]interface{})

	services := make([]ServiceHealth, 0, len(buckets))
	totalReq := int64(0)
	totalErr := int64(0)
	criticalCount := 0

	for _, b := range buckets {
		bucket, _ := b.(map[string]interface{})
		svcName, _ := bucket["key"].(string)
		reqCount, _ := bucket["doc_count"].(int64)
		avgDurAgg, _ := bucket["avg_dur"].(map[string]interface{})
		avgDur, _ := avgDurAgg["value"].(float64)
		p95Agg, _ := bucket["p95_dur"].(map[string]interface{})
		p95Values, _ := p95Agg["values"].(map[string]interface{})
		var p95Dur float64
		for _, v := range p95Values {
			p95Dur, _ = v.(float64)
			break
		}
		errAgg, _ := bucket["err_5xx"].(map[string]interface{})
		errCount, _ := errAgg["doc_count"].(int64)
		timeoutAgg, _ := bucket["timeouts"].(map[string]interface{})
		timeoutCount, _ := timeoutAgg["doc_count"].(int64)

		errRate := float64(0)
		if reqCount > 0 {
			errRate = float64(errCount) / float64(reqCount)
		}
		timeoutRate := float64(0)
		if reqCount > 0 {
			timeoutRate = float64(timeoutCount) / float64(reqCount)
		}
		score, status := calcHealthScore(errRate, timeoutRate, avgDur)
		if status == "critical" {
			criticalCount++
		}

		services = append(services, ServiceHealth{
			Service:      svcName,
			Status:       status,
			HealthScore:  score,
			RequestCount: reqCount,
			ErrorCount:   errCount,
			ErrorRate:    errRate,
			AvgLatencyMs: avgDur,
			P95LatencyMs: p95Dur,
			TimeoutCount: timeoutCount,
			TimeoutRate:  timeoutRate,
			LastUpdated:  time.Now().UTC().Format(time.RFC3339),
		})

		totalReq += reqCount
		totalErr += errCount
	}

	overallScore := float64(100)
	overallStatus := "healthy"
	if len(services) > 0 {
		sumScore := float64(0)
		for _, s := range services {
			sumScore += s.HealthScore
		}
		overallScore = sumScore / float64(len(services))
		if overallScore < 50 {
			overallStatus = "critical"
		} else if overallScore < 75 {
			overallStatus = "warning"
		} else if overallScore < 90 {
			overallStatus = "degraded"
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"window_hours":    hours,
		"overall_status":  overallStatus,
		"overall_score":   overallScore,
		"total_requests":  totalReq,
		"total_errors":    totalErr,
		"critical_count":  criticalCount,
		"service_count":   len(services),
		"services":        services,
		"generated_at":    time.Now().UTC().Format(time.RFC3339),
	})
}

func (a *App) getServiceHealth(c *gin.Context) {
	service := c.Param("service")
	if service == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "service is required"})
		return
	}
	hoursStr := c.DefaultQuery("hours", "1")
	var hours int
	fmt.Sscanf(hoursStr, "%d", &hours)

	query := map[string]interface{}{
		"size": 0,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{"term": map[string]interface{}{"service_name": service}},
					{"range": map[string]interface{}{
						"timestamp": map[string]interface{}{"gte": fmt.Sprintf("now-%dh", hours)},
					}},
				},
			},
		},
		"aggs": map[string]interface{}{
			"avg_dur":    map[string]interface{}{"avg": map[string]interface{}{"field": "duration_ms"}},
			"max_dur":    map[string]interface{}{"max": map[string]interface{}{"field": "duration_ms"}},
			"p50_dur":    map[string]interface{}{"percentiles": map[string]interface{}{"field": "duration_ms", "percents": []int{50, 95, 99}}},
			"err_4xx":    map[string]interface{}{"filter": map[string]interface{}{"range": map[string]interface{}{"status_code": map[string]interface{}{"gte": 400, "lt": 500}}}},
			"err_5xx":    map[string]interface{}{"filter": map[string]interface{}{"range": map[string]interface{}{"status_code": map[string]interface{}{"gte": 500}}}},
			"timeouts":   map[string]interface{}{"filter": map[string]interface{}{"range": map[string]interface{}{"duration_ms": map[string]interface{}{"gte": 1000}}}},
			"per_minute": map[string]interface{}{
				"date_histogram": map[string]interface{}{
					"field":          "timestamp",
					"fixed_interval": "1m",
				},
				"aggs": map[string]interface{}{
					"avg_dur": map[string]interface{}{"avg": map[string]interface{}{"field": "duration_ms"}},
					"err_count": map[string]interface{}{
						"filter": map[string]interface{}{"range": map[string]interface{}{"status_code": map[string]interface{}{"gte": 500}}},
					},
				},
			},
			"top_errors": map[string]interface{}{
				"top_hits": map[string]interface{}{
					"size": 10,
					"sort": []map[string]interface{}{
						{"timestamp": map[string]interface{}{"order": "desc"}},
					},
					"query": map[string]interface{}{
						"range": map[string]interface{}{"status_code": map[string]interface{}{"gte": 500}},
					},
				},
			},
		},
	}

	body, _ := json.Marshal(query)
	req := esapi.SearchRequest{
		Index: []string{a.cfg.ESIndex},
		Body:  bytes.NewReader(body),
	}
	resp, err := req.Do(context.Background(), a.es)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	var esResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&esResp)

	hits, _ := esResp["hits"].(map[string]interface{})
	totalVal, _ := hits["total"].(map[string]interface{})
	reqCount := int64(0)
	switch v := totalVal["value"].(type) {
	case float64:
		reqCount = int64(v)
	case int64:
		reqCount = v
	}

	if reqCount == 0 {
		c.JSON(http.StatusNotFound, gin.H{
			"service": service,
			"error":   "no data found for service",
			"status":  "unknown",
		})
		return
	}

	aggs, _ := esResp["aggregations"].(map[string]interface{})
	avgDurAgg, _ := aggs["avg_dur"].(map[string]interface{})
	avgDur, _ := avgDurAgg["value"].(float64)
	maxDurAgg, _ := aggs["max_dur"].(map[string]interface{})
	maxDur, _ := maxDurAgg["value"].(float64)
	p50Agg, _ := aggs["p50_dur"].(map[string]interface{})
	pValues, _ := p50Agg["values"].(map[string]interface{})
	p50, p95, p99 := float64(0), float64(0), float64(0)
	idx := 0
	for _, v := range pValues {
		switch idx {
		case 0:
			p50, _ = v.(float64)
		case 1:
			p95, _ = v.(float64)
		case 2:
			p99, _ = v.(float64)
		}
		idx++
	}
	err4xxAgg, _ := aggs["err_4xx"].(map[string]interface{})
	err4xx, _ := err4xxAgg["doc_count"].(int64)
	err5xxAgg, _ := aggs["err_5xx"].(map[string]interface{})
	err5xx, _ := err5xxAgg["doc_count"].(int64)
	timeoutAgg, _ := aggs["timeouts"].(map[string]interface{})
	timeoutCount, _ := timeoutAgg["doc_count"].(int64)

	totalErr := err4xx + err5xx
	errRate := float64(totalErr) / float64(reqCount)
	timeoutRate := float64(timeoutCount) / float64(reqCount)
	score, status := calcHealthScore(errRate, timeoutRate, avgDur)

	perMinAgg, _ := aggs["per_minute"].(map[string]interface{})
	perMinBuckets, _ := perMinAgg["buckets"].([]interface{})
	timeSeries := make([]map[string]interface{}, 0, len(perMinBuckets))
	for _, b := range perMinBuckets {
		bucket, _ := b.(map[string]interface{})
		ts, _ := bucket["key_as_string"].(string)
		count, _ := bucket["doc_count"].(int64)
		avgB, _ := bucket["avg_dur"].(map[string]interface{})
		avgV, _ := avgB["value"].(float64)
		errB, _ := bucket["err_count"].(map[string]interface{})
		errV, _ := errB["doc_count"].(int64)
		timeSeries = append(timeSeries, map[string]interface{}{
			"timestamp":      ts,
			"request_count":  count,
			"avg_latency_ms": avgV,
			"error_count":    errV,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"service":        service,
		"status":         status,
		"health_score":   score,
		"window_hours":   hours,
		"request_count":  reqCount,
		"error_count_4xx": err4xx,
		"error_count_5xx": err5xx,
		"total_errors":   totalErr,
		"error_rate":     errRate,
		"timeout_count":  timeoutCount,
		"timeout_rate":   timeoutRate,
		"latency_ms": map[string]interface{}{
			"avg": avgDur,
			"p50": p50,
			"p95": p95,
			"p99": p99,
			"max": maxDur,
		},
		"time_series":    timeSeries,
		"last_updated":   time.Now().UTC().Format(time.RFC3339),
	})
}
