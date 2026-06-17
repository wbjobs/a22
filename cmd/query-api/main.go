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
