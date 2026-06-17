package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"ebpf-serverless-tracing/internal/config"
	"ebpf-serverless-tracing/internal/model"
	"ebpf-serverless-tracing/internal/natsutil"
	"ebpf-serverless-tracing/internal/sampling"
	traceprod "ebpf-serverless-tracing/internal/traceproducer"
	wasmmgr "ebpf-serverless-tracing/internal/wasm"
)

type responseBodyWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (r responseBodyWriter) Write(b []byte) (int, error) {
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}

type TracingMiddleware struct {
	serviceName string
	natsURL     string
	producerURL string
	httpClient  *http.Client
	mu          sync.Mutex
	pending     map[string]*pendingSpan
	sampler     *sampling.DynamicSampler
	wasmMgr     *wasmmgr.WasmPluginManager
	nats        *traceprod.NATSTraceProducer
	useNATS     bool
	stats       MiddlewareStats
}

type pendingSpan struct {
	StartTime time.Time
	Span      model.TraceSpan
}

type MiddlewareStats struct {
	Total         int64
	Sampled       int64
	WasmFiltered  int64
	Published     int64
	Errors        int64
	LastRequestID string
	LastTime      time.Time
}

func NewTracingMiddleware(serviceName string) *TracingMiddleware {
	m := &TracingMiddleware{
		serviceName: serviceName,
		producerURL: getEnvOrDefault("TRACE_PRODUCER_URL", "http://nats-producer:8085/v1/spans"),
		natsURL:     getEnvOrDefault("NATS_URL", "nats://nats:4222"),
		useNATS:     getEnvOrDefault("TRACE_USE_NATS", "true") == "true",
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 16,
			},
		},
		pending: make(map[string]*pendingSpan),
	}

	samplingCfg := sampling.SamplingConfig{
		Strategy:           sampling.StrategyDynamic,
		BaseSampleRate:     0.01,
		MinSampleRate:      0.01,
		MaxSampleRate:      1.0,
		ErrorRateThreshold: 0.05,
		SlidingWindow:      1 * time.Minute,
		AdjustInterval:     5 * time.Second,
		HighErrorBoost:     10.0,
		AlwaysSampleError:  true,
	}
	m.sampler = sampling.NewDynamicSampler(&samplingCfg)

	wasmDir := os.Getenv("WASM_PLUGIN_DIR")
	if wasmDir == "" {
		wasmDir = "./plugins/wasm"
	}
	var wasmErr error
	m.wasmMgr, wasmErr = wasmmgr.NewWasmPluginManager(wasmDir)
	if wasmErr != nil {
		log.Printf("[Tracing:%s] Warning: WASM plugin manager unavailable: %v", serviceName, wasmErr)
	}

	if m.useNATS {
		natsCfg := &natsutil.NATSConfig{
			URLs:          strings.Split(m.natsURL, ","),
			StreamName:    getEnvOrDefault("NATS_STREAM", "TRACES"),
			SubjectPrefix: "trace.spans",
		}
		var natsErr error
		m.nats, natsErr = traceprod.NewNATSProducer(natsCfg, m.sampler, m.wasmMgr)
		if natsErr != nil {
			log.Printf("[Tracing:%s] Warning: NATS producer unavailable (fallback to HTTP): %v", serviceName, natsErr)
			m.useNATS = false
		} else {
			log.Printf("[Tracing:%s] Initialized with NATS direct publishing + dynamic sampling", serviceName)
		}
	}

	go m.cleanupLoop()

	return m
}

func (m *TracingMiddleware) Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		traceID := c.GetHeader("X-Trace-ID")
		parentSpanID := c.GetHeader("X-Parent-Span-ID")
		spanID := c.GetHeader("X-Span-ID")

		if requestID == "" {
			requestID = uuid.New().String()
		}
		if traceID == "" {
			traceID = uuid.New().String()
		}
		if spanID == "" {
			spanID = uuid.New().String()
		}

		c.Set("request_id", requestID)
		c.Set("trace_id", traceID)
		c.Set("span_id", spanID)
		c.Set("parent_span_id", parentSpanID)

		c.Writer.Header().Set("X-Request-ID", requestID)
		c.Writer.Header().Set("X-Trace-ID", traceID)
		c.Writer.Header().Set("X-Span-ID", spanID)

		c.Request.Header.Set("X-Request-ID", requestID)
		c.Request.Header.Set("X-Trace-ID", traceID)
		c.Request.Header.Set("X-Span-ID", spanID)
		if parentSpanID != "" {
			c.Request.Header.Set("X-Parent-Span-ID", parentSpanID)
		}
		c.Request.Header.Set("X-Service-Name", m.serviceName)
		c.Request.Header.Set("X-Function-Name", m.serviceName)

		startTime := time.Now()

		pendingKey := requestID + ":" + spanID
		span := model.TraceSpan{
			RequestID:    requestID,
			TraceID:      traceID,
			SpanID:       spanID,
			ParentSpanID: parentSpanID,
			ServiceName:  m.serviceName,
			FunctionName: m.serviceName,
			Method:       c.Request.Method,
			Path:         c.Request.URL.Path,
			StartTime:    startTime,
			Protocol:     "HTTP",
			SourceIP:     getRemoteIP(c),
		}
		m.mu.Lock()
		m.pending[pendingKey] = &pendingSpan{StartTime: startTime, Span: span}
		m.stats.Total++
		m.stats.LastRequestID = requestID
		m.stats.LastTime = time.Now()
		m.mu.Unlock()

		w := &responseBodyWriter{body: &bytes.Buffer{}, ResponseWriter: c.Writer}
		c.Writer = w

		c.Next()

		duration := time.Since(startTime)
		statusCode := c.Writer.Status()

		m.mu.Lock()
		pending, exists := m.pending[pendingKey]
		if exists {
			delete(m.pending, pendingKey)
		}
		m.mu.Unlock()

		if !exists {
			return
		}

		pending.Span.StatusCode = statusCode
		pending.Span.EndTime = startTime.Add(duration)
		pending.Span.DurationMs = duration.Milliseconds()
		pending.Span.Timestamp = startTime.Add(duration)

		go m.processAndPublish(&pending.Span)

		log.Printf("[Tracing:%s] request_id=%s span_id=%s method=%s path=%s status=%d duration=%dms",
			m.serviceName, requestID, spanID, c.Request.Method, c.Request.URL.Path, statusCode, duration.Milliseconds())
	}
}

func (m *TracingMiddleware) processAndPublish(span *model.TraceSpan) {
	if span == nil {
		return
	}

	if m.sampler != nil && !m.sampler.ShouldSample(span) {
		m.mu.Lock()
		m.stats.Sampled++
		m.mu.Unlock()
		return
	}

	if m.wasmMgr != nil && m.wasmMgr.IsEnabled() {
		result, errs := m.wasmMgr.RunFilters(span)
		if len(errs) > 0 {
			for _, e := range errs {
				log.Printf("[Tracing:%s] WASM filter error: %v", m.serviceName, e)
			}
		}
		if result != nil && !result.Keep {
			m.mu.Lock()
			m.stats.WasmFiltered++
			m.mu.Unlock()
			return
		}
	}

	if m.useNATS && m.nats != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := m.nats.PublishSpan(ctx, span); err != nil {
			m.mu.Lock()
			m.stats.Errors++
			m.mu.Unlock()
			log.Printf("[Tracing:%s] NATS publish error (fallback to HTTP): %v", m.serviceName, err)
			m.sendSpanHTTP(span)
		} else {
			m.mu.Lock()
			m.stats.Published++
			m.mu.Unlock()
		}
		return
	}

	m.sendSpanHTTP(span)
}

func (m *TracingMiddleware) sendSpanHTTP(span *model.TraceSpan) {
	if m.producerURL == "" || strings.HasPrefix(m.producerURL, "disabled") {
		return
	}

	spanJSON, err := json.Marshal(span)
	if err != nil {
		log.Printf("[Tracing:%s] Failed to marshal span: %v", m.serviceName, err)
		return
	}

	req, err := http.NewRequest("POST", m.producerURL, bytes.NewBuffer(spanJSON))
	if err != nil {
		m.mu.Lock()
		m.stats.Errors++
		m.mu.Unlock()
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		m.mu.Lock()
		m.stats.Errors++
		m.mu.Unlock()
		return
	}
	defer resp.Body.Close()
	io.ReadAll(resp.Body)

	m.mu.Lock()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		m.stats.Published++
	} else {
		m.stats.Errors++
	}
	m.mu.Unlock()
}

func (m *TracingMiddleware) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		m.mu.Lock()
		for k, v := range m.pending {
			if now.Sub(v.StartTime) > 5*time.Minute {
				delete(m.pending, k)
			}
		}
		m.mu.Unlock()
	}
}

func (m *TracingMiddleware) Stats() MiddlewareStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stats
}

func (m *TracingMiddleware) Sampler() *sampling.DynamicSampler {
	return m.sampler
}

func (m *TracingMiddleware) WasmManager() *wasmmgr.WasmPluginManager {
	return m.wasmMgr
}

func (m *TracingMiddleware) Close() {
	if m.nats != nil {
		m.nats.Close()
	}
	if m.sampler != nil {
		m.sampler.Stop()
	}
	if m.wasmMgr != nil {
		m.wasmMgr.Stop()
	}
}

func getRemoteIP(c *gin.Context) string {
	if ip := c.GetHeader("X-Forwarded-For"); ip != "" {
		ips := strings.Split(ip, ",")
		return strings.TrimSpace(ips[0])
	}
	if ip := c.GetHeader("X-Real-IP"); ip != "" {
		return ip
	}
	return c.ClientIP()
}

type TracingRoundTripper struct {
	Base        http.RoundTripper
	ServiceName string
	ProducerURL string
	NATSURL     string
	useNATS     bool
	nats        *traceprod.NATSTraceProducer
	sampler     *sampling.DynamicSampler
	wasmMgr     *wasmmgr.WasmPluginManager
	httpClient  *http.Client
	initOnce    sync.Once
	mu          sync.Mutex
	stats       map[string]int64
}

func (t *TracingRoundTripper) init() {
	t.initOnce.Do(func() {
		if t.httpClient == nil {
			t.httpClient = &http.Client{
				Timeout: 5 * time.Second,
				Transport: &http.Transport{
					MaxIdleConnsPerHost: 16,
				},
			}
		}

		t.stats = make(map[string]int64)

		samplingCfg := sampling.SamplingConfig{
			Strategy:           sampling.StrategyDynamic,
			BaseSampleRate:     0.01,
			MinSampleRate:      0.01,
			MaxSampleRate:      1.0,
			ErrorRateThreshold: 0.05,
			AlwaysSampleError:  true,
		}
		t.sampler = sampling.NewDynamicSampler(&samplingCfg)

		natsURL := t.NATSURL
		if natsURL == "" {
			natsURL = os.Getenv("NATS_URL")
			if natsURL == "" {
				natsURL = "nats://nats:4222"
			}
		}

		t.useNATS = getEnvOrDefault("TRACE_USE_NATS", "true") == "true"
		if t.useNATS {
			natsCfg := &natsutil.NATSConfig{
				URLs:          strings.Split(natsURL, ","),
				StreamName:    getEnvOrDefault("NATS_STREAM", "TRACES"),
				SubjectPrefix: "trace.spans",
			}
			var err error
			t.nats, err = traceprod.NewNATSProducer(natsCfg, t.sampler, t.wasmMgr)
			if err != nil {
				log.Printf("[TracingRT:%s] Warning: NATS unavailable: %v", t.ServiceName, err)
				t.useNATS = false
			}
		}
	})
}

func (t *TracingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	t.init()

	requestID := req.Header.Get("X-Request-ID")
	traceID := req.Header.Get("X-Trace-ID")
	parentSpanID := req.Header.Get("X-Parent-Span-ID")

	if requestID == "" {
		requestID = uuid.New().String()
		req.Header.Set("X-Request-ID", requestID)
	}
	if traceID == "" {
		traceID = uuid.New().String()
		req.Header.Set("X-Trace-ID", traceID)
	}
	if parentSpanID == "" {
		if existingParent := req.Header.Get("X-Span-ID"); existingParent != "" {
			parentSpanID = existingParent
		}
	}

	spanID := uuid.New().String()
	req.Header.Set("X-Span-ID", spanID)
	req.Header.Set("X-Parent-Span-ID", parentSpanID)
	req.Header.Set("X-Service-Name", t.ServiceName)
	req.Header.Set("X-Function-Name", t.ServiceName)

	startTime := time.Now()
	destIP, destPort := parseHostPort(req.URL.Host)

	span := model.TraceSpan{
		RequestID:    requestID,
		TraceID:      traceID,
		SpanID:       spanID,
		ParentSpanID: parentSpanID,
		ServiceName:  t.ServiceName,
		FunctionName: req.URL.Host + req.URL.Path,
		Method:       req.Method,
		Path:         req.URL.Path,
		StartTime:    startTime,
		Protocol:     "HTTP",
		DestIP:       destIP,
		DestPort:     destPort,
	}

	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}

	resp, err := base.RoundTrip(req)

	duration := time.Since(startTime)
	span.EndTime = startTime.Add(duration)
	span.DurationMs = duration.Milliseconds()
	span.Timestamp = startTime.Add(duration)

	if err != nil {
		span.StatusCode = 0
	} else {
		span.StatusCode = resp.StatusCode
	}

	go t.publishSpan(&span)

	return resp, err
}

func (t *TracingRoundTripper) publishSpan(span *model.TraceSpan) {
	if span == nil {
		return
	}

	if t.sampler != nil && !t.sampler.ShouldSample(span) {
		t.mu.Lock()
		t.stats["sampled"]++
		t.mu.Unlock()
		return
	}

	if t.wasmMgr != nil && t.wasmMgr.IsEnabled() {
		result, _ := t.wasmMgr.RunFilters(span)
		if result != nil && !result.Keep {
			t.mu.Lock()
			t.stats["wasm_filtered"]++
			t.mu.Unlock()
			return
		}
	}

	if t.useNATS && t.nats != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := t.nats.PublishSpan(ctx, span); err != nil {
			t.mu.Lock()
			t.stats["errors"]++
			t.mu.Unlock()
		} else {
			t.mu.Lock()
			t.stats["published"]++
			t.mu.Unlock()
			return
		}
	}

	t.sendHTTPFallback(span)
}

func (t *TracingRoundTripper) sendHTTPFallback(span *model.TraceSpan) {
	producerURL := t.ProducerURL
	if producerURL == "" {
		producerURL = os.Getenv("TRACE_PRODUCER_URL")
		if producerURL == "" {
			producerURL = "http://nats-producer:8085/v1/spans"
		}
	}
	if producerURL == "" || strings.HasPrefix(producerURL, "disabled") {
		return
	}

	spanJSON, err := json.Marshal(span)
	if err != nil {
		return
	}

	req, err := http.NewRequest("POST", producerURL, bytes.NewBuffer(spanJSON))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := t.httpClient
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	io.ReadAll(resp.Body)
}

func parseHostPort(hostport string) (string, uint16) {
	host := hostport
	var portStr string
	if strings.Contains(hostport, ":") {
		parts := strings.SplitN(hostport, ":", 2)
		host = parts[0]
		portStr = parts[1]
	}

	var port uint16
	switch portStr {
	case "80", "":
		port = 80
	case "443":
		port = 443
	default:
		if p, err := strconv.Atoi(portStr); err == nil && p > 0 && p < 65536 {
			port = uint16(p)
		}
	}

	if strings.HasPrefix(host, "http://") {
		host = host[7:]
	} else if strings.HasPrefix(host, "https://") {
		host = host[8:]
	}

	return host, port
}

func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

var _ = fmt.Sprintf
