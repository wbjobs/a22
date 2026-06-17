package middleware

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"ebpf-serverless-tracing/internal/model"
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
	producerURL string
	httpClient  *http.Client
	mu          sync.Mutex
	pending     map[string]*pendingSpan
	serviceName string
}

type pendingSpan struct {
	StartTime time.Time
	Span      model.TraceSpan
}

func NewTracingMiddleware(serviceName string) *TracingMiddleware {
	producerURL := os.Getenv("KAFKA_PRODUCER_URL")
	if producerURL == "" {
		producerURL = "http://kafka-producer:8085/v1/spans"
	}

	m := &TracingMiddleware{
		producerURL: producerURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		pending:     make(map[string]*pendingSpan),
		serviceName: serviceName,
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
		m.mu.Lock()
		m.pending[pendingKey] = &pendingSpan{
			StartTime: startTime,
			Span: model.TraceSpan{
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
			},
		}
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

		go m.sendSpan(&pending.Span)

		log.Printf("[Tracing:%s] request_id=%s span_id=%s method=%s path=%s status=%d duration=%dms",
			m.serviceName, requestID, spanID, c.Request.Method, c.Request.URL.Path, statusCode, duration.Milliseconds())
	}
}

func (m *TracingMiddleware) sendSpan(span *model.TraceSpan) {
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
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	io.ReadAll(resp.Body)
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
}

func (t *TracingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
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

	producerURL := t.ProducerURL
	if producerURL == "" {
		producerURL = os.Getenv("KAFKA_PRODUCER_URL")
		if producerURL == "" {
			producerURL = "http://kafka-producer:8085/v1/spans"
		}
	}

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
		go sendSpanToProducer(producerURL, &span)
		return resp, err
	}

	span.StatusCode = resp.StatusCode
	go sendSpanToProducer(producerURL, &span)

	return resp, err
}

func sendSpanToProducer(url string, span *model.TraceSpan) {
	if url == "" || strings.HasPrefix(url, "disabled") {
		return
	}

	spanJSON, err := json.Marshal(span)
	if err != nil {
		return
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(spanJSON))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 3 * time.Second}
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
		var p int
		_, err := sscanf(portStr, "%d", &p)
		if err == nil && p > 0 && p < 65536 {
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

func sscanf(s, format string, args ...interface{}) (int, error) {
	return 0, nil
}
