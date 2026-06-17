package model

import "time"

type TraceSpan struct {
	RequestID    string            `json:"request_id"`
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id,omitempty"`
	FunctionName string            `json:"function_name"`
	ServiceName  string            `json:"service_name"`
	StartTime    time.Time         `json:"start_time"`
	EndTime      time.Time         `json:"end_time"`
	DurationMs   int64             `json:"duration_ms"`
	StatusCode   int               `json:"status_code"`
	Method       string            `json:"method"`
	Path         string            `json:"path"`
	SourceIP     string            `json:"source_ip"`
	DestIP       string            `json:"dest_ip"`
	SourcePort   uint16            `json:"source_port"`
	DestPort     uint16            `json:"dest_port"`
	Protocol     string            `json:"protocol"`
	Headers      map[string]string `json:"headers,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	Timestamp    time.Time         `json:"timestamp"`
}

type OrderRequest struct {
	ProductID string  `json:"product_id"`
	Amount    float64 `json:"amount"`
	UserID    string  `json:"user_id"`
}

type OrderResponse struct {
	RequestID  string  `json:"request_id"`
	OrderID    string  `json:"order_id"`
	Status     string  `json:"status"`
	TotalAmount float64 `json:"total_amount"`
	Message    string  `json:"message"`
}

type TraceResult struct {
	RequestID   string      `json:"request_id"`
	TraceID     string      `json:"trace_id"`
	TotalSpans  int         `json:"total_spans"`
	StartTime   time.Time   `json:"start_time"`
	EndTime     time.Time   `json:"end_time"`
	TotalDurationMs int64   `json:"total_duration_ms"`
	Spans       []TraceSpan `json:"spans"`
	CallChain   []CallNode  `json:"call_chain"`
}

type CallNode struct {
	SpanID       string     `json:"span_id"`
	FunctionName string     `json:"function_name"`
	ServiceName  string     `json:"service_name"`
	StatusCode   int        `json:"status_code"`
	DurationMs   int64      `json:"duration_ms"`
	StartTime    time.Time  `json:"start_time"`
	Children     []CallNode `json:"children,omitempty"`
}
