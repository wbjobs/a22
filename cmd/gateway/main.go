package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"ebpf-serverless-tracing/internal/config"
	"ebpf-serverless-tracing/internal/model"
)

type App struct {
	cfg *config.Config
}

func main() {
	cfg := config.Load()
	app := &App{cfg: cfg}

	r := gin.Default()

	r.Use(app.requestIDMiddleware())
	r.Use(app.loggingMiddleware())

	r.POST("/api/order", app.handleOrder)
	r.GET("/health", app.healthCheck)

	log.Printf("API Gateway starting on port %s", cfg.GatewayPort)
	if err := r.Run(":" + cfg.GatewayPort); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func (a *App) requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}
		c.Set("request_id", requestID)
		c.Header("X-Request-ID", requestID)

		traceID := c.GetHeader("X-Trace-ID")
		if traceID == "" {
			traceID = uuid.New().String()
		}
		c.Set("trace_id", traceID)
		c.Header("X-Trace-ID", traceID)

		c.Next()
	}
}

func (a *App) loggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		duration := time.Since(start)
		requestID, _ := c.Get("request_id")
		log.Printf("[Gateway] request_id=%s method=%s path=%s status=%d duration=%v",
			requestID, c.Request.Method, c.Request.URL.Path, c.Writer.Status(), duration)
	}
}

func (a *App) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"service": "api-gateway",
		"status":  "healthy",
		"time":    time.Now().UTC(),
	})
}

func (a *App) handleOrder(c *gin.Context) {
	var req model.OrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	requestID, _ := c.Get("request_id")
	traceID, _ := c.Get("trace_id")
	spanID := uuid.New().String()

	startTime := time.Now()

	orderResp, err := a.callFunctionA(c, &req, requestID.(string), traceID.(string), spanID)
	if err != nil {
		duration := time.Since(startTime)
		log.Printf("[Gateway] request_id=%s FunctionA failed: %v, duration=%v", requestID, err, duration)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":      fmt.Sprintf("Order service failed: %v", err),
			"request_id": requestID,
		})
		return
	}

	totalDuration := time.Since(startTime)

	log.Printf("[Gateway] request_id=%s order completed, order_id=%s, total_duration=%v",
		requestID, orderResp.OrderID, totalDuration)

	c.JSON(http.StatusOK, model.OrderResponse{
		RequestID:   requestID.(string),
		OrderID:     orderResp.OrderID,
		Status:      orderResp.Status,
		TotalAmount: orderResp.TotalAmount,
		Message:     fmt.Sprintf("Order processed successfully in %v", totalDuration),
	})
}

type FunctionAResponse struct {
	OrderID     string  `json:"order_id"`
	Status      string  `json:"status"`
	TotalAmount float64 `json:"total_amount"`
	Message     string  `json:"message"`
}

func (a *App) callFunctionA(c *gin.Context, req *model.OrderRequest, requestID, traceID, parentSpanID string) (*FunctionAResponse, error) {
	body, _ := json.Marshal(req)

	httpReq, err := http.NewRequestWithContext(c, "POST", a.cfg.FunctionAURL, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Request-ID", requestID)
	httpReq.Header.Set("X-Trace-ID", traceID)
	httpReq.Header.Set("X-Parent-Span-ID", parentSpanID)
	httpReq.Header.Set("X-Forwarded-For", c.ClientIP())

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("FunctionA HTTP error: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("FunctionA returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result FunctionAResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("FunctionA parse error: %w", err)
	}

	return &result, nil
}
