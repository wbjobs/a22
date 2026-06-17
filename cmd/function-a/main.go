package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"ebpf-serverless-tracing/internal/chaos"
)

type OrderRequest struct {
	ProductID string  `json:"product_id"`
	Amount    float64 `json:"amount"`
	UserID    string  `json:"user_id"`
}

type OrderResponse struct {
	OrderID     string  `json:"order_id"`
	Status      string  `json:"status"`
	TotalAmount float64 `json:"total_amount"`
	Message     string  `json:"message"`
}

type PaymentRequest struct {
	OrderID    string  `json:"order_id"`
	Amount     float64 `json:"amount"`
	UserID     string  `json:"user_id"`
	ProductID  string  `json:"product_id"`
}

type PaymentResponse struct {
	PaymentID     string  `json:"payment_id"`
	OrderID       string  `json:"order_id"`
	Status        string  `json:"status"`
	Amount        float64 `json:"amount"`
	TransactionID string  `json:"transaction_id"`
}

var functionBURL string

func init() {
	functionBURL = getEnv("FUNCTION_B_URL", "http://function-b:8083/payment")
}

func main() {
	port := getEnv("FUNCTION_A_PORT", "8082")
	r := gin.Default()

	r.Use(func(c *gin.Context) {
		start := time.Now()
		requestID := c.GetHeader("X-Request-ID")
		c.Next()
		duration := time.Since(start)
		log.Printf("[FunctionA:Go] request_id=%s path=%s status=%d duration=%v",
			requestID, c.Request.URL.Path, c.Writer.Status(), duration)
	})
	r.Use(chaos.Middleware())

	admin := r.Group("/admin")
	chaos.RegisterAdminRoutes(admin)

	r.POST("/order", handleOrder)
	r.GET("/health", healthCheck)

	log.Printf("FunctionA (Go - Order Service) starting on port %s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("Failed to start: %v", err)
	}
}

func healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"service":  "function-a",
		"language": "go",
		"status":   "healthy",
		"time":     time.Now().UTC(),
	})
}

func handleOrder(c *gin.Context) {
	requestID := c.GetHeader("X-Request-ID")
	traceID := c.GetHeader("X-Trace-ID")
	spanID := uuid.New().String()
	startTime := time.Now()

	var req OrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Printf("[FunctionA] request_id=%s invalid request: %v", requestID, err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	log.Printf("[FunctionA] request_id=%s processing order: product=%s amount=%.2f user=%s",
		requestID, req.ProductID, req.Amount, req.UserID)

	orderID := fmt.Sprintf("ORD-%s", uuid.New().String()[:8])
	totalAmount := req.Amount * 1.08

	time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)

	paymentReq := PaymentRequest{
		OrderID:   orderID,
		Amount:    totalAmount,
		UserID:    req.UserID,
		ProductID: req.ProductID,
	}

	paymentResp, err := callFunctionB(&paymentReq, requestID, traceID, spanID)
	if err != nil {
		duration := time.Since(startTime)
		log.Printf("[FunctionA] request_id=%s call to FunctionB failed: %v, duration=%v",
			requestID, err, duration)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   fmt.Sprintf("Payment service call failed: %v", err),
			"order_id": orderID,
		})
		return
	}

	duration := time.Since(startTime)
	log.Printf("[FunctionA] request_id=%s order completed: order_id=%s payment_status=%s duration=%v",
		requestID, orderID, paymentResp.Status, duration)

	c.JSON(http.StatusOK, OrderResponse{
		OrderID:     orderID,
		Status:      paymentResp.Status,
		TotalAmount: totalAmount,
		Message:     fmt.Sprintf("Order processed, payment_id=%s", paymentResp.PaymentID),
	})
}

func callFunctionB(req *PaymentRequest, requestID, traceID, parentSpanID string) (*PaymentResponse, error) {
	body, _ := json.Marshal(req)

	httpReq, err := http.NewRequest("POST", functionBURL, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Request-ID", requestID)
	httpReq.Header.Set("X-Trace-ID", traceID)
	httpReq.Header.Set("X-Parent-Span-ID", parentSpanID)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("HTTP error: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody))
	}

	var result PaymentResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	return &result, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
