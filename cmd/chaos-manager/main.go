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
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

type InjectionType string

const (
	TypeLatency   InjectionType = "latency"
	TypeError     InjectionType = "error"
	TypeAbort     InjectionType = "abort"
	TypeException InjectionType = "exception"
)

type InjectionRule struct {
	ID          string            `json:"id"`
	ServiceName string            `json:"service_name" binding:"required"`
	Type        InjectionType     `json:"type" binding:"required,oneof=latency error abort exception"`
	Probability float64           `json:"probability"`
	LatencyMs   int               `json:"latency_ms,omitempty"`
	StatusCode  int               `json:"status_code,omitempty"`
	Message     string            `json:"message,omitempty"`
	DurationSec int               `json:"duration_sec"`
	ExpiresAt   time.Time         `json:"expires_at"`
	CreatedAt   time.Time         `json:"created_at"`
	Enabled     bool              `json:"enabled"`
	Paths       []string          `json:"paths,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	HitCount    int64             `json:"hit_count"`
	CreatedBy   string            `json:"created_by,omitempty"`
	Reason      string            `json:"reason,omitempty"`
}

type ChaosManager struct {
	mu       sync.RWMutex
	rules    map[string]*InjectionRule
	services map[string]string
}

func NewChaosManager() *ChaosManager {
	return &ChaosManager{
		rules:    make(map[string]*InjectionRule),
		services: map[string]string{
			"gateway":    "http://gateway:8080",
			"function-a": "http://function-a:8082",
			"function-b": "http://function-b:8083",
			"function-c": "http://function-c:8084",
		},
	}
}

func (m *ChaosManager) AddRule(rule *InjectionRule) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if rule.ID == "" {
		rule.ID = fmt.Sprintf("rule-%d", time.Now().UnixNano())
	}
	rule.CreatedAt = time.Now()
	if rule.Probability <= 0 {
		rule.Probability = 1.0
	}
	if rule.Probability > 1.0 {
		rule.Probability = 1.0
	}
	if rule.DurationSec > 0 && rule.ExpiresAt.IsZero() {
		rule.ExpiresAt = time.Now().Add(time.Duration(rule.DurationSec) * time.Second)
	}
	rule.Enabled = true

	m.rules[rule.ID] = rule

	log.Printf("[ChaosManager] Rule added: id=%s service=%s type=%s prob=%.2f expires=%v",
		rule.ID, rule.ServiceName, rule.Type, rule.Probability, rule.ExpiresAt)

	go m.pushRuleToService(rule)
	return nil
}

func (m *ChaosManager) pushRuleToService(rule *InjectionRule) {
	baseURL, ok := m.services[rule.ServiceName]
	if !ok {
		log.Printf("[ChaosManager] Unknown service: %s", rule.ServiceName)
		return
	}
	url := fmt.Sprintf("%s/admin/chaos/rules", baseURL)
	body, _ := json.Marshal(rule)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[ChaosManager] Failed to push rule to %s: %v", rule.ServiceName, err)
		return
	}
	defer resp.Body.Close()
	log.Printf("[ChaosManager] Rule pushed to %s, HTTP %d", rule.ServiceName, resp.StatusCode)
}

func (m *ChaosManager) RemoveRule(id string) bool {
	m.mu.Lock()
	rule, ok := m.rules[id]
	if !ok {
		m.mu.Unlock()
		return false
	}
	delete(m.rules, id)
	service := rule.ServiceName
	m.mu.Unlock()

	baseURL, exists := m.services[service]
	if exists {
		url := fmt.Sprintf("%s/admin/chaos/rules/%s", baseURL, id)
		client := &http.Client{Timeout: 5 * time.Second}
		req, _ := http.NewRequest("DELETE", url, nil)
		client.Do(req)
	}
	log.Printf("[ChaosManager] Rule removed: id=%s", id)
	return true
}

func (m *ChaosManager) ClearRules() int {
	m.mu.Lock()
	rules := make([]*InjectionRule, 0, len(m.rules))
	for _, r := range m.rules {
		rules = append(rules, r)
	}
	m.rules = make(map[string]*InjectionRule)
	m.mu.Unlock()

	count := 0
	for _, r := range rules {
		baseURL, ok := m.services[r.ServiceName]
		if ok {
			url := fmt.Sprintf("%s/admin/chaos/clear", baseURL)
			client := &http.Client{Timeout: 5 * time.Second}
			client.Post(url, "application/json", nil)
		}
		count++
	}
	return count
}

func (m *ChaosManager) GetRulesForService(service string) []*InjectionRule {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*InjectionRule, 0)
	now := time.Now()
	for _, r := range m.rules {
		if service != "" && r.ServiceName != service {
			continue
		}
		if !r.ExpiresAt.IsZero() && r.ExpiresAt.Before(now) {
			continue
		}
		result = append(result, r)
	}
	return result
}

func (m *ChaosManager) GetAllRules() []*InjectionRule {
	return m.GetRulesForService("")
}

func (m *ChaosManager) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		m.mu.Lock()
		now := time.Now()
		for id, r := range m.rules {
			if !r.ExpiresAt.IsZero() && r.ExpiresAt.Before(now) {
				delete(m.rules, id)
				log.Printf("[ChaosManager] Expired rule removed: id=%s", id)
			}
		}
		m.mu.Unlock()
	}
}

func main() {
	port := os.Getenv("CHAOS_MANAGER_PORT")
	if port == "" {
		port = "8088"
	}

	manager := NewChaosManager()
	go manager.cleanupLoop()

	r := gin.Default()

	r.Use(func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Printf("[ChaosManager] method=%s path=%s status=%d duration=%v",
			c.Request.Method, c.Request.URL.Path, c.Writer.Status(), time.Since(start))
	})

	api := r.Group("/api")
	{
		api.GET("/rules", func(c *gin.Context) {
			service := c.Query("service")
			rules := manager.GetRulesForService(service)
			c.JSON(200, rules)
		})

		api.GET("/rules/:id", func(c *gin.Context) {
			id := c.Param("id")
			rules := manager.GetAllRules()
			for _, r := range rules {
				if r.ID == id {
					c.JSON(200, r)
					return
				}
			}
			c.JSON(404, gin.H{"error": "rule not found"})
		})

		api.POST("/rules", func(c *gin.Context) {
			var rule InjectionRule
			if err := c.ShouldBindJSON(&rule); err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			if err := manager.AddRule(&rule); err != nil {
				c.JSON(500, gin.H{"error": err.Error()})
				return
			}
			c.JSON(201, rule)
		})

		api.PUT("/rules/:id/enable", func(c *gin.Context) {
			id := c.Param("id")
			manager.mu.Lock()
			defer manager.mu.Unlock()
			if r, ok := manager.rules[id]; ok {
				r.Enabled = true
				c.JSON(200, r)
			} else {
				c.JSON(404, gin.H{"error": "not found"})
			}
		})

		api.PUT("/rules/:id/disable", func(c *gin.Context) {
			id := c.Param("id")
			manager.mu.Lock()
			defer manager.mu.Unlock()
			if r, ok := manager.rules[id]; ok {
				r.Enabled = false
				c.JSON(200, r)
			} else {
				c.JSON(404, gin.H{"error": "not found"})
			}
		})

		api.DELETE("/rules/:id", func(c *gin.Context) {
			id := c.Param("id")
			if manager.RemoveRule(id) {
				c.JSON(200, gin.H{"status": "removed"})
			} else {
				c.JSON(404, gin.H{"error": "not found"})
			}
		})

		api.POST("/clear", func(c *gin.Context) {
			count := manager.ClearRules()
			c.JSON(200, gin.H{"status": "cleared", "removed_count": count})
		})

		api.POST("/inject/latency", func(c *gin.Context) {
			var req struct {
				Service    string  `json:"service" binding:"required"`
				Ms         int     `json:"ms" binding:"min=1"`
				Prob       float64 `json:"prob"`
				Duration   int     `json:"duration_sec"`
				Paths      []string `json:"paths,omitempty"`
				Reason     string  `json:"reason"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			if req.Duration == 0 {
				req.Duration = 300
			}
			if req.Prob == 0 {
				req.Prob = 1.0
			}
			rule := &InjectionRule{
				ID:          fmt.Sprintf("latency-%d", time.Now().UnixNano()),
				ServiceName: req.Service,
				Type:        TypeLatency,
				Probability: req.Prob,
				LatencyMs:   req.Ms,
				DurationSec: req.Duration,
				Paths:       req.Paths,
				Reason:      req.Reason,
				Enabled:     true,
			}
			manager.AddRule(rule)
			c.JSON(201, gin.H{
				"status":  "injected",
				"rule_id": rule.ID,
				"service": req.Service,
				"type":    "latency",
				"ms":      req.Ms,
				"duration_sec": req.Duration,
				"message": fmt.Sprintf("Latency chaos injected to %s: %dms for %ds", req.Service, req.Ms, req.Duration),
			})
		})

		api.POST("/inject/error", func(c *gin.Context) {
			var req struct {
				Service    string  `json:"service" binding:"required"`
				StatusCode int     `json:"status_code"`
				Prob       float64 `json:"prob"`
				Duration   int     `json:"duration_sec"`
				Message    string  `json:"message"`
				Paths      []string `json:"paths,omitempty"`
				Reason     string  `json:"reason"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			if req.StatusCode == 0 {
				req.StatusCode = 500
			}
			if req.Duration == 0 {
				req.Duration = 300
			}
			if req.Prob == 0 {
				req.Prob = 1.0
			}
			if req.Message == "" {
				req.Message = "Chaos Engineering: Simulated Server Error"
			}
			rule := &InjectionRule{
				ID:          fmt.Sprintf("error-%d", time.Now().UnixNano()),
				ServiceName: req.Service,
				Type:        TypeError,
				Probability: req.Prob,
				StatusCode:  req.StatusCode,
				Message:     req.Message,
				DurationSec: req.Duration,
				Paths:       req.Paths,
				Reason:      req.Reason,
				Enabled:     true,
			}
			manager.AddRule(rule)
			c.JSON(201, gin.H{
				"status":  "injected",
				"rule_id": rule.ID,
				"service": req.Service,
				"type":    "error",
				"status_code": req.StatusCode,
				"duration_sec": req.Duration,
				"message": fmt.Sprintf("Error chaos injected to %s: HTTP %d for %ds", req.Service, req.StatusCode, req.Duration),
			})
		})

		api.POST("/inject/abort", func(c *gin.Context) {
			var req struct {
				Service  string  `json:"service" binding:"required"`
				Prob     float64 `json:"prob"`
				Duration int     `json:"duration_sec"`
				Reason   string  `json:"reason"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			if req.Duration == 0 {
				req.Duration = 60
			}
			if req.Prob == 0 {
				req.Prob = 1.0
			}
			rule := &InjectionRule{
				ID:          fmt.Sprintf("abort-%d", time.Now().UnixNano()),
				ServiceName: req.Service,
				Type:        TypeAbort,
				Probability: req.Prob,
				StatusCode:  503,
				DurationSec: req.Duration,
				Reason:      req.Reason,
				Enabled:     true,
			}
			manager.AddRule(rule)
			c.JSON(201, gin.H{
				"status":  "injected",
				"rule_id": rule.ID,
				"service": req.Service,
				"message": fmt.Sprintf("Connection abort chaos injected to %s for %ds", req.Service, req.Duration),
			})
		})

		api.GET("/services", func(c *gin.Context) {
			services := make([]map[string]interface{}, 0)
			manager.mu.RLock()
			for name, url := range manager.services {
				activeRules := 0
				now := time.Now()
				for _, r := range manager.rules {
					if r.ServiceName == name && r.Enabled && (r.ExpiresAt.IsZero() || r.ExpiresAt.After(now)) {
						activeRules++
					}
				}
				services = append(services, map[string]interface{}{
					"name":        name,
					"admin_url":   url + "/admin",
					"active_rules": activeRules,
				})
			}
			manager.mu.RUnlock()
			c.JSON(200, services)
		})

		api.GET("/stats", func(c *gin.Context) {
			all := manager.GetAllRules()
			active := 0
			perService := make(map[string]int)
			perType := make(map[string]int)
			now := time.Now()
			for _, r := range all {
				if r.Enabled && (r.ExpiresAt.IsZero() || r.ExpiresAt.After(now)) {
					active++
					perService[r.ServiceName]++
					perType[string(r.Type)]++
				}
			}
			c.JSON(200, gin.H{
				"total_rules":   len(all),
				"active_rules":  active,
				"per_service":   perService,
				"per_type":      perType,
			})
		})
	}

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"service": "chaos-manager",
			"status":  "healthy",
			"time":    time.Now().UTC(),
		})
	})

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

	go func() {
		log.Printf("Chaos Engineering Manager starting on port %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Chaos Manager...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Shutdown failed: %v", err)
	}
	log.Println("Chaos Manager stopped")
}
