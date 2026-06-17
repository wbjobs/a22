package chaos

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
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
	ServiceName string            `json:"service_name"`
	Type        InjectionType     `json:"type"`
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
}

type ChaosEngine struct {
	mu       sync.RWMutex
	rules    map[string]*InjectionRule
	service  string
	rng      *rand.Rand
	manager  string
}

var (
	defaultEngine *ChaosEngine
	defaultOnce   sync.Once
)

func DefaultEngine() *ChaosEngine {
	defaultOnce.Do(func() {
		svc := os.Getenv("CHAOS_SERVICE_NAME")
		if svc == "" {
			svc = os.Getenv("FUNCTION_NAME")
		}
		mgr := os.Getenv("CHAOS_MANAGER_URL")
		if mgr == "" {
			mgr = "http://chaos-manager:8088"
		}
		defaultEngine = NewChaosEngine(svc, mgr)
	})
	return defaultEngine
}

func NewChaosEngine(serviceName, managerURL string) *ChaosEngine {
	e := &ChaosEngine{
		rules:   make(map[string]*InjectionRule),
		service: serviceName,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
		manager: managerURL,
	}

	if managerURL != "" {
		go e.syncLoop()
	}

	log.Printf("[Chaos] Engine initialized for service=%s, manager=%s", serviceName, managerURL)
	return e
}

func (e *ChaosEngine) syncLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		e.fetchRulesFromManager()
		e.cleanupExpired()
	}
}

func (e *ChaosEngine) fetchRulesFromManager() {
	if e.manager == "" || e.service == "" {
		return
	}
	url := fmt.Sprintf("%s/api/rules?service=%s", e.manager, e.service)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return
	}
	var rules []*InjectionRule
	if err := json.NewDecoder(resp.Body).Decode(&rules); err != nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range rules {
		if r.Enabled && !r.ExpiresAt.IsZero() && r.ExpiresAt.After(time.Now()) {
			e.rules[r.ID] = r
		} else {
			delete(e.rules, r.ID)
		}
	}
}

func (e *ChaosEngine) AddRule(rule *InjectionRule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rule.CreatedAt = time.Now()
	if rule.DurationSec > 0 && rule.ExpiresAt.IsZero() {
		rule.ExpiresAt = time.Now().Add(time.Duration(rule.DurationSec) * time.Second)
	}
	e.rules[rule.ID] = rule
	log.Printf("[Chaos] Rule added: id=%s type=%s prob=%.2f expires=%v",
		rule.ID, rule.Type, rule.Probability, rule.ExpiresAt)
}

func (e *ChaosEngine) RemoveRule(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.rules, id)
	log.Printf("[Chaos] Rule removed: id=%s", id)
}

func (e *ChaosEngine) ClearRules() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = make(map[string]*InjectionRule)
	log.Printf("[Chaos] All rules cleared")
}

func (e *ChaosEngine) ListRules() []*InjectionRule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]*InjectionRule, 0, len(e.rules))
	for _, r := range e.rules {
		result = append(result, r)
	}
	return result
}

func (e *ChaosEngine) cleanupExpired() {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	for id, r := range e.rules {
		if !r.ExpiresAt.IsZero() && r.ExpiresAt.Before(now) {
			delete(e.rules, id)
			log.Printf("[Chaos] Rule expired and removed: id=%s", id)
		}
	}
}

func (e *ChaosEngine) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		rule := e.findMatchingRule(c)
		if rule != nil {
			e.applyRule(c, rule)
		}
		c.Next()
	}
}

func (e *ChaosEngine) findMatchingRule(c *gin.Context) *InjectionRule {
	e.mu.RLock()
	defer e.mu.RUnlock()

	now := time.Now()
	for _, r := range e.rules {
		if !r.Enabled {
			continue
		}
		if !r.ExpiresAt.IsZero() && r.ExpiresAt.Before(now) {
			continue
		}
		if len(r.Paths) > 0 {
			matched := false
			for _, p := range r.Paths {
				if p == c.Request.URL.Path {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		if len(r.Headers) > 0 {
			matched := true
			for k, v := range r.Headers {
				if c.GetHeader(k) != v {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
		}
		if e.rng.Float64() < r.Probability {
			return r
		}
	}
	return nil
}

func (e *ChaosEngine) applyRule(c *gin.Context, rule *InjectionRule) {
	e.mu.Lock()
	rule.HitCount++
	e.mu.Unlock()

	log.Printf("[Chaos] Injecting %s for %s %s (rule=%s)",
		rule.Type, c.Request.Method, c.Request.URL.Path, rule.ID)

	switch rule.Type {
	case TypeLatency:
		delay := time.Duration(rule.LatencyMs) * time.Millisecond
		if delay <= 0 {
			delay = 500 * time.Millisecond
		}
		time.Sleep(delay)

	case TypeError:
		status := rule.StatusCode
		if status == 0 {
			status = 500
		}
		msg := rule.Message
		if msg == "" {
			msg = "Chaos Engineering: Injected Error"
		}
		c.JSON(status, gin.H{
			"error":      msg,
			"chaos_rule": rule.ID,
			"chaos_type": string(rule.Type),
		})
		c.Abort()

	case TypeAbort:
		status := rule.StatusCode
		if status == 0 {
			status = 503
		}
		c.AbortWithStatus(status)

	case TypeException:
		msg := rule.Message
		if msg == "" {
			msg = "Chaos Engineering: Simulated Exception"
		}
		panic(msg)
	}
}

func Middleware() gin.HandlerFunc {
	return DefaultEngine().Middleware()
}

func AddRule(rule *InjectionRule) {
	DefaultEngine().AddRule(rule)
}

func RemoveRule(id string) {
	DefaultEngine().RemoveRule(id)
}

func ClearRules() {
	DefaultEngine().ClearRules()
}

func ListRules() []*InjectionRule {
	return DefaultEngine().ListRules()
}

func RegisterAdminRoutes(r *gin.RouterGroup) {
	eng := DefaultEngine()

	r.GET("/chaos/rules", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"service": eng.service,
			"rules":   eng.ListRules(),
		})
	})

	r.POST("/chaos/rules", func(c *gin.Context) {
		var rule InjectionRule
		if err := c.ShouldBindJSON(&rule); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		if rule.ID == "" {
			rule.ID = fmt.Sprintf("rule-%d", time.Now().UnixNano())
		}
		if rule.Probability <= 0 {
			rule.Probability = 1.0
		}
		if rule.Probability > 1.0 {
			rule.Probability = 1.0
		}
		rule.Enabled = true
		rule.ServiceName = eng.service
		eng.AddRule(&rule)
		c.JSON(201, rule)
	})

	r.DELETE("/chaos/rules/:id", func(c *gin.Context) {
		id := c.Param("id")
		eng.RemoveRule(id)
		c.JSON(200, gin.H{"status": "removed", "id": id})
	})

	r.POST("/chaos/clear", func(c *gin.Context) {
		eng.ClearRules()
		c.JSON(200, gin.H{"status": "cleared"})
	})

	r.POST("/chaos/inject/latency", func(c *gin.Context) {
		var req struct {
			Ms       int     `json:"ms" binding:"min=1"`
			Prob     float64 `json:"prob"`
			Duration int     `json:"duration_sec"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		rule := &InjectionRule{
			ID:          fmt.Sprintf("latency-%d", time.Now().UnixNano()),
			ServiceName: eng.service,
			Type:        TypeLatency,
			Probability: req.Prob,
			LatencyMs:   req.Ms,
			DurationSec: req.Duration,
			Enabled:     true,
		}
		if rule.Probability == 0 {
			rule.Probability = 1.0
		}
		if rule.DurationSec == 0 {
			rule.DurationSec = 60
		}
		eng.AddRule(rule)
		c.JSON(200, rule)
	})

	r.POST("/chaos/inject/error", func(c *gin.Context) {
		var req struct {
			StatusCode int     `json:"status_code"`
			Prob       float64 `json:"prob"`
			Duration   int     `json:"duration_sec"`
			Message    string  `json:"message"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		rule := &InjectionRule{
			ID:          fmt.Sprintf("error-%d", time.Now().UnixNano()),
			ServiceName: eng.service,
			Type:        TypeError,
			Probability: req.Prob,
			StatusCode:  req.StatusCode,
			Message:     req.Message,
			DurationSec: req.Duration,
			Enabled:     true,
		}
		if rule.Probability == 0 {
			rule.Probability = 1.0
		}
		if rule.StatusCode == 0 {
			rule.StatusCode = 500
		}
		if rule.DurationSec == 0 {
			rule.DurationSec = 60
		}
		eng.AddRule(rule)
		c.JSON(200, rule)
	})
}
