package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
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

type App struct {
	cfg        *config.Config
	nats       *traceprod.NATSTraceProducer
	sampler    *sampling.DynamicSampler
	wasmMgr    *wasmmgr.WasmPluginManager
	mu         sync.Mutex
	stats      AppStats
}

type AppStats struct {
	MessagesReceived int64
	MessagesSent     int64
	MessagesSampled  int64
	MessagesWasmFiltered int64
	MessagesFailed   int64
	LastMessageTime  time.Time
	LastRequestID    string
	Uptime           time.Time
}

func main() {
	cfg := config.Load()

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
	sampler := sampling.NewDynamicSampler(&samplingCfg)
	defer sampler.Stop()

	wasmDir := os.Getenv("WASM_PLUGIN_DIR")
	if wasmDir == "" {
		wasmDir = "./plugins/wasm"
	}
	wasmMgr, err := wasmmgr.NewWasmPluginManager(wasmDir)
	if err != nil {
		log.Printf("[Producer] Warning: failed to init WASM manager: %v, continuing without WASM", err)
	} else {
		defer wasmMgr.Stop()
		log.Printf("[Producer] WASM plugin system initialized, dir=%s", wasmDir)
	}

	natsURLs := strings.Split(getEnv("NATS_URLS", "nats://nats:4222"), ",")
	natsCfg := &natsutil.NATSConfig{
		URLs:           natsURLs,
		StreamName:     getEnv("NATS_STREAM", "TRACES"),
		SubjectPrefix:  "trace.spans",
		MaxMsgs:        100000,
		MaxBytes:       2 * 1024 * 1024 * 1024,
		Replicas:       1,
		RetentionHours: 168,
	}

	natsProducer, err := traceprod.NewNATSProducer(natsCfg, sampler, wasmMgr)
	if err != nil {
		log.Fatalf("[Producer] Failed to create NATS producer: %v", err)
	}
	defer natsProducer.Close()

	app := &App{
		cfg:     cfg,
		nats:    natsProducer,
		sampler: sampler,
		wasmMgr: wasmMgr,
	}
	app.stats.Uptime = time.Now()

	r := gin.Default()

	r.POST("/v1/spans", app.handleSpan)
	r.POST("/v1/spans/batch", app.handleBatchSpans)

	admin := r.Group("/admin")
	{
		admin.GET("/sampling/rates", app.getSamplingRates)
		admin.GET("/sampling/stats", app.getSamplingStats)
		admin.POST("/sampling/force-full", app.forceFullSample)
		admin.POST("/sampling/rate", app.setSampleRate)
		admin.POST("/sampling/config", app.updateSamplingConfig)
		admin.POST("/sampling/reset", app.resetSampling)

		admin.GET("/wasm/plugins", app.listWasmPlugins)
		admin.POST("/wasm/plugins", app.uploadWasmPlugin)
		admin.DELETE("/wasm/plugins/:id", app.unloadWasmPlugin)
		admin.POST("/wasm/plugins/:id/enable", app.enableWasmPlugin)
		admin.POST("/wasm/plugins/:id/disable", app.disableWasmPlugin)
		admin.POST("/wasm/plugins/:id/config", app.setWasmPluginConfig)
		admin.POST("/wasm/disable", func(c *gin.Context) {
			if wasmMgr != nil {
				wasmMgr.SetEnabled(false)
			}
			c.JSON(200, gin.H{"status": "wasm disabled"})
		})
		admin.POST("/wasm/enable", func(c *gin.Context) {
			if wasmMgr != nil {
				wasmMgr.SetEnabled(true)
			}
			c.JSON(200, gin.H{"status": "wasm enabled"})
		})
	}

	r.GET("/health", app.healthCheck)
	r.GET("/stats", app.getStats)

	port := getEnv("NATS_PRODUCER_PORT", "8085")

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

	go func() {
		log.Printf("[Producer] NATS Trace Producer starting on port %s, NATS: %v", port, natsURLs)
		log.Printf("[Producer] Dynamic sampling: base_rate=%.2f%% error_threshold=%.0f%% always_errors=%v",
			samplingCfg.BaseSampleRate*100, samplingCfg.ErrorRateThreshold*100, samplingCfg.AlwaysSampleError)
		if wasmMgr != nil {
			plugins := wasmMgr.ListPlugins()
			log.Printf("[Producer] Loaded WASM plugins: %d", len(plugins))
			for _, p := range plugins {
				log.Printf("  - %s v%s (%s): %s", p.Name, p.Version, p.Status, p.Description)
			}
		}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[Producer] Shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	natsProducer.WaitForPending(5 * time.Second)
	srv.Shutdown(ctx)
	log.Println("[Producer] Stopped")
}

func (a *App) handleSpan(c *gin.Context) {
	var span model.TraceSpan
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read body"})
		return
	}

	if err := json.Unmarshal(body, &span); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON: " + err.Error()})
		return
	}

	if span.RequestID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing request_id"})
		return
	}

	a.mu.Lock()
	a.stats.MessagesReceived++
	a.mu.Unlock()

	if span.SpanID == "" {
		span.SpanID = uuid.New().String()
	}
	if span.Timestamp.IsZero() {
		span.Timestamp = time.Now()
	}
	if span.StartTime.IsZero() {
		span.StartTime = span.Timestamp
	}
	if span.EndTime.IsZero() {
		span.EndTime = span.StartTime.Add(time.Duration(span.DurationMs) * time.Millisecond)
	}

	publishCtx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	if err := a.nats.PublishSpan(publishCtx, &span); err != nil {
		a.mu.Lock()
		a.stats.MessagesFailed++
		a.mu.Unlock()
		log.Printf("[Producer] Failed to publish span req=%s: %v", span.RequestID, err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Failed to publish: " + err.Error()})
		return
	}

	a.mu.Lock()
	a.stats.MessagesSent++
	a.stats.LastMessageTime = time.Now()
	a.stats.LastRequestID = span.RequestID
	a.mu.Unlock()

	c.JSON(http.StatusAccepted, gin.H{
		"status":     "accepted",
		"request_id": span.RequestID,
		"span_id":    span.SpanID,
		"sampled":    true,
	})
}

func (a *App) handleBatchSpans(c *gin.Context) {
	var spans []model.TraceSpan
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read body"})
		return
	}

	if err := json.Unmarshal(body, &spans); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON: " + err.Error()})
		return
	}

	if len(spans) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Empty batch"})
		return
	}

	a.mu.Lock()
	a.stats.MessagesReceived += int64(len(spans))
	a.mu.Unlock()

	publishCtx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
	defer cancel()

	sent := 0
	failed := 0
	sampled := 0
	wasmFiltered := 0

	spanPtrs := make([]*model.TraceSpan, 0, len(spans))
	for i := range spans {
		s := &spans[i]
		if s.RequestID == "" {
			continue
		}
		if s.SpanID == "" {
			s.SpanID = uuid.New().String()
		}
		if s.Timestamp.IsZero() {
			s.Timestamp = time.Now()
		}
		spanPtrs = append(spanPtrs, s)
	}

	for _, s := range spanPtrs {
		if err := a.nats.PublishSpan(publishCtx, s); err != nil {
			failed++
		}
	}

	natsStats := a.nats.Stats()
	sent = int(natsStats.Published)
	sampled = int(natsStats.SampledOut)
	wasmFiltered = int(natsStats.WasmFiltered)

	a.mu.Lock()
	a.stats.MessagesSent += int64(sent)
	a.stats.MessagesFailed += int64(failed)
	a.stats.MessagesSampled += int64(sampled)
	a.stats.MessagesWasmFiltered += int64(wasmFiltered)
	if len(spans) > 0 {
		a.stats.LastMessageTime = time.Now()
		a.stats.LastRequestID = spans[0].RequestID
	}
	a.mu.Unlock()

	c.JSON(http.StatusAccepted, gin.H{
		"status":           "processed",
		"total_received":   len(spans),
		"published":        sent,
		"sampled_out":      sampled,
		"wasm_filtered":    wasmFiltered,
		"failed":           failed,
	})
}

func (a *App) healthCheck(c *gin.Context) {
	a.mu.Lock()
	stats := a.stats
	a.mu.Unlock()

	natsStatus := "disconnected"
	if a.nats != nil && a.nats.Connected() {
		natsStatus = "connected"
	}

	samplingRates := a.sampler.GetAllRates()

	c.JSON(http.StatusOK, gin.H{
		"service":        "nats-trace-producer",
		"status":         "healthy",
		"nats":           natsStatus,
		"uptime_seconds": int64(time.Since(stats.Uptime).Seconds()),
		"sampling_rates": samplingRates,
		"wasm_enabled":   a.wasmMgr != nil && a.wasmMgr.IsEnabled(),
		"time":           time.Now().UTC(),
	})
}

func (a *App) getStats(c *gin.Context) {
	a.mu.Lock()
	stats := a.stats
	a.mu.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"service": "nats-trace-producer",
		"messages": gin.H{
			"received":        stats.MessagesReceived,
			"sent":            stats.MessagesSent,
			"sampled_out":     stats.MessagesSampled,
			"wasm_filtered":   stats.MessagesWasmFiltered,
			"failed":          stats.MessagesFailed,
			"last_request_id": stats.LastRequestID,
			"last_message":    stats.LastMessageTime,
		},
		"sampling": a.sampler.GetAllStats(),
		"nats":     a.nats.Stats(),
		"uptime_seconds": int64(time.Since(stats.Uptime).Seconds()),
	})
}

func (a *App) getSamplingRates(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"rates": a.sampler.GetAllRates(),
		"config": a.sampler.Config(),
	})
}

func (a *App) getSamplingStats(c *gin.Context) {
	c.JSON(http.StatusOK, a.sampler.GetAllStats())
}

func (a *App) forceFullSample(c *gin.Context) {
	var req struct {
		DurationSeconds int `json:"duration_seconds" binding:"min=0"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		req.DurationSeconds = 60
	}

	dur := time.Duration(req.DurationSeconds) * time.Second
	if dur == 0 {
		dur = 60 * time.Second
	}

	a.sampler.ForceFullSample(dur)

	c.JSON(http.StatusOK, gin.H{
		"status":   "full_sample_enabled",
		"duration": dur.String(),
	})
}

func (a *App) setSampleRate(c *gin.Context) {
	var req struct {
		Service string  `json:"service"`
		Rate    float64 `json:"rate" binding:"required,min=0,max=1"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Service == "" || req.Service == "_global" {
		a.sampler.SetGlobalRate(req.Rate)
	} else {
		a.sampler.SetServiceRate(req.Service, req.Rate)
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "rate_updated",
		"service": req.Service,
		"rate":    req.Rate,
		"rates":   a.sampler.GetAllRates(),
	})
}

func (a *App) updateSamplingConfig(c *gin.Context) {
	var req struct {
		BaseSampleRate     *float64 `json:"base_sample_rate"`
		MinSampleRate      *float64 `json:"min_sample_rate"`
		MaxSampleRate      *float64 `json:"max_sample_rate"`
		ErrorRateThreshold *float64 `json:"error_rate_threshold"`
		HighErrorBoost     *float64 `json:"high_error_boost"`
		AlwaysSampleError  *bool    `json:"always_sample_error"`
		AdjustIntervalMs   *int64   `json:"adjust_interval_ms"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cfg := a.sampler.Config()
	if req.BaseSampleRate != nil {
		cfg.BaseSampleRate = *req.BaseSampleRate
	}
	if req.MinSampleRate != nil {
		cfg.MinSampleRate = *req.MinSampleRate
	}
	if req.MaxSampleRate != nil {
		cfg.MaxSampleRate = *req.MaxSampleRate
	}
	if req.ErrorRateThreshold != nil {
		cfg.ErrorRateThreshold = *req.ErrorRateThreshold
	}
	if req.HighErrorBoost != nil {
		cfg.HighErrorBoost = *req.HighErrorBoost
	}
	if req.AlwaysSampleError != nil {
		cfg.AlwaysSampleError = *req.AlwaysSampleError
	}
	if req.AdjustIntervalMs != nil {
		cfg.AdjustInterval = time.Duration(*req.AdjustIntervalMs) * time.Millisecond
	}

	a.sampler.UpdateConfig(cfg)

	c.JSON(http.StatusOK, gin.H{
		"status": "config_updated",
		"config": a.sampler.Config(),
	})
}

func (a *App) resetSampling(c *gin.Context) {
	a.sampler.Reset()
	c.JSON(http.StatusOK, gin.H{
		"status": "sampling_reset",
		"rates":  a.sampler.GetAllRates(),
	})
}

func (a *App) listWasmPlugins(c *gin.Context) {
	if a.wasmMgr == nil {
		c.JSON(http.StatusOK, gin.H{"plugins": []interface{}{}, "enabled": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"plugins": a.wasmMgr.ListPlugins(),
		"enabled": a.wasmMgr.IsEnabled(),
	})
}

func (a *App) uploadWasmPlugin(c *gin.Context) {
	if a.wasmMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "WASM system not available"})
		return
	}

	name := c.PostForm("name")
	description := c.PostForm("description")
	author := c.PostForm("author")
	version := c.PostForm("version")
	config := c.PostForm("config")
	priorityStr := c.PostForm("priority")
	priority, _ := strconv.Atoi(priorityStr)

	if name == "" {
		name = "plugin-" + uuid.New().String()[:8]
	}
	if version == "" {
		version = "0.1.0"
	}

	file, header, err := c.Request.FormFile("wasm")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing wasm file: " + err.Error()})
		return
	}
	defer file.Close()

	wasmBytes, err := io.ReadAll(file)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read wasm file"})
		return
	}

	if len(wasmBytes) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Empty wasm file"})
		return
	}

	log.Printf("[WASM] Uploading plugin: name=%s version=%s size=%d filename=%s",
		name, version, len(wasmBytes), header.Filename)

	plugin, err := a.wasmMgr.UploadPlugin(name, description, author, version, wasmBytes, config, priority)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to load plugin: " + err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"status":  "plugin_loaded",
		"plugin":  plugin,
	})
}

func (a *App) unloadWasmPlugin(c *gin.Context) {
	if a.wasmMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "WASM system not available"})
		return
	}
	pluginID := c.Param("id")
	if err := a.wasmMgr.UnloadPlugin(pluginID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "plugin_unloaded", "id": pluginID})
}

func (a *App) enableWasmPlugin(c *gin.Context) {
	if a.wasmMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "WASM system not available"})
		return
	}
	pluginID := c.Param("id")
	if err := a.wasmMgr.EnablePlugin(pluginID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "plugin_enabled", "id": pluginID})
}

func (a *App) disableWasmPlugin(c *gin.Context) {
	if a.wasmMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "WASM system not available"})
		return
	}
	pluginID := c.Param("id")
	if err := a.wasmMgr.DisablePlugin(pluginID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "plugin_disabled", "id": pluginID})
}

func (a *App) setWasmPluginConfig(c *gin.Context) {
	if a.wasmMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "WASM system not available"})
		return
	}
	pluginID := c.Param("id")

	var req struct {
		Config string `json:"config"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := a.wasmMgr.SetPluginConfig(pluginID, req.Config); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "config_updated", "id": pluginID})
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
