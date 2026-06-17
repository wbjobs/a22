package sampling

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"ebpf-serverless-tracing/internal/model"
)

type SamplingStrategy int

const (
	StrategyDynamic      SamplingStrategy = iota
	StrategyFullSample
	StrategyAdaptiveRate
)

type SamplingConfig struct {
	Strategy           SamplingStrategy
	BaseSampleRate     float64
	MinSampleRate      float64
	MaxSampleRate      float64
	ErrorRateThreshold float64
	SlidingWindow      time.Duration
	AdjustInterval     time.Duration
	HighErrorBoost     float64
	AlwaysSampleError  bool
}

type serviceStats struct {
	totalRequests  atomic.Int64
	errorRequests  atomic.Int64
	windowStart    atomic.Int64
}

type DynamicSampler struct {
	cfg           SamplingConfig
	stats         sync.Map
	rates         sync.Map
	mu            sync.RWMutex
	globalRate    atomic.Value
	rngPool       sync.Pool
	adjustTicker  *time.Ticker
	stopChan      chan struct{}
}

var defaultConfig = SamplingConfig{
	Strategy:           StrategyDynamic,
	BaseSampleRate:     0.01,
	MinSampleRate:      0.01,
	MaxSampleRate:      1.0,
	ErrorRateThreshold: 0.05,
	SlidingWindow:      1 * time.Minute,
	AdjustInterval:     5 * time.Second,
	HighErrorBoost:     10.0,
	AlwaysSampleError:  true,
}

func NewDynamicSampler(cfg *SamplingConfig) *DynamicSampler {
	if cfg == nil {
		cfg = &defaultConfig
	}

	s := &DynamicSampler{
		cfg:      *cfg,
		stopChan: make(chan struct{}),
		rngPool: sync.Pool{
			New: func() interface{} {
				return rand.New(rand.NewSource(time.Now().UnixNano()))
			},
		},
	}

	s.globalRate.Store(cfg.BaseSampleRate)

	go s.adjustmentLoop()

	return s
}

func (s *DynamicSampler) DefaultConfig() SamplingConfig {
	return defaultConfig
}

func (s *DynamicSampler) ShouldSample(span *model.TraceSpan) bool {
	if span == nil {
		return true
	}

	if s.cfg.AlwaysSampleError && span.StatusCode >= 400 && span.StatusCode != 0 {
		return true
	}

	serviceName := span.ServiceName
	if serviceName == "" {
		serviceName = span.FunctionName
	}
	if serviceName == "" {
		serviceName = "global"
	}

	st, _ := s.stats.LoadOrStore(serviceName, &serviceStats{})
	stats := st.(*serviceStats)
	stats.totalRequests.Add(1)

	if span.StatusCode >= 400 && span.StatusCode != 0 {
		stats.errorRequests.Add(1)
	}

	var sampleRate float64
	if sr, ok := s.rates.Load(serviceName); ok {
		sampleRate = sr.(float64)
	} else {
		sampleRate = s.globalRate.Load().(float64)
	}

	if sampleRate >= 1.0 {
		return true
	}
	if sampleRate <= 0 {
		return false
	}

	rng := s.rngPool.Get().(*rand.Rand)
	defer s.rngPool.Put(rng)
	return rng.Float64() < sampleRate
}

func (s *DynamicSampler) GetServiceRate(serviceName string) float64 {
	if rate, ok := s.rates.Load(serviceName); ok {
		return rate.(float64)
	}
	return s.globalRate.Load().(float64)
}

func (s *DynamicSampler) SetServiceRate(serviceName string, rate float64) {
	clamped := clampRate(rate, s.cfg.MinSampleRate, s.cfg.MaxSampleRate)
	s.rates.Store(serviceName, clamped)
}

func (s *DynamicSampler) GetGlobalRate() float64 {
	return s.globalRate.Load().(float64)
}

func (s *DynamicSampler) SetGlobalRate(rate float64) {
	clamped := clampRate(rate, s.cfg.MinSampleRate, s.cfg.MaxSampleRate)
	s.globalRate.Store(clamped)
}

func (s *DynamicSampler) GetAllRates() map[string]float64 {
	result := make(map[string]float64)
	result["_global"] = s.globalRate.Load().(float64)
	s.rates.Range(func(k, v interface{}) bool {
		result[k.(string)] = v.(float64)
		return true
	})
	return result
}

func (s *DynamicSampler) GetAllStats() map[string]map[string]interface{} {
	result := make(map[string]map[string]interface{})

	var globalTotal, globalError int64
	s.stats.Range(func(k, v interface{}) bool {
		serviceName := k.(string)
		st := v.(*serviceStats)
		total := st.totalRequests.Load()
		errors := st.errorRequests.Load()
		globalTotal += total
		globalError += errors

		errorRate := 0.0
		if total > 0 {
			errorRate = float64(errors) / float64(total)
		}

		rate, _ := s.rates.Load(serviceName)
		sr := 0.0
		if rate != nil {
			sr = rate.(float64)
		}

		result[serviceName] = map[string]interface{}{
			"total_requests": total,
			"error_requests": errors,
			"error_rate":     errorRate,
			"sample_rate":    sr,
		}
		return true
	})

	globalErrRate := 0.0
	if globalTotal > 0 {
		globalErrRate = float64(globalError) / float64(globalTotal)
	}
	result["_global"] = map[string]interface{}{
		"total_requests": globalTotal,
		"error_requests": globalError,
		"error_rate":     globalErrRate,
		"sample_rate":    s.globalRate.Load().(float64),
	}

	return result
}

func (s *DynamicSampler) ForceFullSample(duration time.Duration) {
	s.globalRate.Store(1.0)
	s.rates.Range(func(k, _ interface{}) bool {
		s.rates.Store(k, 1.0)
		return true
	})

	if duration > 0 {
		go func() {
			time.Sleep(duration)
			s.resetRates()
		}()
	}
}

func (s *DynamicSampler) Reset() {
	s.resetRates()
	s.stats.Range(func(k, v interface{}) bool {
		st := v.(*serviceStats)
		st.totalRequests.Store(0)
		st.errorRequests.Store(0)
		return true
	})
}

func (s *DynamicSampler) resetRates() {
	s.globalRate.Store(s.cfg.BaseSampleRate)
	s.rates.Range(func(k, _ interface{}) bool {
		s.rates.Store(k, s.cfg.BaseSampleRate)
		return true
	})
}

func (s *DynamicSampler) adjustmentLoop() {
	s.adjustTicker = time.NewTicker(s.cfg.AdjustInterval)
	defer s.adjustTicker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-s.adjustTicker.C:
			s.adjustAllRates()
		}
	}
}

func (s *DynamicSampler) adjustAllRates() {
	var globalTotal, globalError int64

	s.stats.Range(func(k, v interface{}) bool {
		serviceName := k.(string)
		st := v.(*serviceStats)
		total := st.totalRequests.Load()
		errors := st.errorRequests.Load()
		globalTotal += total
		globalError += errors

		newRate := s.calculateNewRate(total, errors, serviceName)
		s.rates.Store(serviceName, newRate)

		if time.Since(time.Unix(st.windowStart.Load(), 0)) > s.cfg.SlidingWindow {
			st.totalRequests.Store(0)
			st.errorRequests.Store(0)
			st.windowStart.Store(time.Now().Unix())
		}
		return true
	})

	globalRate := s.calculateNewRate(globalTotal, globalError, "_global")
	s.globalRate.Store(globalRate)
}

func (s *DynamicSampler) calculateNewRate(total, errors int64, serviceName string) float64 {
	if total < 10 {
		return s.cfg.BaseSampleRate
	}

	errorRate := float64(errors) / float64(total)

	var currentRate float64
	if serviceName == "_global" {
		currentRate = s.globalRate.Load().(float64)
	} else {
		if r, ok := s.rates.Load(serviceName); ok {
			currentRate = r.(float64)
		} else {
			currentRate = s.cfg.BaseSampleRate
		}
	}

	var newRate float64
	if errorRate >= s.cfg.ErrorRateThreshold {
		boost := 1.0 + (errorRate/s.cfg.ErrorRateThreshold-1)*s.cfg.HighErrorBoost
		if boost > 50 {
			boost = 50
		}
		newRate = currentRate * boost
	} else if errorRate < s.cfg.ErrorRateThreshold*0.5 {
		newRate = currentRate * 0.9
	} else {
		return currentRate
	}

	return clampRate(newRate, s.cfg.MinSampleRate, s.cfg.MaxSampleRate)
}

func clampRate(rate, min, max float64) float64 {
	if rate < min {
		return min
	}
	if rate > max {
		return max
	}
	return rate
}

func (s *DynamicSampler) Stop() {
	close(s.stopChan)
	if s.adjustTicker != nil {
		s.adjustTicker.Stop()
	}
}

func (s *DynamicSampler) Config() SamplingConfig {
	return s.cfg
}

func (s *DynamicSampler) UpdateConfig(cfg SamplingConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cfg.MinSampleRate > 0 {
		s.cfg.MinSampleRate = cfg.MinSampleRate
	}
	if cfg.MaxSampleRate > 0 && cfg.MaxSampleRate >= s.cfg.MinSampleRate {
		s.cfg.MaxSampleRate = cfg.MaxSampleRate
	}
	if cfg.BaseSampleRate >= s.cfg.MinSampleRate && cfg.BaseSampleRate <= s.cfg.MaxSampleRate {
		s.cfg.BaseSampleRate = cfg.BaseSampleRate
	}
	if cfg.ErrorRateThreshold > 0 && cfg.ErrorRateThreshold <= 1 {
		s.cfg.ErrorRateThreshold = cfg.ErrorRateThreshold
	}
	if cfg.HighErrorBoost > 0 {
		s.cfg.HighErrorBoost = cfg.HighErrorBoost
	}
	if cfg.AdjustInterval > 0 {
		s.cfg.AdjustInterval = cfg.AdjustInterval
		if s.adjustTicker != nil {
			s.adjustTicker.Reset(cfg.AdjustInterval)
		}
	}
	s.cfg.AlwaysSampleError = cfg.AlwaysSampleError
}
