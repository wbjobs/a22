package wasm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	wasmtime "github.com/bytecodealliance/wasmtime-go/v20"

	"ebpf-serverless-tracing/internal/model"
)

type FilterResult struct {
	Keep    bool   `json:"keep"`
	Reason  string `json:"reason,omitempty"`
	Modifed bool   `json:"modified"`
}

type PluginStatus string

const (
	PluginStatusActive  PluginStatus = "active"
	PluginStatusPaused  PluginStatus = "paused"
	PluginStatusError   PluginStatus = "error"
	PluginStatusLoading PluginStatus = "loading"
)

type WasmPlugin struct {
	ID          string       `json:"id"`
	Name        string       `json:"name"`
	Description string       `json:"description"`
	Author      string       `json:"author"`
	Version     string       `json:"version"`
	Hash        string       `json:"hash"`
	FilePath    string       `json:"-"`
	Status      PluginStatus `json:"status"`
	Priority    int          `json:"priority"`
	Config      string       `json:"config,omitempty"`
	LoadedAt    time.Time    `json:"loaded_at"`
	LastUsedAt  time.Time    `json:"last_used_at"`
	TotalCalls  int64        `json:"total_calls"`
	PassCount   int64        `json:"pass_count"`
	DropCount   int64        `json:"drop_count"`
	AvgExecNs   int64        `json:"avg_exec_ns"`
}

type pluginInstance struct {
	plugin   *WasmPlugin
	engine   *wasmtime.Engine
	store    *wasmtime.Store
	module   *wasmtime.Module
	instance *wasmtime.Instance
	memory   *wasmtime.Memory
	malloc   *wasmtime.Func
	free     *wasmtime.Func
	filter   *wasmtime.Func
	init     *wasmtime.Func
	dealloc  *wasmtime.Func
}

type WasmPluginManager struct {
	mu          sync.RWMutex
	plugins     map[string]*pluginInstance
	order       []string
	pluginDir   string
	engine      *wasmtime.Engine
	config      *wasmtime.Config
	enabled     bool
	maxMemoryMB int32
}

func NewWasmPluginManager(pluginDir string) (*WasmPluginManager, error) {
	if pluginDir == "" {
		pluginDir = "./plugins/wasm"
	}

	if err := os.MkdirAll(pluginDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create plugin dir: %w", err)
	}

	cfg := wasmtime.NewConfig()
	cfg.SetConsumeFuel(false)
	cfg.SetMaxWasmStack(1024)

	engine := wasmtime.NewEngineWithConfig(cfg)

	mgr := &WasmPluginManager{
		plugins:     make(map[string]*pluginInstance),
		order:       make([]string, 0),
		pluginDir:   pluginDir,
		engine:      engine,
		config:      cfg,
		enabled:     true,
		maxMemoryMB: 128,
	}

	if err := mgr.loadExistingPlugins(); err != nil {
		log.Printf("[WASM] Warning: failed to load existing plugins: %v", err)
	}

	return mgr, nil
}

func (m *WasmPluginManager) loadExistingPlugins() error {
	files, err := filepath.Glob(filepath.Join(m.pluginDir, "*.wasm"))
	if err != nil {
		return err
	}
	for _, f := range files {
		pluginID := filepath.Base(f)[:len(filepath.Base(f))-5]
		if err := m.loadPluginFromFile(f, pluginID, "", false); err != nil {
			log.Printf("[WASM] Failed to load %s: %v", f, err)
		}
	}
	return nil
}

func (m *WasmPluginManager) UploadPlugin(name, description, author, version, wasmBinary []byte, config string, priority int) (*WasmPlugin, error) {
	if len(wasmBinary) == 0 {
		return nil, errors.New("empty wasm binary")
	}

	hashBytes := sha256.Sum256(wasmBinary)
	hash := hex.EncodeToString(hashBytes[:])

	pluginID := fmt.Sprintf("%s-%s-%s", name, version, hash[:8])
	filePath := filepath.Join(m.pluginDir, pluginID+".wasm")

	if err := os.WriteFile(filePath, wasmBinary, 0644); err != nil {
		return nil, fmt.Errorf("failed to write plugin file: %w", err)
	}

	if err := m.loadPluginFromFile(filePath, pluginID, config, true); err != nil {
		os.Remove(filePath)
		return nil, fmt.Errorf("failed to load plugin: %w", err)
	}

	m.mu.RLock()
	inst := m.plugins[pluginID]
	m.mu.RUnlock()
	if inst != nil {
		inst.plugin.Description = description
		inst.plugin.Author = author
		inst.plugin.Name = name
		inst.plugin.Version = version
		inst.plugin.Config = config
		inst.plugin.Priority = priority
		return inst.plugin, nil
	}

	return nil, errors.New("plugin loaded but not found")
}

func (m *WasmPluginManager) loadPluginFromFile(filePath, pluginID, config string, active bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.plugins[pluginID]; exists {
		return fmt.Errorf("plugin %s already loaded", pluginID)
	}

	module, err := wasmtime.NewModuleFromFile(m.engine, filePath)
	if err != nil {
		return fmt.Errorf("failed to compile module: %w", err)
	}

	store := wasmtime.NewStore(m.engine)

	linker := wasmtime.NewLinker(m.engine)
	if err := linker.Define(store, "env", "log", wasmtime.WrapFunc(store, m.hostLog(pluginID))); err != nil {
		return fmt.Errorf("define log: %w", err)
	}
	if err := linker.Define(store, "env", "get_config", wasmtime.WrapFunc(store, m.hostGetConfig(pluginID))); err != nil {
		return fmt.Errorf("define get_config: %w", err)
	}
	if err := linker.Define(store, "env", "set_result", wasmtime.WrapFunc(store, m.hostSetResult(pluginID))); err != nil {
		return fmt.Errorf("define set_result: %w", err)
	}

	instance, err := linker.Instantiate(store, module)
	if err != nil {
		return fmt.Errorf("failed to instantiate: %w", err)
	}

	malloc := instance.GetFunc(store, "malloc")
	free := instance.GetFunc(store, "free")
	filter := instance.GetFunc(store, "filter_span")
	init := instance.GetFunc(store, "init")
	dealloc := instance.GetFunc(store, "dealloc")

	if filter == nil {
		return fmt.Errorf("plugin missing required export: filter_span")
	}

	memory := instance.GetMemory(store, "memory")

	plugin := &WasmPlugin{
		ID:          pluginID,
		Name:        pluginID,
		FilePath:    filePath,
		Status:      PluginStatusLoading,
		Priority:    0,
		Config:      config,
		LoadedAt:    time.Now(),
	}

	inst := &pluginInstance{
		plugin:   plugin,
		engine:   m.engine,
		store:    store,
		module:   module,
		instance: instance,
		memory:   memory,
		malloc:   malloc,
		free:     free,
		filter:   filter,
		init:     init,
		dealloc:  dealloc,
	}

	if init != nil {
		_, err := init.Call(store)
		if err != nil {
			log.Printf("[WASM:%s] init() returned error: %v", pluginID, err)
			plugin.Status = PluginStatusError
		}
	}

	if active {
		plugin.Status = PluginStatusActive
	}

	m.plugins[pluginID] = inst

	inserted := false
	for i, id := range m.order {
		if m.plugins[id] != nil && plugin.Priority > m.plugins[id].plugin.Priority {
			m.order = append(m.order[:i+1], m.order[i:]...)
			m.order[i] = pluginID
			inserted = true
			break
		}
	}
	if !inserted {
		m.order = append(m.order, pluginID)
	}

	log.Printf("[WASM] Plugin loaded: %s (status=%s)", pluginID, plugin.Status)
	return nil
}

func (m *WasmPluginManager) UnloadPlugin(pluginID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, exists := m.plugins[pluginID]
	if !exists {
		return fmt.Errorf("plugin %s not found", pluginID)
	}

	if inst.dealloc != nil {
		_, _ = inst.dealloc.Call(inst.store)
	}

	delete(m.plugins, pluginID)

	for i, id := range m.order {
		if id == pluginID {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}

	if inst.plugin.FilePath != "" {
		os.Remove(inst.plugin.FilePath)
	}

	log.Printf("[WASM] Plugin unloaded: %s", pluginID)
	return nil
}

func (m *WasmPluginManager) EnablePlugin(pluginID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, exists := m.plugins[pluginID]
	if !exists {
		return fmt.Errorf("plugin %s not found", pluginID)
	}
	inst.plugin.Status = PluginStatusActive
	log.Printf("[WASM] Plugin enabled: %s", pluginID)
	return nil
}

func (m *WasmPluginManager) DisablePlugin(pluginID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, exists := m.plugins[pluginID]
	if !exists {
		return fmt.Errorf("plugin %s not found", pluginID)
	}
	inst.plugin.Status = PluginStatusPaused
	log.Printf("[WASM] Plugin disabled: %s", pluginID)
	return nil
}

func (m *WasmPluginManager) SetPluginConfig(pluginID, config string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, exists := m.plugins[pluginID]
	if !exists {
		return fmt.Errorf("plugin %s not found", pluginID)
	}
	inst.plugin.Config = config
	return nil
}

func (m *WasmPluginManager) ListPlugins() []*WasmPlugin {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*WasmPlugin, 0, len(m.order))
	for _, id := range m.order {
		if inst, ok := m.plugins[id]; ok {
			p := *inst.plugin
			result = append(result, &p)
		}
	}
	return result
}

func (m *WasmPluginManager) RunFilters(span *model.TraceSpan) (*FilterResult, []error) {
	if !m.enabled {
		return &FilterResult{Keep: true}, nil
	}

	m.mu.RLock()
	order := make([]string, len(m.order))
	copy(order, m.order)
	m.mu.RUnlock()

	errors := make([]error, 0)
	keep := true
	var finalReason string

	for _, pluginID := range order {
		m.mu.RLock()
		inst, ok := m.plugins[pluginID]
		m.mu.RUnlock()
		if !ok || inst.plugin.Status != PluginStatusActive {
			continue
		}

		result, err := m.runSingleFilter(inst, span)
		if err != nil {
			errors = append(errors, fmt.Errorf("%s: %w", pluginID, err))
			continue
		}

		inst.plugin.TotalCalls++
		inst.plugin.LastUsedAt = time.Now()
		if result.Keep {
			inst.plugin.PassCount++
		} else {
			inst.plugin.DropCount++
			keep = false
			finalReason = fmt.Sprintf("%s: %s", pluginID, result.Reason)
			break
		}
	}

	return &FilterResult{
		Keep:   keep,
		Reason: finalReason,
	}, errors
}

func (m *WasmPluginManager) runSingleFilter(inst *pluginInstance, span *model.TraceSpan) (*FilterResult, error) {
	start := time.Now()

	spanBytes, err := json.Marshal(span)
	if err != nil {
		return nil, err
	}

	spanLen := len(spanBytes)
	if inst.memory == nil || inst.malloc == nil {
		return m.runFilterViaJSON(inst, string(spanBytes))
	}

	size := spanLen + 1024
	resultPtr, err := inst.malloc.Call(inst.store, size)
	if err != nil {
		return m.runFilterViaJSON(inst, string(spanBytes))
	}
	ptrVal := resultPtr.(int32)
	defer func() {
		if inst.free != nil {
			inst.free.Call(inst.store, ptrVal)
		}
	}()

	mem := inst.memory.UnsafeData(inst.store)
	if int(ptrVal)+spanLen < len(mem) {
		copy(mem[ptrVal:], spanBytes)
	} else {
		return m.runFilterViaJSON(inst, string(spanBytes))
	}

	res, err := inst.filter.Call(inst.store, ptrVal, int32(spanLen))
	if err != nil {
		return nil, fmt.Errorf("filter call error: %w", err)
	}

	resultInt, ok := res.(int32)
	if !ok {
		return &FilterResult{Keep: true}, nil
	}

	execTime := time.Since(start).Nanoseconds()
	inst.plugin.AvgExecNs = (inst.plugin.AvgExecNs*int64(inst.plugin.TotalCalls) + execTime) / (inst.plugin.TotalCalls + 1)

	if resultInt > 0 {
		return &FilterResult{Keep: true}, nil
	}
	return &FilterResult{Keep: false, Reason: "wasm_filter"}, nil
}

func (m *WasmPluginManager) runFilterViaJSON(inst *pluginInstance, spanJSON string) (*FilterResult, error) {
	res, err := inst.filter.Call(inst.store, spanJSON)
	if err != nil {
		return nil, err
	}

	switch v := res.(type) {
	case int32:
		return &FilterResult{Keep: v > 0}, nil
	case int64:
		return &FilterResult{Keep: v > 0}, nil
	case bool:
		return &FilterResult{Keep: v}, nil
	default:
		return &FilterResult{Keep: true}, nil
	}
}

func (m *WasmPluginManager) hostLog(pluginID string) func(int32, int32) {
	return func(ptr, len int32) {
		m.mu.RLock()
		inst := m.plugins[pluginID]
		m.mu.RUnlock()
		if inst == nil || inst.memory == nil {
			return
		}
		mem := inst.memory.UnsafeData(inst.store)
		if int(ptr)+int(len) >= len(mem) {
			return
		}
		msg := string(mem[ptr : ptr+len])
		log.Printf("[WASM:%s] %s", pluginID, msg)
	}
}

func (m *WasmPluginManager) hostGetConfig(pluginID string) func(int32) int32 {
	return func(ptr int32) int32 {
		m.mu.RLock()
		inst := m.plugins[pluginID]
		m.mu.RUnlock()
		if inst == nil || inst.memory == nil || inst.malloc == nil {
			return 0
		}
		config := inst.plugin.Config
		if config == "" {
			return 0
		}
		configBytes := []byte(config)
		res, err := inst.malloc.Call(inst.store, int32(len(configBytes)+1))
		if err != nil {
			return 0
		}
		configPtr := res.(int32)
		mem := inst.memory.UnsafeData(inst.store)
		if int(configPtr)+len(configBytes) < len(mem) {
			copy(mem[configPtr:], configBytes)
			mem[configPtr+int32(len(configBytes))] = 0
		}
		return configPtr
	}
}

func (m *WasmPluginManager) hostSetResult(pluginID string) func(int32, int32, int32) {
	return func(keep, reasonPtr, reasonLen int32) {
	}
}

func (m *WasmPluginManager) SetEnabled(enabled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enabled = enabled
	log.Printf("[WASM] Plugin system enabled=%v", enabled)
}

func (m *WasmPluginManager) IsEnabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.enabled
}

func (m *WasmPluginManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, inst := range m.plugins {
		if inst.dealloc != nil {
			inst.dealloc.Call(inst.store)
		}
		delete(m.plugins, id)
	}
	m.order = nil
}

type filterContext struct {
	spanJSON []byte
	result   bool
	reason   string
}
