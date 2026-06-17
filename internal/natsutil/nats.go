package natsutil

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"ebpf-serverless-tracing/internal/model"
	"ebpf-serverless-tracing/internal/sampling"
	"ebpf-serverless-tracing/internal/wasm"
)

const (
	DefaultStreamName     = "TRACES"
	DefaultSubjectPrefix  = "trace.spans"
	DefaultConsumerName   = "es-consumer"
	DefaultMaxMsgs        = 100000
	DefaultMaxBytes       = 1 * 1024 * 1024 * 1024
	DefaultReplicas       = 1
	DefaultRetentionHours = 168
)

type NATSConfig struct {
	URLs           []string
	StreamName     string
	SubjectPrefix  string
	ConsumerName   string
	MaxMsgs        int64
	MaxBytes       int64
	Replicas       int
	RetentionHours int
}

func DefaultConfig() *NATSConfig {
	return &NATSConfig{
		URLs:           []string{"nats://nats:4222"},
		StreamName:     DefaultStreamName,
		SubjectPrefix:  DefaultSubjectPrefix,
		ConsumerName:   DefaultConsumerName,
		MaxMsgs:        DefaultMaxMsgs,
		MaxBytes:       DefaultMaxBytes,
		Replicas:       DefaultReplicas,
		RetentionHours: DefaultRetentionHours,
	}
}

type NATSTraceProducer struct {
	cfg      *NATSConfig
	nc       *nats.Conn
	js       nats.JetStreamContext
	mu       sync.RWMutex
	connected bool
	stats    ProducerStats
	sampler  *sampling.DynamicSampler
	wasmMgr  *wasm.WasmPluginManager
}

type ProducerStats struct {
	Published   int64
	SampledOut  int64
	WasmFiltered int64
	Errors      int64
	LastPublish time.Time
	Batches     int64
	BytesSent   int64
}

func NewNATSProducer(cfg *NATSConfig, sampler *sampling.DynamicSampler, wasmMgr *wasm.WasmPluginManager) (*NATSTraceProducer, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	opts := []nats.Option{
		nats.Name("trace-producer"),
		nats.MaxReconnects(60),
		nats.ReconnectWait(2 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("[NATS Producer] Disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("[NATS Producer] Reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Printf("[NATS Producer] Connection closed")
		}),
		nats.PingInterval(20 * time.Second),
		nats.MaxPingsOutstanding(3),
	}

	nc, err := nats.Connect(strings.Join(cfg.URLs, ","), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(4096))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	p := &NATSTraceProducer{
		cfg:      cfg,
		nc:       nc,
		js:       js,
		connected: true,
		sampler:  sampler,
		wasmMgr:  wasmMgr,
	}

	if err := p.ensureStream(); err != nil {
		log.Printf("[NATS Producer] Warning: stream creation failed (will retry): %v", err)
	}

	log.Printf("[NATS Producer] Connected to %s, stream=%s", cfg.URLs, cfg.StreamName)
	return p, nil
}

func (p *NATSTraceProducer) ensureStream() error {
	subjects := []string{
		fmt.Sprintf("%s.>", p.cfg.SubjectPrefix),
	}

	streamConfig := &nats.StreamConfig{
		Name:        p.cfg.StreamName,
		Subjects:    subjects,
		Retention:   nats.LimitsPolicy,
		MaxMsgs:     p.cfg.MaxMsgs,
		MaxBytes:    p.cfg.MaxBytes,
		MaxAge:      time.Duration(p.cfg.RetentionHours) * time.Hour,
		Storage:     nats.FileStorage,
		Replicas:    p.cfg.Replicas,
		Discard:     nats.DiscardOld,
		Duplicates:  2 * time.Minute,
		AllowDirect: true,
	}

	si, err := p.js.StreamInfo(p.cfg.StreamName)
	if err == nil && si != nil {
		_, err := p.js.UpdateStream(streamConfig)
		return err
	}

	_, err = p.js.AddStream(streamConfig)
	if err != nil {
		if err.Error() == "stream name already in use" {
			_, err = p.js.UpdateStream(streamConfig)
		}
	}
	return err
}

func (p *NATSTraceProducer) PublishSpan(ctx context.Context, span *model.TraceSpan) error {
	if span == nil {
		return nil
	}

	if p.sampler != nil {
		if !p.sampler.ShouldSample(span) {
			p.mu.Lock()
			p.stats.SampledOut++
			p.mu.Unlock()
			return nil
		}
	}

	if p.wasmMgr != nil && p.wasmMgr.IsEnabled() {
		result, errs := p.wasmMgr.RunFilters(span)
		if len(errs) > 0 {
			for _, e := range errs {
				log.Printf("[NATS Producer] WASM filter error: %v", e)
			}
		}
		if result != nil && !result.Keep {
			p.mu.Lock()
			p.stats.WasmFiltered++
			p.mu.Unlock()
			return nil
		}
	}

	data, err := json.Marshal(span)
	if err != nil {
		p.mu.Lock()
		p.stats.Errors++
		p.mu.Unlock()
		return fmt.Errorf("marshal span: %w", err)
	}

	subject := p.buildSubject(span)

	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{},
	}
	msg.Header.Set("X-Request-ID", span.RequestID)
	msg.Header.Set("X-Trace-ID", span.TraceID)
	msg.Header.Set("X-Function", span.FunctionName)
	msg.Header.Set("X-Status-Code", fmt.Sprintf("%d", span.StatusCode))

	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			msg.Header.Set("Nats-Expected-Last-Sequence", "")
			_ = deadline
		}
	}

	paf, err := p.js.PublishMsgAsync(msg)
	if err != nil {
		p.mu.Lock()
		p.stats.Errors++
		p.mu.Unlock()
		return fmt.Errorf("publish: %w", err)
	}

	p.mu.Lock()
	p.stats.Published++
	p.stats.LastPublish = time.Now()
	p.stats.BytesSent += int64(len(data))
	p.mu.Unlock()

	go func() {
		select {
		case <-paf.Ok():
		case err := <-paf.Err():
			if err != nil {
				p.mu.Lock()
				p.stats.Errors++
				p.mu.Unlock()
				log.Printf("[NATS Producer] Async publish error: %v", err)
			}
		case <-time.After(5 * time.Second):
			p.mu.Lock()
			p.stats.Errors++
			p.mu.Unlock()
		}
	}()

	return nil
}

func (p *NATSTraceProducer) PublishBatch(ctx context.Context, spans []*model.TraceSpan) (int, int, error) {
	published := 0
	filtered := 0

	for _, span := range spans {
		if err := p.PublishSpan(ctx, span); err != nil {
			log.Printf("[NATS Producer] Batch publish error: %v", err)
			continue
		}
		if p.lastPublished() {
			published++
		} else {
			filtered++
		}
	}

	p.mu.Lock()
	p.stats.Batches++
	p.mu.Unlock()

	return published, filtered, nil
}

func (p *NATSTraceProducer) lastPublished() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return true
}

func (p *NATSTraceProducer) buildSubject(span *model.TraceSpan) string {
	service := span.ServiceName
	if service == "" {
		service = span.FunctionName
	}
	if service == "" {
		service = "unknown"
	}

	status := "ok"
	if span.StatusCode >= 500 {
		status = "error"
	} else if span.StatusCode >= 400 {
		status = "client_error"
	}

	return fmt.Sprintf("%s.%s.%s", p.cfg.SubjectPrefix, service, status)
}

func (p *NATSTraceProducer) Stats() ProducerStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stats
}

func (p *NATSTraceProducer) Drain() {
	if p.nc != nil {
		p.nc.Drain()
	}
}

func (p *NATSTraceProducer) Close() {
	if p.nc != nil {
		p.nc.Close()
	}
}

func (p *NATSTraceProducer) Connected() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.connected && p.nc != nil && p.nc.IsConnected()
}

func (p *NATSTraceProducer) WaitForPending(d time.Duration) error {
	select {
	case <-p.js.PublishAsyncComplete():
		return nil
	case <-time.After(d):
		return fmt.Errorf("timeout waiting for pending publishes")
	}
}

type NATSTraceConsumer struct {
	cfg           *NATSConfig
	nc            *nats.Conn
	js            nats.JetStreamContext
	sub           *nats.Subscription
	mu            sync.RWMutex
	connected     bool
	stats         ConsumerStats
	handler       SpanHandler
}

type ConsumerStats struct {
	Consumed  int64
	Processed int64
	Errors    int64
	Redelivered int64
	Pending   int64
	LastMsg   time.Time
	Batches   int64
}

type SpanHandler func(ctx context.Context, span *model.TraceSpan) error

func NewNATSConsumer(cfg *NATSConfig, handler SpanHandler) (*NATSTraceConsumer, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	if handler == nil {
		return nil, fmt.Errorf("handler is required")
	}

	opts := []nats.Option{
		nats.Name("trace-consumer"),
		nats.MaxReconnects(60),
		nats.ReconnectWait(2 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("[NATS Consumer] Disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("[NATS Consumer] Reconnected to %s", nc.ConnectedUrl())
		}),
		nats.PingInterval(20 * time.Second),
		nats.MaxPingsOutstanding(3),
	}

	nc, err := nats.Connect(strings.Join(cfg.URLs, ","), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	c := &NATSTraceConsumer{
		cfg:       cfg,
		nc:        nc,
		js:        js,
		connected: true,
		handler:   handler,
	}

	if err := c.ensureStreamAndConsumer(); err != nil {
		log.Printf("[NATS Consumer] Warning: stream/consumer setup: %v", err)
	}

	log.Printf("[NATS Consumer] Connected to %s, stream=%s, consumer=%s",
		cfg.URLs, cfg.StreamName, cfg.ConsumerName)
	return c, nil
}

func (c *NATSTraceConsumer) ensureStreamAndConsumer() error {
	subjects := []string{fmt.Sprintf("%s.>", c.cfg.SubjectPrefix)}
	_, err := c.js.StreamInfo(c.cfg.StreamName)
	if err != nil {
		_, err = c.js.AddStream(&nats.StreamConfig{
			Name:       c.cfg.StreamName,
			Subjects:   subjects,
			Retention:  nats.LimitsPolicy,
			MaxMsgs:    c.cfg.MaxMsgs,
			MaxBytes:   c.cfg.MaxBytes,
			MaxAge:     time.Duration(c.cfg.RetentionHours) * time.Hour,
			Storage:    nats.FileStorage,
			Replicas:   c.cfg.Replicas,
			Discard:    nats.DiscardOld,
			Duplicates: 2 * time.Minute,
		})
		if err != nil && !strings.Contains(err.Error(), "already in use") {
			return err
		}
	}

	ci, _ := c.js.ConsumerInfo(c.cfg.StreamName, c.cfg.ConsumerName)
	if ci == nil {
		_, err = c.js.AddConsumer(c.cfg.StreamName, &nats.ConsumerConfig{
			Durable:        c.cfg.ConsumerName,
			DeliverSubject: "",
			DeliverGroup:   "",
			FilterSubject:  fmt.Sprintf("%s.>", c.cfg.SubjectPrefix),
			AckPolicy:      nats.AckExplicitPolicy,
			AckWait:        30 * time.Second,
			MaxDeliver:     5,
			MaxAckPending:  1000,
			ReplayPolicy:   nats.ReplayInstantPolicy,
			DeliverPolicy:  nats.DeliverNewPolicy,
			MaxWaiting:     1024,
			FlowControl:    true,
			Heartbeat:      5 * time.Second,
		})
		if err != nil {
			return fmt.Errorf("create consumer: %w", err)
		}
	}

	return nil
}

func (c *NATSTraceConsumer) Consume(ctx context.Context) error {
	sub, err := c.js.PullSubscribe(
		fmt.Sprintf("%s.>", c.cfg.SubjectPrefix),
		c.cfg.ConsumerName,
		nats.Bind(c.cfg.StreamName, c.cfg.ConsumerName),
		nats.ManualAck(),
	)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	c.mu.Lock()
	c.sub = sub
	c.connected = true
	c.mu.Unlock()

	log.Printf("[NATS Consumer] Pull subscription started")

	batchSize := 100
	waitTime := 500 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			log.Printf("[NATS Consumer] Context cancelled, stopping")
			return nil
		default:
			msgs, err := sub.Fetch(batchSize, nats.MaxWait(waitTime))
			if err != nil {
				if err == nats.ErrTimeout || err == nats.ErrBadSubscription {
					continue
				}
				if ctx.Err() != nil {
					return nil
				}
				log.Printf("[NATS Consumer] Fetch error: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			c.mu.Lock()
			c.stats.Consumed += int64(len(msgs))
			c.stats.Batches++
			c.mu.Unlock()

			c.processBatch(ctx, msgs)
		}
	}
}

func (c *NATSTraceConsumer) processBatch(ctx context.Context, msgs []*nats.Msg) {
	for _, msg := range msgs {
		var span model.TraceSpan
		if err := json.Unmarshal(msg.Data, &span); err != nil {
			log.Printf("[NATS Consumer] Unmarshal error: %v", err)
			c.mu.Lock()
			c.stats.Errors++
			c.mu.Unlock()
			msg.Term()
			continue
		}

		if err := c.handler(ctx, &span); err != nil {
			log.Printf("[NATS Consumer] Handler error for req=%s: %v", span.RequestID, err)
			c.mu.Lock()
			c.stats.Errors++
			c.mu.Unlock()

			meta, err := msg.Metadata()
			if err == nil && meta.NumDelivered > 3 {
				msg.Term()
			} else {
				msg.Nak()
			}
			continue
		}

		if err := msg.Ack(); err != nil {
			log.Printf("[NATS Consumer] Ack error: %v", err)
		}

		c.mu.Lock()
		c.stats.Processed++
		c.stats.LastMsg = time.Now()
		c.mu.Unlock()
	}
}

func (c *NATSTraceConsumer) Stats() ConsumerStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

func (c *NATSTraceConsumer) Close() {
	if c.sub != nil {
		c.sub.Unsubscribe()
	}
	if c.nc != nil {
		c.nc.Drain()
		c.nc.Close()
	}
}

func (c *NATSTraceConsumer) Connected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected && c.nc != nil && c.nc.IsConnected()
}
