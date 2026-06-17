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
)

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
	Consumed    int64
	Processed   int64
	Errors      int64
	Redelivered int64
	Pending     int64
	LastMsg     time.Time
	Batches     int64
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
