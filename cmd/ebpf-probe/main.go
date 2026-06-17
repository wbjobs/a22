package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/perf"
	"github.com/cilium/ebpf/rlimit"
	"github.com/vishvananda/netlink"

	"ebpf-serverless-tracing/internal/config"
	"ebpf-serverless-tracing/internal/ebpf"
	"ebpf-serverless-tracing/internal/model"
)

//go:generate go run github.com/cilium/ebpf/cmd/bpf2go -cc clang httpTrace ../internal/ebpf/trace_http.c -- -I../internal/ebpf -I/usr/include -I/usr/include/x86_64-linux-gnu

type pendingRequest struct {
	StartTime time.Time
	Event     *ebpf.HTTPEvent
}

type App struct {
	cfg             *config.Config
	pendingRequests sync.Map
	objs            httpTraceObjects
	links           []link.Link
	perfReader      *perf.Reader
	kafkaClient     *http.Client
	stopChan        chan struct{}
}

func main() {
	cfg := config.Load()
	app := &App{
		cfg:         cfg,
		kafkaClient: &http.Client{Timeout: 10 * time.Second},
		stopChan:    make(chan struct{}),
	}

	if err := rlimit.RemoveMemlock(); err != nil {
		log.Printf("Warning: removing memlock limit: %v", err)
	}

	if err := app.loadEBPF(); err != nil {
		log.Fatalf("Failed to load eBPF: %v", err)
	}
	defer app.objs.Close()
	defer app.cleanup()

	ifaces, err := detectNetworkInterfaces()
	if err != nil {
		log.Fatalf("Failed to detect interfaces: %v", err)
	}

	if len(ifaces) == 0 {
		log.Fatalf("No suitable network interfaces found")
	}

	log.Printf("Attaching eBPF to interfaces: %v", ifaces)
	if err := app.attachToInterfaces(ifaces); err != nil {
		log.Fatalf("Failed to attach eBPF: %v", err)
	}

	if err := app.setupPerfReader(); err != nil {
		log.Fatalf("Failed to setup perf reader: %v", err)
	}
	defer app.perfReader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go app.eventLoop(ctx)

	go app.startHealthServer()

	log.Println("eBPF network tracer started successfully")
	log.Printf("Kafka REST proxy: %s", cfg.KafkaBrokers)

	select {
	case <-sigChan:
		log.Println("Received shutdown signal")
	case <-app.stopChan:
		log.Println("Stopping...")
	}

	cancel()
}

func (a *App) loadEBPF() error {
	spec, err := loadHTTPTrace()
	if err != nil {
		return fmt.Errorf("loading bpf spec: %w", err)
	}

	if err := spec.LoadAndAssign(&a.objs, nil); err != nil {
		return fmt.Errorf("loading objects: %w", err)
	}

	return nil
}

func (a *App) attachToInterfaces(ifaces []string) error {
	for _, ifaceName := range ifaces {
		iface, err := netlink.LinkByName(ifaceName)
		if err != nil {
			log.Printf("Warning: interface %s not found: %v", ifaceName, err)
			continue
		}

		ingress, err := link.AttachTCX(link.TCXOptions{
			Interface: iface.Attrs().Index,
			Program:   a.objs.TraceIngress,
			Attach:    link.TCXIngress,
		})
		if err != nil {
			var tcErr error
			clsIngress, tcErr := link.AttachTCEgress(link.TCOptions{
				Interface: iface.Attrs().Index,
				Program:   a.objs.TraceIngress,
			})
			if tcErr != nil {
				log.Printf("Warning: failed to attach ingress to %s: %v / %v", ifaceName, err, tcErr)
			} else {
				a.links = append(a.links, clsIngress)
				log.Printf("Attached TC ingress to %s (fallback)", ifaceName)
			}
		} else {
			a.links = append(a.links, ingress)
			log.Printf("Attached TCX ingress to %s", ifaceName)
		}

		egress, err := link.AttachTCX(link.TCXOptions{
			Interface: iface.Attrs().Index,
			Program:   a.objs.TraceEgress,
			Attach:    link.TCXEgress,
		})
		if err != nil {
			var tcErr error
			clsEgress, tcErr := link.AttachTCEgress(link.TCOptions{
				Interface: iface.Attrs().Index,
				Program:   a.objs.TraceEgress,
			})
			if tcErr != nil {
				log.Printf("Warning: failed to attach egress to %s: %v / %v", ifaceName, err, tcErr)
			} else {
				a.links = append(a.links, clsEgress)
				log.Printf("Attached TC egress to %s (fallback)", ifaceName)
			}
		} else {
			a.links = append(a.links, egress)
			log.Printf("Attached TCX egress to %s", ifaceName)
		}
	}

	if len(a.links) == 0 {
		return fmt.Errorf("failed to attach to any interface")
	}

	return nil
}

func (a *App) setupPerfReader() error {
	reader, err := perf.NewReader(a.objs.Events, 64*1024*1024)
	if err != nil {
		return fmt.Errorf("creating perf reader: %w", err)
	}
	a.perfReader = reader
	return nil
}

func (a *App) eventLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	cleanupTicker := time.NewTicker(30 * time.Second)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			continue
		case <-cleanupTicker.C:
			a.cleanupPendingRequests()
		default:
			record, err := a.perfReader.Read()
			if err != nil {
				if errors.Is(err, perf.ErrClosed) {
					return
				}
				log.Printf("Error reading perf record: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if record.LostSamples != 0 {
				log.Printf("Lost %d perf samples", record.LostSamples)
				continue
			}

			evt, err := ebpf.ParseHTTPEvent(record.RawSample)
			if err != nil {
				log.Printf("Error parsing event: %v", err)
				continue
			}

			a.processEvent(evt)
		}
	}
}

func (a *App) processEvent(evt *ebpf.HTTPEvent) {
	if evt.RequestID == "" && evt.Method == "" && evt.StatusCode == 0 {
		return
	}

	log.Printf("eBPF captured: %s", evt.String())

	if evt.IsRequest() {
		key := fmt.Sprintf("%s:%d-%s:%d-%s",
			evt.SrcIP, evt.SrcPort, evt.DstIP, evt.DstPort, evt.RequestID)
		a.pendingRequests.Store(key, &pendingRequest{
			StartTime: time.Now(),
			Event:     evt,
		})
		return
	}

	if evt.IsResponse() {
		keyPattern := fmt.Sprintf("%s:%d-%s:%d-%s",
			evt.DstIP, evt.DstPort, evt.SrcIP, evt.SrcPort, evt.RequestID)

		var matchedReq *pendingRequest
		a.pendingRequests.Range(func(k, v interface{}) bool {
			key, _ := k.(string)
			if key == keyPattern {
				matchedReq = v.(*pendingRequest)
				a.pendingRequests.Delete(k)
				return false
			}
			req, _ := v.(*pendingRequest)
			if req.Event != nil && req.Event.RequestID == evt.RequestID && evt.RequestID != "" {
				matchedReq = req
				a.pendingRequests.Delete(k)
				return false
			}
			return true
		})

		var span model.TraceSpan
		span.Timestamp = time.Now()
		span.RequestID = evt.RequestID
		span.TraceID = evt.TraceID
		span.SpanID = evt.SpanID
		span.ParentSpanID = evt.ParentSpanID
		span.FunctionName = evt.FunctionName
		span.ServiceName = evt.ServiceName
		span.StatusCode = int(evt.StatusCode)
		span.Protocol = "HTTP"
		span.SourceIP = evt.SrcIP.String()
		span.DestIP = evt.DstIP.String()
		span.SourcePort = evt.SrcPort
		span.DestPort = evt.DstPort

		if matchedReq != nil {
			span.StartTime = matchedReq.StartTime
			span.EndTime = time.Now()
			span.DurationMs = span.EndTime.Sub(span.StartTime).Milliseconds()
			span.Method = matchedReq.Event.Method
			span.Path = matchedReq.Event.Path
			if span.RequestID == "" {
				span.RequestID = matchedReq.Event.RequestID
			}
			if span.TraceID == "" {
				span.TraceID = matchedReq.Event.TraceID
			}
			if span.FunctionName == "" {
				span.FunctionName = matchedReq.Event.FunctionName
			}
			if span.ServiceName == "" {
				span.ServiceName = matchedReq.Event.ServiceName
			}
		} else {
			now := time.Now()
			span.StartTime = now
			span.EndTime = now
			span.DurationMs = 0
		}

		if span.FunctionName == "" {
			span.FunctionName = detectFunctionName(span.DestPort, evt.Path)
		}

		if span.ServiceName == "" {
			span.ServiceName = span.FunctionName
		}

		a.sendToKafka(&span)
		return
	}

	if evt.RequestID != "" {
		span := model.TraceSpan{
			Timestamp:    time.Now(),
			RequestID:    evt.RequestID,
			TraceID:      evt.TraceID,
			SpanID:       evt.SpanID,
			ParentSpanID: evt.ParentSpanID,
			FunctionName: evt.FunctionName,
			ServiceName:  evt.ServiceName,
			StatusCode:   0,
			Method:       evt.Method,
			Path:         evt.Path,
			Protocol:     "HTTP",
			SourceIP:     evt.SrcIP.String(),
			DestIP:       evt.DstIP.String(),
			SourcePort:   evt.SrcPort,
			DestPort:     evt.DstPort,
			StartTime:    time.Now(),
			EndTime:      time.Now(),
			DurationMs:   0,
		}

		if span.FunctionName == "" {
			span.FunctionName = detectFunctionName(span.DestPort, evt.Path)
			span.ServiceName = span.FunctionName
		}

		a.sendToKafka(&span)
	}
}

func detectFunctionName(port uint16, path string) string {
	switch port {
	case 8080:
		return "api-gateway"
	case 8082:
		return "function-a-order"
	case 8083:
		return "function-b-payment"
	case 8084:
		return "function-c-notification"
	}

	switch {
	case contains(path, "/order"):
		return "function-a-order"
	case contains(path, "/payment"):
		return "function-b-payment"
	case contains(path, "/notify"):
		return "function-c-notification"
	case contains(path, "/api/"):
		return "api-gateway"
	default:
		return "unknown-service"
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && (s[:len(substr)] == substr || indexOf(s, substr) >= 0))
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func (a *App) sendToKafka(span *model.TraceSpan) {
	if span.RequestID == "" {
		return
	}

	spanJSON, err := json.Marshal(span)
	if err != nil {
		log.Printf("Error marshaling span: %v", err)
		return
	}

	log.Printf("[eBPF->Kafka] req=%s span=%s fn=%s status=%d duration=%dms",
		span.RequestID, span.SpanID, span.FunctionName, span.StatusCode, span.DurationMs)

	kafkaURL := fmt.Sprintf("http://%s/topics/%s", a.cfg.KafkaBrokers, a.cfg.KafkaTopic)
	if a.cfg.KafkaBrokers == "kafka:9092" {
		kafkaURL = "http://kafka-rest:8082/topics/" + a.cfg.KafkaTopic
	}

	kafkaMsg := map[string]interface{}{
		"records": []map[string]interface{}{
			{
				"key":   span.RequestID,
				"value": string(spanJSON),
			},
		},
	}

	msgJSON, _ := json.Marshal(kafkaMsg)
	req, err := http.NewRequest("POST", kafkaURL, bytes.NewBuffer(msgJSON))
	if err != nil {
		log.Printf("Error creating Kafka request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/vnd.kafka.json.v2+json")
	req.Header.Set("Accept", "application/vnd.kafka.v2+json")

	go func() {
		resp, err := a.kafkaClient.Do(req)
		if err != nil {
			log.Printf("Warning: failed to send to Kafka (will retry via local channel): %v", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			log.Printf("Warning: Kafka returned status %d", resp.StatusCode)
		}
	}()
}

func (a *App) cleanupPendingRequests() {
	now := time.Now()
	a.pendingRequests.Range(func(k, v interface{}) bool {
		req, ok := v.(*pendingRequest)
		if !ok {
			a.pendingRequests.Delete(k)
			return true
		}
		if now.Sub(req.StartTime) > 2*time.Minute {
			a.pendingRequests.Delete(k)
		}
		return true
	})
}

func (a *App) cleanup() {
	for _, l := range a.links {
		l.Close()
	}
}

func (a *App) startHealthServer() {
	healthPort := "9400"
	if p := os.Getenv("EBPF_PROBE_HEALTH_PORT"); p != "" {
		healthPort = p
	}
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"service":"ebpf-probe","status":"healthy","time":"%s"}`, time.Now().UTC().Format(time.RFC3339))
	})
	log.Printf("eBPF probe health server on :%s", healthPort)
	if err := http.ListenAndServe(":"+healthPort, nil); err != nil {
		log.Printf("Health server error: %v", err)
	}
}

func detectNetworkInterfaces() ([]string, error) {
	candidates := []string{"eth0", "ens192", "ens160", "eno1", "enp0s3", "wlan0", "docker0", "cilium_host"}
	var result []string

	for _, name := range candidates {
		_, err := netlink.LinkByName(name)
		if err == nil {
			result = append(result, name)
		}
	}

	links, err := netlink.LinkList()
	if err != nil {
		return result, nil
	}

	for _, l := range links {
		attrs := l.Attrs()
		if attrs.Flags&net.FlagUp == 0 {
			continue
		}
		if attrs.Name == "lo" {
			continue
		}
		found := false
		for _, r := range result {
			if r == attrs.Name {
				found = true
				break
			}
		}
		if !found {
			result = append(result, attrs.Name)
		}
	}

	return result, nil
}
