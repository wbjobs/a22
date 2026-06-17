PROJECT_NAME := ebpf-serverless-tracing
GATEWAY_IMG := ebpf-serverless/api-gateway
FUNCTION_A_IMG := ebpf-serverless/function-a
FUNCTION_B_IMG := ebpf-serverless/function-b
FUNCTION_C_IMG := ebpf-serverless/function-c
NATS_PRODUCER_IMG := ebpf-serverless/nats-producer
NATS_CONSUMER_IMG := ebpf-serverless/nats-consumer
QUERY_API_IMG := ebpf-serverless/query-api
EBPF_PROBE_IMG := ebpf-serverless/ebpf-probe
CHAOS_MANAGER_IMG := ebpf-serverless/chaos-manager
AI_PREDICTOR_IMG := ebpf-serverless/ai-predictor

GO_CMD ?= go
DOCKER_CMD ?= docker
DOCKER_COMPOSE ?= docker compose
KUBECTL ?= kubectl

.PHONY: help build build-go docker-build docker-build-all docker-push k8s-deploy k8s-delete \
        compose-up compose-down compose-logs clean test verify ebpf-generate \
        wasm-build wasm-build-rust sampling-stats wasm-list wasm-upload \
        nats-streams nats-consumers \
        ai-predictions ai-predict ai-send-alert \
        chaos-stats chaos-inject-latency chaos-inject-error chaos-clear chaos-list \
        topology health-overview health-service

help:
	@echo "===== Build ====="
	@echo "  make build                - Build all Go binaries locally"
	@echo "  make docker-build-all     - Build all Docker images"
	@echo "  make verify               - Verify code compiles and format"
	@echo ""
	@echo "===== Runtime ====="
	@echo "  make compose-up           - Start local dev environment (NATS + JetStream)"
	@echo "  make compose-down         - Stop local dev environment"
	@echo "  make compose-logs         - View logs from all services"
	@echo ""
	@echo "===== Dynamic Sampling ====="
	@echo "  make sampling-stats       - View sampling rates and stats"
	@echo "  make sampling-full        - Force 100% sampling for 60s"
	@echo "  make sampling-rate RATE=0.5 - Set global sample rate"
	@echo ""
	@echo "===== WASM Plugins ====="
	@echo "  make wasm-build           - Build all Rust WASM filter plugins"
	@echo "  make wasm-list            - List loaded WASM plugins"
	@echo "  make wasm-upload FILE=xxx - Upload WASM plugin file"
	@echo "  make wasm-enable ID=xxx   - Enable a WASM plugin"
	@echo "  make wasm-disable ID=xxx  - Disable a WASM plugin"
	@echo ""
	@echo "===== NATS / JetStream ====="
	@echo "  make nats-streams         - List JetStream streams"
	@echo "  make nats-consumers       - List JetStream consumers"
	@echo "  make nats-report          - Show NATS traffic report"
	@echo ""
	@echo "===== AI Fault Prediction ====="
	@echo "  make ai-predictions       - View all AI predictions"
	@echo "  make ai-predict SVC=xxx   - Predict timeout for specific service"
	@echo "  make ai-send-alert        - Send test alert to DingTalk/Feishu"
	@echo ""
	@echo "===== Chaos Engineering ====="
	@echo "  make chaos-stats          - View chaos injection stats"
	@echo "  make chaos-list           - List active chaos rules"
	@echo "  make chaos-inject-latency SVC=function-a MS=500 - Inject latency"
	@echo "  make chaos-inject-error SVC=function-a CODE=500 - Inject error"
	@echo "  make chaos-clear          - Clear all chaos rules"
	@echo ""
	@echo "===== Topology & Health ====="
	@echo "  make topology             - View service topology data"
	@echo "  make topology-ui          - Open topology visualization in browser"
	@echo "  make health-overview      - View overall health status"
	@echo "  make health-service SVC=xxx - View specific service health"
	@echo ""
	@echo "===== Testing ====="
	@echo "  make test                 - Run Go tests"
	@echo "  make test-request         - Send test order request"
	@echo "  make test-trace REQUEST_ID=<id> - Query trace by ID"
	@echo "  make test-stats           - Query API stats"
	@echo ""
	@echo "===== Kubernetes ====="
	@echo "  make k8s-deploy           - Deploy to K8s"
	@echo "  make k8s-delete           - Delete all K8s resources"

build: build-gateway build-function-a build-nats-producer build-nats-consumer build-query-api build-ebpf-probe build-chaos-manager

build-gateway:
	@echo "Building API Gateway..."
	@mkdir -p bin
	CGO_ENABLED=0 $(GO_CMD) build -ldflags="-s -w" -o bin/gateway ./cmd/gateway

build-function-a:
	@echo "Building Function A (Go)..."
	@mkdir -p bin
	CGO_ENABLED=0 $(GO_CMD) build -ldflags="-s -w" -o bin/function-a ./cmd/function-a

build-nats-producer:
	@echo "Building NATS Producer..."
	@mkdir -p bin
	CGO_ENABLED=1 $(GO_CMD) build -ldflags="-s -w" -o bin/nats-producer ./cmd/nats-producer

build-nats-consumer:
	@echo "Building NATS Consumer..."
	@mkdir -p bin
	CGO_ENABLED=0 $(GO_CMD) build -ldflags="-s -w" -o bin/nats-consumer ./cmd/nats-consumer

build-query-api:
	@echo "Building Query API..."
	@mkdir -p bin
	CGO_ENABLED=0 $(GO_CMD) build -ldflags="-s -w" -o bin/query-api ./cmd/query-api

build-ebpf-probe: ebpf-generate
	@echo "Building eBPF Probe..."
	@mkdir -p bin
	CGO_ENABLED=0 $(GO_CMD) build -ldflags="-s -w" -o bin/ebpf-probe ./cmd/ebpf-probe

build-chaos-manager:
	@echo "Building Chaos Engineering Manager..."
	@mkdir -p bin
	CGO_ENABLED=0 $(GO_CMD) build -ldflags="-s -w" -o bin/chaos-manager ./cmd/chaos-manager

ebpf-generate:
	@echo "Generating eBPF bindings..."
	cd cmd/ebpf-probe && $(GO_CMD) generate ./... 2>/dev/null || echo "eBPF generation skipped (requires clang+bpf2go)"

wasm-build: wasm-build-rust

wasm-build-rust:
	@echo "Building Rust WASM filter plugins..."
	@if [ "$$(uname)" = "Windows_NT" ] || [ "$$(echo $$OS | grep -i windows)" ]; then \
		powershell -ExecutionPolicy Bypass -File wasm-plugins/build.ps1; \
	else \
		bash wasm-plugins/build.sh; \
	fi

docker-build-all: docker-build-gateway docker-build-function-a docker-build-function-b \
                  docker-build-function-c docker-build-nats-producer \
                  docker-build-nats-consumer docker-build-query-api docker-build-ebpf-probe \
                  docker-build-chaos-manager docker-build-ai-predictor

docker-build-gateway:
	$(DOCKER_CMD) build -t $(GATEWAY_IMG):latest -f docker/Dockerfile.gateway .

docker-build-function-a:
	$(DOCKER_CMD) build -t $(FUNCTION_A_IMG):latest -f docker/Dockerfile.function-a .

docker-build-function-b:
	$(DOCKER_CMD) build -t $(FUNCTION_B_IMG):latest -f docker/Dockerfile.function-b .

docker-build-function-c:
	$(DOCKER_CMD) build -t $(FUNCTION_C_IMG):latest -f docker/Dockerfile.function-c .

docker-build-nats-producer:
	$(DOCKER_CMD) build -t $(NATS_PRODUCER_IMG):latest -f docker/Dockerfile.nats-producer .

docker-build-nats-consumer:
	$(DOCKER_CMD) build -t $(NATS_CONSUMER_IMG):latest -f docker/Dockerfile.nats-consumer .

docker-build-query-api:
	$(DOCKER_CMD) build -t $(QUERY_API_IMG):latest -f docker/Dockerfile.query-api .

docker-build-ebpf-probe:
	$(DOCKER_CMD) build -t $(EBPF_PROBE_IMG):latest -f docker/Dockerfile.ebpf-probe .

docker-build-chaos-manager:
	$(DOCKER_CMD) build -t $(CHAOS_MANAGER_IMG):latest -f docker/Dockerfile.chaos-manager .

docker-build-ai-predictor:
	$(DOCKER_CMD) build -t $(AI_PREDICTOR_IMG):latest -f docker/Dockerfile.ai-predictor .

docker-push:
	$(DOCKER_CMD) push $(GATEWAY_IMG):latest
	$(DOCKER_CMD) push $(FUNCTION_A_IMG):latest
	$(DOCKER_CMD) push $(FUNCTION_B_IMG):latest
	$(DOCKER_CMD) push $(FUNCTION_C_IMG):latest
	$(DOCKER_CMD) push $(NATS_PRODUCER_IMG):latest
	$(DOCKER_CMD) push $(NATS_CONSUMER_IMG):latest
	$(DOCKER_CMD) push $(QUERY_API_IMG):latest
	$(DOCKER_CMD) push $(EBPF_PROBE_IMG):latest

compose-up:
	@echo "Starting full stack with NATS + JetStream + Dynamic Sampling + WASM + AI Prediction + Chaos Engineering..."
	$(DOCKER_COMPOSE) up -d --build
	@echo ""
	@echo "Wait ~90s for all services to initialize, then:"
	@echo "  Gateway:       http://localhost:8080"
	@echo "  Query API:     http://localhost:8081"
	@echo "  Topology UI:   http://localhost:8081/web/topology.html"
	@echo "  Producer:      http://localhost:8085 (sampling + WASM admin)"
	@echo "  Consumer:      http://localhost:8086"
	@echo "  AI Predictor:  http://localhost:8087 (LSTM fault prediction)"
	@echo "  Chaos Manager: http://localhost:8088 (chaos injection admin)"
	@echo "  Kibana:        http://localhost:5601"
	@echo "  NATS UI:       http://localhost:8222"
	@echo ""
	@echo "Quick commands:"
	@echo "  make topology-ui          - Open topology visualization"
	@echo "  make ai-predictions       - View AI fault predictions"
	@echo "  make chaos-inject-latency SVC=function-a MS=500 - Inject latency"
	@echo "  make health-overview      - View health overview"
	@echo ""
	@echo "Run 'make compose-logs' to view logs"

compose-down:
	$(DOCKER_COMPOSE) down -v

compose-logs:
	$(DOCKER_COMPOSE) logs -f --tail=100

compose-restart:
	$(DOCKER_COMPOSE) restart

sampling-stats:
	@echo "===== Sampling Stats ====="
	@curl -s http://localhost:8085/admin/sampling/stats | python -m json.tool 2>/dev/null || curl -s http://localhost:8085/admin/sampling/stats
	@echo ""
	@echo "===== Sampling Rates ====="
	@curl -s http://localhost:8085/admin/sampling/rates | python -m json.tool 2>/dev/null || curl -s http://localhost:8085/admin/sampling/rates

sampling-full:
	@echo "Forcing 100% sampling for 60 seconds..."
	@curl -s -X POST http://localhost:8085/admin/sampling/force-full \
		-H "Content-Type: application/json" \
		-d '{"duration_seconds": 60}' | python -m json.tool 2>/dev/null || cat

sampling-rate:
	@if [ -z "$(RATE)" ]; then echo "Usage: make sampling-rate RATE=0.5 (0.01-1.0)"; exit 1; fi
	@echo "Setting global sample rate to $(RATE)"
	@curl -s -X POST http://localhost:8085/admin/sampling/rate \
		-H "Content-Type: application/json" \
		-d '{"rate": $(RATE)}' | python -m json.tool 2>/dev/null || cat

wasm-list:
	@echo "===== Loaded WASM Plugins ====="
	@curl -s http://localhost:8085/admin/wasm/plugins | python -m json.tool 2>/dev/null || curl -s http://localhost:8085/admin/wasm/plugins

wasm-upload:
	@if [ -z "$(FILE)" ]; then echo "Usage: make wasm-upload FILE=./plugins/wasm/slow-request-filter.wasm"; exit 1; fi
	@if [ ! -f "$(FILE)" ]; then echo "File not found: $(FILE)"; exit 1; fi
	@echo "Uploading WASM plugin: $(FILE)"
	@curl -s -X POST http://localhost:8085/admin/wasm/plugins \
		-F "name=custom-filter" \
		-F "version=0.1.0" \
		-F "description=Custom filter" \
		-F "wasm=@$(FILE)" | python -m json.tool 2>/dev/null || cat

wasm-enable:
	@if [ -z "$(ID)" ]; then echo "Usage: make wasm-enable ID=<plugin-id>"; exit 1; fi
	@curl -s -X POST http://localhost:8085/admin/wasm/plugins/$(ID)/enable | python -m json.tool 2>/dev/null || cat

wasm-disable:
	@if [ -z "$(ID)" ]; then echo "Usage: make wasm-disable ID=<plugin-id>"; exit 1; fi
	@curl -s -X POST http://localhost:8085/admin/wasm/plugins/$(ID)/disable | python -m json.tool 2>/dev/null || cat

nats-streams:
	@echo "===== JetStream Streams ====="
	@docker exec tracing-nats nats --server nats:4222 str ls -l || \
		$(DOCKER_CMD) run --rm --network tracing-network natsio/nats-box:0.14.1 \
		nats --server nats:4222 str ls -l

nats-consumers:
	@echo "===== JetStream Consumers ====="
	@docker exec tracing-nats nats --server nats:4222 con ls TRACES || \
		$(DOCKER_CMD) run --rm --network tracing-network natsio/nats-box:0.14.1 \
		nats --server nats:4222 con ls TRACES

nats-report:
	@echo "===== NATS Traffic Report ====="
	@curl -s http://localhost:8085/stats | python -m json.tool 2>/dev/null || curl -s http://localhost:8085/stats
	@echo ""
	@echo "===== Consumer Lag ====="
	@curl -s http://localhost:8086/stats | python -m json.tool 2>/dev/null || curl -s http://localhost:8086/stats

k8s-deploy: k8s-deploy-ns k8s-deploy-infra k8s-deploy-functions k8s-deploy-tracing

k8s-deploy-ns:
	$(KUBECTL) apply -f k8s/namespace.yaml

k8s-deploy-infra:
	$(KUBECTL) apply -f k8s/infra.yaml

k8s-deploy-functions:
	$(KUBECTL) apply -f k8s/api-gateway.yaml
	$(KUBECTL) apply -f k8s/functions.yaml

k8s-deploy-tracing:
	$(KUBECTL) apply -f k8s/tracing-services.yaml

k8s-delete:
	-$(KUBECTL) delete namespace serverless-tracing --ignore-not-found
	-$(KUBECTL) delete -f k8s/ --ignore-not-found

k8s-logs-gateway:
	$(KUBECTL) logs -f -n serverless-tracing -l app=api-gateway

k8s-logs-consumer:
	$(KUBECTL) logs -f -n serverless-tracing -l app=nats-consumer

k8s-status:
	$(KUBECTL) get all -n serverless-tracing

k8s-port-forward-gateway:
	$(KUBECTL) port-forward -n serverless-tracing svc/api-gateway 8080:8080

k8s-port-forward-query:
	$(KUBECTL) port-forward -n serverless-tracing svc/query-api 8081:8081

k8s-port-forward-kibana:
	$(KUBECTL) port-forward -n serverless-tracing svc/kibana 5601:5601

test:
	$(GO_CMD) test -v -race ./internal/...

verify:
	@echo "Checking go fmt..."
	@test -z "$$($(GO_CMD) fmt ./...)" || (echo "Files need formatting, run: go fmt ./..." && exit 1)
	@echo "Checking go vet..."
	$(GO_CMD) vet ./...
	@echo "Checking build (no wasm)..."
	CGO_ENABLED=0 $(GO_CMD) build ./cmd/gateway ./cmd/function-a ./cmd/nats-consumer ./cmd/query-api
	@echo "All checks passed!"

test-request:
	@echo "Sending test order request..."
	@curl -s -X POST http://localhost:8080/api/order \
		-H "Content-Type: application/json" \
		-d '{"product_id":"P12345","amount":299.99,"user_id":"U78901"}' | python -m json.tool 2>/dev/null || cat

test-trace:
	@if [ -z "$(REQUEST_ID)" ]; then echo "Usage: make test-trace REQUEST_ID=<request-id>"; exit 1; fi
	@echo "Querying trace: $(REQUEST_ID)"
	@curl -s http://localhost:8081/api/trace/$(REQUEST_ID) | python -m json.tool 2>/dev/null || cat

test-waterfall:
	@if [ -z "$(REQUEST_ID)" ]; then echo "Usage: make test-waterfall REQUEST_ID=<request-id>"; exit 1; fi
	@curl -s http://localhost:8081/api/waterfall/$(REQUEST_ID) | python -m json.tool 2>/dev/null || cat

test-stats:
	@echo "Query API stats (last 1 hour)..."
	@curl -s http://localhost:8081/api/stats | python -m json.tool 2>/dev/null || cat

test-services:
	@echo "Tracked services..."
	@curl -s http://localhost:8081/api/services | python -m json.tool 2>/dev/null || cat

# ===== AI Fault Prediction =====

ai-predictions:
	@echo "===== AI Fault Predictions ====="
	@curl -s http://localhost:8087/api/predictions | python -m json.tool 2>/dev/null || curl -s http://localhost:8087/api/predictions

ai-predict:
	@if [ -z "$(SVC)" ]; then echo "Usage: make ai-predict SVC=function-a"; exit 1; fi
	@echo "Predicting timeout risk for service: $(SVC)"
	@curl -s http://localhost:8087/api/predictions/$(SVC) | python -m json.tool 2>/dev/null || cat

ai-send-alert:
	@echo "Sending test alert..."
	@curl -s -X POST http://localhost:8087/api/alert \
		-H "Content-Type: application/json" \
		-d '{"service":"test-service","confidence":0.85,"avg_latency":1500,"error_rate":0.08,"details":"Test alert from make command"}' | python -m json.tool 2>/dev/null || cat

# ===== Chaos Engineering =====

chaos-stats:
	@echo "===== Chaos Engineering Stats ====="
	@curl -s http://localhost:8088/api/stats | python -m json.tool 2>/dev/null || curl -s http://localhost:8088/api/stats
	@echo ""
	@echo "===== Available Services ====="
	@curl -s http://localhost:8088/api/services | python -m json.tool 2>/dev/null || curl -s http://localhost:8088/api/services

chaos-list:
	@echo "===== Active Chaos Rules ====="
	@curl -s http://localhost:8088/api/rules | python -m json.tool 2>/dev/null || curl -s http://localhost:8088/api/rules

chaos-inject-latency:
	@if [ -z "$(SVC)" ] || [ -z "$(MS)" ]; then echo "Usage: make chaos-inject-latency SVC=function-a MS=500 [DURATION=300] [PROB=1.0]"; exit 1; fi
	@echo "Injecting latency $(MS)ms to $(SVC)..."
	@curl -s -X POST http://localhost:8088/api/inject/latency \
		-H "Content-Type: application/json" \
		-d "{\"service\":\"$(SVC)\",\"ms\":$(MS),\"duration_sec\":$(or $(DURATION),300),\"prob\":$(or $(PROB),1.0),\"reason\":\"Manual test from CLI\"}" | python -m json.tool 2>/dev/null || cat

chaos-inject-error:
	@if [ -z "$(SVC)" ]; then echo "Usage: make chaos-inject-error SVC=function-a [CODE=500] [DURATION=300] [PROB=1.0]"; exit 1; fi
	@echo "Injecting error $(or $(CODE),500) to $(SVC)..."
	@curl -s -X POST http://localhost:8088/api/inject/error \
		-H "Content-Type: application/json" \
		-d "{\"service\":\"$(SVC)\",\"status_code\":$(or $(CODE),500),\"duration_sec\":$(or $(DURATION),300),\"prob\":$(or $(PROB),1.0),\"reason\":\"Manual test from CLI\"}" | python -m json.tool 2>/dev/null || cat

chaos-clear:
	@echo "Clearing ALL chaos rules..."
	@curl -s -X POST http://localhost:8088/api/clear | python -m json.tool 2>/dev/null || cat

# ===== Topology & Health =====

topology:
	@echo "===== Service Topology ====="
	@curl -s http://localhost:8081/api/topology | python -m json.tool 2>/dev/null || curl -s http://localhost:8081/api/topology

topology-ui:
	@echo "Opening topology visualization..."
	@echo "Please open in browser: http://localhost:8081/web/topology.html"
	-@start http://localhost:8081/web/topology.html 2>/dev/null || \
		xdg-open http://localhost:8081/web/topology.html 2>/dev/null || \
		open http://localhost:8081/web/topology.html 2>/dev/null || true

health-overview:
	@echo "===== Overall Health Overview ====="
	@curl -s http://localhost:8081/api/health/overview | python -m json.tool 2>/dev/null || curl -s http://localhost:8081/api/health/overview

health-service:
	@if [ -z "$(SVC)" ]; then echo "Usage: make health-service SVC=function-a"; exit 1; fi
	@echo "===== Health Status: $(SVC) ====="
	@curl -s http://localhost:8081/api/health/$(SVC) | python -m json.tool 2>/dev/null || cat

clean:
	@echo "Cleaning build artifacts..."
	@rm -rf bin/
	@rm -f cmd/ebpf-probe/*.o
	@find . -name "*.go" -path "*/generated/*" -delete 2>/dev/null || true
