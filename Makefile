PROJECT_NAME := ebpf-serverless-tracing
GATEWAY_IMG := ebpf-serverless/api-gateway
FUNCTION_A_IMG := ebpf-serverless/function-a
FUNCTION_B_IMG := ebpf-serverless/function-b
FUNCTION_C_IMG := ebpf-serverless/function-c
NATS_PRODUCER_IMG := ebpf-serverless/nats-producer
NATS_CONSUMER_IMG := ebpf-serverless/nats-consumer
QUERY_API_IMG := ebpf-serverless/query-api
EBPF_PROBE_IMG := ebpf-serverless/ebpf-probe

GO_CMD ?= go
DOCKER_CMD ?= docker
DOCKER_COMPOSE ?= docker compose
KUBECTL ?= kubectl

.PHONY: help build build-go docker-build docker-build-all docker-push k8s-deploy k8s-delete \
        compose-up compose-down compose-logs clean test verify ebpf-generate \
        wasm-build wasm-build-rust sampling-stats wasm-list wasm-upload \
        nats-streams nats-consumers

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
	@echo "===== Testing ====="
	@echo "  make test                 - Run Go tests"
	@echo "  make test-request         - Send test order request"
	@echo "  make test-trace REQUEST_ID=<id> - Query trace by ID"
	@echo "  make test-stats           - Query API stats"
	@echo ""
	@echo "===== Kubernetes ====="
	@echo "  make k8s-deploy           - Deploy to K8s"
	@echo "  make k8s-delete           - Delete all K8s resources"

build: build-gateway build-function-a build-nats-producer build-nats-consumer build-query-api build-ebpf-probe

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
                  docker-build-nats-consumer docker-build-query-api docker-build-ebpf-probe

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
	@echo "Starting full stack with NATS + JetStream + Dynamic Sampling + WASM..."
	$(DOCKER_COMPOSE) up -d --build
	@echo ""
	@echo "Wait ~60s for all services to initialize, then:"
	@echo "  Gateway:     http://localhost:8080"
	@echo "  Query API:   http://localhost:8081"
	@echo "  Producer:    http://localhost:8085 (sampling + WASM admin)"
	@echo "  Consumer:    http://localhost:8086"
	@echo "  Kibana:      http://localhost:5601"
	@echo "  NATS UI:     http://localhost:8222"
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

clean:
	@echo "Cleaning build artifacts..."
	@rm -rf bin/
	@rm -f cmd/ebpf-probe/*.o
	@find . -name "*.go" -path "*/generated/*" -delete 2>/dev/null || true
