PROJECT_NAME := ebpf-serverless-tracing
GATEWAY_IMG := ebpf-serverless/api-gateway
FUNCTION_A_IMG := ebpf-serverless/function-a
FUNCTION_B_IMG := ebpf-serverless/function-b
FUNCTION_C_IMG := ebpf-serverless/function-c
KAFKA_PRODUCER_IMG := ebpf-serverless/kafka-producer
ES_CONSUMER_IMG := ebpf-serverless/es-consumer
QUERY_API_IMG := ebpf-serverless/query-api
EBPF_PROBE_IMG := ebpf-serverless/ebpf-probe

GO_CMD ?= go
DOCKER_CMD ?= docker
DOCKER_COMPOSE ?= docker compose
KUBECTL ?= kubectl

.PHONY: help build build-go docker-build docker-build-all docker-push k8s-deploy k8s-delete \
        compose-up compose-down compose-logs clean test verify ebpf-generate

help:
	@echo "Available targets:"
	@echo "  make build              - Build all Go binaries locally"
	@echo "  make docker-build-all   - Build all Docker images"
	@echo "  make compose-up         - Start local dev environment with docker-compose"
	@echo "  make compose-down       - Stop local dev environment"
	@echo "  make compose-logs       - View logs from all services"
	@echo "  make k8s-deploy         - Deploy everything to Kubernetes"
	@echo "  make k8s-delete         - Delete all K8s resources"
	@echo "  make test               - Run Go tests"
	@echo "  make verify             - Verify code compiles and format"
	@echo "  make ebpf-generate      - Regenerate eBPF Go bindings"
	@echo "  make clean              - Clean build artifacts"

build: build-gateway build-function-a build-kafka-producer build-es-consumer build-query-api

build-gateway:
	@echo "Building API Gateway..."
	@mkdir -p bin
	$(GO_CMD) build -ldflags="-s -w" -o bin/gateway ./cmd/gateway

build-function-a:
	@echo "Building Function A (Go)..."
	@mkdir -p bin
	$(GO_CMD) build -ldflags="-s -w" -o bin/function-a ./cmd/function-a

build-kafka-producer:
	@echo "Building Kafka Producer..."
	@mkdir -p bin
	$(GO_CMD) build -ldflags="-s -w" -o bin/kafka-producer ./cmd/kafka-producer

build-es-consumer:
	@echo "Building ES Consumer..."
	@mkdir -p bin
	$(GO_CMD) build -ldflags="-s -w" -o bin/es-consumer ./cmd/es-consumer

build-query-api:
	@echo "Building Query API..."
	@mkdir -p bin
	$(GO_CMD) build -ldflags="-s -w" -o bin/query-api ./cmd/query-api

build-ebpf-probe: ebpf-generate
	@echo "Building eBPF Probe..."
	@mkdir -p bin
	$(GO_CMD) build -ldflags="-s -w" -o bin/ebpf-probe ./cmd/ebpf-probe

ebpf-generate:
	@echo "Generating eBPF bindings..."
	cd cmd/ebpf-probe && $(GO_CMD) generate ./...

docker-build-all: docker-build-gateway docker-build-function-a docker-build-function-b \
                  docker-build-function-c docker-build-kafka-producer \
                  docker-build-es-consumer docker-build-query-api docker-build-ebpf-probe

docker-build-gateway:
	$(DOCKER_CMD) build -t $(GATEWAY_IMG):latest -f docker/Dockerfile.gateway .

docker-build-function-a:
	$(DOCKER_CMD) build -t $(FUNCTION_A_IMG):latest -f docker/Dockerfile.function-a .

docker-build-function-b:
	$(DOCKER_CMD) build -t $(FUNCTION_B_IMG):latest -f docker/Dockerfile.function-b .

docker-build-function-c:
	$(DOCKER_CMD) build -t $(FUNCTION_C_IMG):latest -f docker/Dockerfile.function-c .

docker-build-kafka-producer:
	$(DOCKER_CMD) build -t $(KAFKA_PRODUCER_IMG):latest -f docker/Dockerfile.kafka-producer .

docker-build-es-consumer:
	$(DOCKER_CMD) build -t $(ES_CONSUMER_IMG):latest -f docker/Dockerfile.es-consumer .

docker-build-query-api:
	$(DOCKER_CMD) build -t $(QUERY_API_IMG):latest -f docker/Dockerfile.query-api .

docker-build-ebpf-probe:
	$(DOCKER_CMD) build -t $(EBPF_PROBE_IMG):latest -f docker/Dockerfile.ebpf-probe .

docker-push:
	$(DOCKER_CMD) push $(GATEWAY_IMG):latest
	$(DOCKER_CMD) push $(FUNCTION_A_IMG):latest
	$(DOCKER_CMD) push $(FUNCTION_B_IMG):latest
	$(DOCKER_CMD) push $(FUNCTION_C_IMG):latest
	$(DOCKER_CMD) push $(KAFKA_PRODUCER_IMG):latest
	$(DOCKER_CMD) push $(ES_CONSUMER_IMG):latest
	$(DOCKER_CMD) push $(QUERY_API_IMG):latest
	$(DOCKER_CMD) push $(EBPF_PROBE_IMG):latest

compose-up:
	$(DOCKER_COMPOSE) up -d --build

compose-down:
	$(DOCKER_COMPOSE) down -v

compose-logs:
	$(DOCKER_COMPOSE) logs -f

compose-restart:
	$(DOCKER_COMPOSE) restart

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
	$(KUBECTL) logs -f -n serverless-tracing -l app=es-consumer

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
	@test -z "$(shell $(GO_CMD) fmt ./...)" || (echo "Files need formatting, run: go fmt ./..." && exit 1)
	@echo "Checking go vet..."
	$(GO_CMD) vet ./...
	@echo "Checking build..."
	$(GO_CMD) build ./...
	@echo "All checks passed!"

test-request:
	@echo "Sending test order request..."
	@curl -s -X POST http://localhost:8080/api/order \
		-H "Content-Type: application/json" \
		-d '{"product_id":"P12345","amount":299.99,"user_id":"U78901"}' | python -m json.tool 2>/dev/null || cat

test-trace:
	@echo "Usage: make test-trace REQUEST_ID=<request-id>"
	@if [ -n "$(REQUEST_ID)" ]; then \
		curl -s http://localhost:8081/api/trace/$(REQUEST_ID) | python -m json.tool 2>/dev/null || cat; \
	else \
		echo "Please specify REQUEST_ID"; \
	fi

test-stats:
	@echo "Query API stats..."
	@curl -s http://localhost:8081/api/stats | python -m json.tool 2>/dev/null || cat

test-services:
	@echo "Listing tracked services..."
	@curl -s http://localhost:8081/api/services | python -m json.tool 2>/dev/null || cat

test-functions:
	@echo "Listing tracked functions..."
	@curl -s http://localhost:8081/api/functions | python -m json.tool 2>/dev/null || cat

test-waterfall:
	@echo "Usage: make test-waterfall REQUEST_ID=<request-id>"
	@if [ -n "$(REQUEST_ID)" ]; then \
		curl -s http://localhost:8081/api/waterfall/$(REQUEST_ID) | python -m json.tool 2>/dev/null || cat; \
	else \
		echo "Please specify REQUEST_ID"; \
	fi

clean:
	@echo "Cleaning build artifacts..."
	@rm -rf bin/
	@rm -f cmd/ebpf-probe/*.o
	@find . -name "*.go" -path "*/generated/*" -delete 2>/dev/null || true
