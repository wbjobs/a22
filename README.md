# 基于eBPF和WASM的Serverless函数链路追踪系统

## 系统架构

```
┌─────────────┐    ┌──────────────┐    ┌───────────┐    ┌───────────────┐
│  HTTP Client│───▶│  API Gateway │───▶│ FunctionA │───▶│   FunctionB   │
│             │    │    (Go)      │    │   (Go)    │    │  (Node.js)    │
└─────────────┘    └──────────────┘    └───────────┘    └──────┬────────┘
                                                               │
                                                               ▼
                        ┌──────────────────────────────────────────────┐
                        │              FunctionC (Python)              │
                        └──────────────────────────────────────────────┘
                                          │
                                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        eBPF Network Probe                            │
│  (抓取所有Pod进出流量，提取请求ID、函数名、耗时、状态码)               │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
                        ┌──────────────┐
                        │    Kafka     │◀── 追踪数据队列
                        └──────┬───────┘
                               │
                               ▼
                        ┌──────────────┐
                        │ ES Consumer  │
                        └──────┬───────┘
                               │
                               ▼
                        ┌──────────────┐
                        │Elasticsearch │◀── 链路数据存储
                        └──────┬───────┘
                               │
                               ▼
                        ┌──────────────┐
                        │ Query API    │◀── 调用链查询接口
                        └──────────────┘
```

## 组件说明

1. **API Gateway** - 入口网关，生成X-Request-ID，按顺序调用三个函数
2. **FunctionA (Go)** - 订单服务，处理订单逻辑
3. **FunctionB (Node.js)** - 支付服务，处理支付逻辑
4. **FunctionC (Python)** - 通知服务，发送通知
5. **eBPF Probe** - Cilium eBPF程序，抓取TCP流量，解析HTTP头
6. **Kafka Producer** - 接收eBPF数据推送到Kafka
7. **ES Consumer** - 消费Kafka数据写入Elasticsearch
8. **Query API** - 提供调用链查询接口

## 快速开始

### 本地开发环境（使用docker-compose）

```bash
# 启动基础设施
docker-compose up -d zookeeper kafka elasticsearch

# 构建所有服务
docker-compose build

# 启动所有服务
docker-compose up -d
```

### Kubernetes部署

```bash
# 部署Cilium eBPF
kubectl apply -f k8s/cilium/

# 部署基础设施
kubectl apply -f k8s/infra/

# 部署函数服务
kubectl apply -f k8s/functions/

# 部署追踪服务
kubectl apply -f k8s/tracing/
```

## API使用

### 发起测试请求

```bash
curl -X POST http://localhost:8080/api/order \
  -H "Content-Type: application/json" \
  -d '{"product_id": "P123", "amount": 99.99, "user_id": "U456"}'
```

### 查询调用链

```bash
curl http://localhost:8081/api/trace/{request-id}
```
