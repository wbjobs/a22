module ebpf-serverless-tracing

go 1.21

require (
	github.com/elastic/go-elasticsearch/v8 v8.13.1
	github.com/gin-gonic/gin v1.9.1
	github.com/google/uuid v1.6.0
	github.com/segmentio/kafka-go v0.4.48
	github.com/cilium/ebpf v0.12.3
	github.com/vishvananda/netlink v1.1.0
	golang.org/x/sys v0.18.0
	google.golang.org/protobuf v1.34.1
)
