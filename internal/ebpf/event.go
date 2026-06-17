package ebpf

import (
	"encoding/binary"
	"fmt"
	"net"
)

type HTTPEvent struct {
	TimestampNs   uint64
	SrcIP         net.IP
	DstIP         net.IP
	SrcPort       uint16
	DstPort       uint16
	Direction     uint8
	Method        string
	Path          string
	StatusCode    uint16
	RequestID     string
	TraceID       string
	SpanID        string
	ParentSpanID  string
	FunctionName  string
	ServiceName   string
	ContentType   string
	ContentLength uint32
	Payload       []byte
}

func ParseHTTPEvent(raw []byte) (*HTTPEvent, error) {
	if len(raw) < 720 {
		return nil, fmt.Errorf("event data too short: %d bytes", len(raw))
	}

	evt := &HTTPEvent{}
	offset := 0

	evt.TimestampNs = binary.LittleEndian.Uint64(raw[offset:offset+8])
	offset += 8

	saddr := binary.LittleEndian.Uint32(raw[offset:offset+4])
	offset += 4
	daddr := binary.LittleEndian.Uint32(raw[offset:offset+4])
	offset += 4

	evt.SrcIP = make(net.IP, 4)
	binary.LittleEndian.PutUint32(evt.SrcIP, saddr)
	evt.SrcIP = net.IPv4(evt.SrcIP[0], evt.SrcIP[1], evt.SrcIP[2], evt.SrcIP[3])

	evt.DstIP = make(net.IP, 4)
	binary.LittleEndian.PutUint32(evt.DstIP, daddr)
	evt.DstIP = net.IPv4(evt.DstIP[0], evt.DstIP[1], evt.DstIP[2], evt.DstIP[3])

	evt.SrcPort = binary.LittleEndian.Uint16(raw[offset:offset+2])
	offset += 2
	evt.DstPort = binary.LittleEndian.Uint16(raw[offset:offset+2])
	offset += 2

	evt.Direction = raw[offset]
	offset++

	methodLen := uint8(raw[offset])
	offset++

	evt.StatusCode = binary.LittleEndian.Uint16(raw[offset:offset+2])
	offset += 2

	payloadLen := binary.LittleEndian.Uint16(raw[offset:offset+2])
	offset += 2

	methodBytes := raw[offset:offset+16]
	offset += 16
	if methodLen > 0 && int(methodLen) <= 16 {
		evt.Method = string(methodBytes[:methodLen])
	}

	pathBytes := raw[offset:offset+128]
	offset += 128
	evt.Path = cString(pathBytes)

	reqIDBytes := raw[offset:offset+64]
	offset += 64
	evt.RequestID = cString(reqIDBytes)

	traceIDBytes := raw[offset:offset+64]
	offset += 64
	evt.TraceID = cString(traceIDBytes)

	spanIDBytes := raw[offset:offset+64]
	offset += 64
	evt.SpanID = cString(spanIDBytes)

	parentSpanIDBytes := raw[offset:offset+64]
	offset += 64
	evt.ParentSpanID = cString(parentSpanIDBytes)

	funcNameBytes := raw[offset:offset+64]
	offset += 64
	evt.FunctionName = cString(funcNameBytes)

	svcNameBytes := raw[offset:offset+64]
	offset += 64
	evt.ServiceName = cString(svcNameBytes)

	contentTypeBytes := raw[offset:offset+64]
	offset += 64
	evt.ContentType = cString(contentTypeBytes)

	evt.ContentLength = binary.LittleEndian.Uint32(raw[offset:offset+4])
	offset += 4

	if payloadLen > 0 && int(payloadLen) <= 256 {
		evt.Payload = make([]byte, payloadLen)
		copy(evt.Payload, raw[offset:offset+int(payloadLen)])
	}

	return evt, nil
}

func cString(b []byte) string {
	for i, v := range b {
		if v == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}

func (e *HTTPEvent) IsRequest() bool {
	return len(e.Method) > 0 && e.StatusCode == 0
}

func (e *HTTPEvent) IsResponse() bool {
	return e.StatusCode > 0
}

func (e *HTTPEvent) DirectionStr() string {
	if e.Direction == 1 {
		return "egress"
	}
	return "ingress"
}

func (e *HTTPEvent) String() string {
	return fmt.Sprintf(
		"[HTTP %s] %s:%d -> %s:%d | method=%s path=%s status=%d | req=%s trace=%s | fn=%s svc=%s",
		e.DirectionStr(),
		e.SrcIP, e.SrcPort,
		e.DstIP, e.DstPort,
		e.Method, e.Path, e.StatusCode,
		e.RequestID, e.TraceID,
		e.FunctionName, e.ServiceName,
	)
}
