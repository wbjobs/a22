// +build ignore

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include <string.h>

#define MAX_PAYLOAD 256
#define MAX_ENTRIES 8192
#define HTTP_METHOD_MAX 16
#define HTTP_PATH_MAX 128
#define HTTP_HEADER_MAX 64

char LICENSE[] SEC("license") = "GPL";

struct conn_key {
    __u32 saddr;
    __u32 daddr;
    __u16 sport;
    __u16 dport;
    __u32 seq;
} __attribute__((packed));

struct http_event {
    __u64 timestamp_ns;
    __u32 saddr;
    __u32 daddr;
    __u16 sport;
    __u16 dport;
    __u8 direction;
    __u8 method_len;
    __u16 status_code;
    __u16 payload_len;
    char method[HTTP_METHOD_MAX];
    char path[HTTP_PATH_MAX];
    char request_id[64];
    char trace_id[64];
    char span_id[64];
    char parent_span_id[64];
    char function_name[HTTP_HEADER_MAX];
    char service_name[HTTP_HEADER_MAX];
    char content_type[HTTP_HEADER_MAX];
    __u32 content_length;
    char payload[MAX_PAYLOAD];
} __attribute__((packed));

struct conn_info {
    __u64 request_start_ns;
    char request_id[64];
    char trace_id[64];
    char method[HTTP_METHOD_MAX];
    char path[HTTP_PATH_MAX];
    __u8 method_set;
    __u8 headers_parsed;
} __attribute__((packed));

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, MAX_ENTRIES);
    __type(key, struct conn_key);
    __type(value, struct conn_info);
} conn_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
    __uint(key_size, sizeof(__u32));
    __uint(value_size, sizeof(__u32));
    __uint(max_entries, 1024);
} events SEC(".maps");

static __always_inline int parse_http_headers(void *data, void *data_end,
                                              struct http_event *evt,
                                              struct conn_info *conn) {
    char *p = (char *)data;
    char *end = (char *)data_end;
    int remaining = end - p;
    if (remaining <= 0) return 0;

    int header_len = 0;
    int in_headers = 1;

    while (p + 3 < end && in_headers && header_len < 1500) {
        int line_len = 0;
        char *line_start = p;

        while (p + 1 < end && line_len < 256) {
            if (p[0] == '\r' && p[1] == '\n') {
                break;
            }
            p++;
            line_len++;
        }

        if (p + 1 >= end) break;

        if (line_len == 0) {
            in_headers = 0;
            p += 2;
            break;
        }

        if (conn->method_set == 0 && line_len > 4) {
            if ((line_start[0] == 'G' && line_start[1] == 'E' && line_start[2] == 'T' && line_start[3] == ' ') ||
                (line_start[0] == 'P' && line_start[1] == 'O' && line_start[2] == 'S' && line_start[3] == 'T') ||
                (line_start[0] == 'P' && line_start[1] == 'U' && line_start[2] == 'T' && line_start[3] == ' ') ||
                (line_start[0] == 'D' && line_start[1] == 'E' && line_start[2] == 'L') ||
                (line_start[0] == 'H' && line_start[1] == 'E' && line_start[2] == 'A') ||
                (line_start[0] == 'O' && line_start[1] == 'P')) {

                int m_len = 0;
                char *m_start = line_start;
                while (m_len < HTTP_METHOD_MAX - 1 && m_start + m_len < end && m_start[m_len] != ' ') {
                    m_len++;
                }
                if (m_len > 0) {
                    __builtin_memcpy(conn->method, m_start, m_len);
                    conn->method[m_len] = '\0';
                    evt->method_len = m_len;

                    if (evt->method_len < HTTP_METHOD_MAX) {
                        __builtin_memcpy(evt->method, conn->method, evt->method_len);
                        evt->method[evt->method_len] = '\0';
                    }

                    char *path_start = m_start + m_len + 1;
                    if (path_start < end) {
                        int p_len = 0;
                        while (p_len < HTTP_PATH_MAX - 1 && path_start + p_len < end && path_start[p_len] != ' ') {
                            p_len++;
                        }
                        if (p_len > 0) {
                            __builtin_memcpy(conn->path, path_start, p_len);
                            conn->path[p_len] = '\0';
                            if (p_len < HTTP_PATH_MAX) {
                                __builtin_memcpy(evt->path, conn->path, p_len);
                                evt->path[p_len] = '\0';
                            }
                        }
                    }
                    conn->method_set = 1;
                }
            } else if (line_start[0] == 'H' && line_start[1] == 'T' && line_start[2] == 'T' && line_start[3] == 'P') {
                char *code_start = line_start;
                while (code_start < end && *code_start != ' ') code_start++;
                if (code_start + 4 < end) {
                    code_start++;
                    evt->status_code = (code_start[0] - '0') * 100 +
                                       (code_start[1] - '0') * 10 +
                                       (code_start[2] - '0');
                }
            }
        }

        if (line_len > 12 && line_start + 12 < end) {
            int match = 1;
            char *hdr_name = "X-Request-ID:";
            for (int i = 0; i < 13; i++) {
                if (i < line_len && (line_start[i] | 32) != (hdr_name[i] | 32)) {
                    match = 0;
                    break;
                }
            }
            if (match) {
                char *v_start = line_start + 13;
                while (v_start < end && *v_start == ' ') v_start++;
                int v_len = 0;
                while (v_len < 63 && v_start + v_len < p && v_start[v_len] != '\r') {
                    conn->request_id[v_len] = v_start[v_len];
                    evt->request_id[v_len] = v_start[v_len];
                    v_len++;
                }
                conn->request_id[v_len] = '\0';
                evt->request_id[v_len] = '\0';
            }
        }

        if (line_len > 11 && line_start + 11 < end) {
            int match = 1;
            char *hdr_name = "X-Trace-ID:";
            for (int i = 0; i < 11; i++) {
                if (i < line_len && (line_start[i] | 32) != (hdr_name[i] | 32)) {
                    match = 0;
                    break;
                }
            }
            if (match) {
                char *v_start = line_start + 11;
                while (v_start < end && *v_start == ' ') v_start++;
                int v_len = 0;
                while (v_len < 63 && v_start + v_len < p && v_start[v_len] != '\r') {
                    conn->trace_id[v_len] = v_start[v_len];
                    evt->trace_id[v_len] = v_start[v_len];
                    v_len++;
                }
                conn->trace_id[v_len] = '\0';
                evt->trace_id[v_len] = '\0';
            }
        }

        if (line_len > 10 && line_start + 10 < end) {
            int match = 1;
            char *hdr_name = "X-Span-ID:";
            for (int i = 0; i < 10; i++) {
                if (i < line_len && (line_start[i] | 32) != (hdr_name[i] | 32)) {
                    match = 0;
                    break;
                }
            }
            if (match) {
                char *v_start = line_start + 10;
                while (v_start < end && *v_start == ' ') v_start++;
                int v_len = 0;
                while (v_len < 63 && v_start + v_len < p && v_start[v_len] != '\r') {
                    evt->span_id[v_len] = v_start[v_len];
                    v_len++;
                }
                evt->span_id[v_len] = '\0';
            }
        }

        if (line_len > 17 && line_start + 17 < end) {
            int match = 1;
            char *hdr_name = "X-Parent-Span-ID:";
            for (int i = 0; i < 17; i++) {
                if (i < line_len && (line_start[i] | 32) != (hdr_name[i] | 32)) {
                    match = 0;
                    break;
                }
            }
            if (match) {
                char *v_start = line_start + 17;
                while (v_start < end && *v_start == ' ') v_start++;
                int v_len = 0;
                while (v_len < 63 && v_start + v_len < p && v_start[v_len] != '\r') {
                    evt->parent_span_id[v_len] = v_start[v_len];
                    v_len++;
                }
                evt->parent_span_id[v_len] = '\0';
            }
        }

        if (line_len > 16 && line_start + 16 < end) {
            int match = 1;
            char *hdr_name = "X-Function-Name:";
            for (int i = 0; i < 16; i++) {
                if (i < line_len && (line_start[i] | 32) != (hdr_name[i] | 32)) {
                    match = 0;
                    break;
                }
            }
            if (match) {
                char *v_start = line_start + 16;
                while (v_start < end && *v_start == ' ') v_start++;
                int v_len = 0;
                while (v_len < HTTP_HEADER_MAX - 1 && v_start + v_len < p && v_start[v_len] != '\r') {
                    evt->function_name[v_len] = v_start[v_len];
                    v_len++;
                }
                evt->function_name[v_len] = '\0';
            }
        }

        if (line_len > 15 && line_start + 15 < end) {
            int match = 1;
            char *hdr_name = "X-Service-Name:";
            for (int i = 0; i < 15; i++) {
                if (i < line_len && (line_start[i] | 32) != (hdr_name[i] | 32)) {
                    match = 0;
                    break;
                }
            }
            if (match) {
                char *v_start = line_start + 15;
                while (v_start < end && *v_start == ' ') v_start++;
                int v_len = 0;
                while (v_len < HTTP_HEADER_MAX - 1 && v_start + v_len < p && v_start[v_len] != '\r') {
                    evt->service_name[v_len] = v_start[v_len];
                    v_len++;
                }
                evt->service_name[v_len] = '\0';
            }
        }

        if (line_len > 15 && line_start + 15 < end) {
            int match = 1;
            char *hdr_name = "Content-Length:";
            for (int i = 0; i < 15; i++) {
                if (i < line_len && (line_start[i] | 32) != (hdr_name[i] | 32)) {
                    match = 0;
                    break;
                }
            }
            if (match) {
                char *v_start = line_start + 15;
                while (v_start < end && *v_start == ' ') v_start++;
                __u32 cl = 0;
                while (v_start < p && *v_start >= '0' && *v_start <= '9') {
                    cl = cl * 10 + (*v_start - '0');
                    v_start++;
                }
                evt->content_length = cl;
            }
        }

        if (line_len > 14 && line_start + 14 < end) {
            int match = 1;
            char *hdr_name = "Content-Type:";
            for (int i = 0; i < 13; i++) {
                if (i < line_len && (line_start[i] | 32) != (hdr_name[i] | 32)) {
                    match = 0;
                    break;
                }
            }
            if (match) {
                char *v_start = line_start + 13;
                while (v_start < end && *v_start == ' ') v_start++;
                int v_len = 0;
                while (v_len < HTTP_HEADER_MAX - 1 && v_start + v_len < p && v_start[v_len] != '\r' && v_start[v_len] != ';') {
                    evt->content_type[v_len] = v_start[v_len];
                    v_len++;
                }
                evt->content_type[v_len] = '\0';
            }
        }

        p += 2;
        header_len += line_len + 2;
    }

    conn->headers_parsed = 1;

    if (p < end) {
        int body_len = end - p;
        if (body_len > MAX_PAYLOAD) body_len = MAX_PAYLOAD;
        if (body_len > 0) {
            __builtin_memcpy(evt->payload, p, body_len);
            evt->payload_len = body_len;
        }
    }

    return 1;
}

SEC("tc")
int trace_egress(struct __sk_buff *skb) {
    void *data = (void *)(long)skb->data;
    void *data_end = (void *)(long)skb->data_end;

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) return TC_ACT_OK;
    if (eth->h_proto != __constant_htons(ETH_P_IP)) return TC_ACT_OK;

    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end) return TC_ACT_OK;
    if (ip->protocol != IPPROTO_TCP) return TC_ACT_OK;

    struct tcphdr *tcp = (void *)ip + (ip->ihl * 4);
    if ((void *)(tcp + 1) > data_end) return TC_ACT_OK;

    void *payload = (void *)tcp + (tcp->doff * 4);
    int payload_len = data_end - payload;
    if (payload_len <= 0) return TC_ACT_OK;

    struct conn_key key = {};
    key.saddr = ip->saddr;
    key.daddr = ip->daddr;
    key.sport = tcp->source;
    key.dport = tcp->dest;
    key.seq = tcp->seq;

    struct conn_info *conn = bpf_map_lookup_elem(&conn_map, &key);
    struct conn_info new_conn = {};

    if (!conn) {
        new_conn.request_start_ns = bpf_ktime_get_ns();
        bpf_map_update_elem(&conn_map, &key, &new_conn, BPF_ANY);
        conn = bpf_map_lookup_elem(&conn_map, &key);
        if (!conn) return TC_ACT_OK;
    }

    if (payload_len > 3 &&
        (((char *)payload)[0] == 'G' || ((char *)payload)[0] == 'P' ||
         ((char *)payload)[0] == 'D' || ((char *)payload)[0] == 'H' ||
         ((char *)payload)[0] == 'O' || ((char *)payload)[0] == 'T' ||
         ((char *)payload)[0] == 'h' || ((char *)payload)[0] == 'H'))) {

        struct http_event evt = {};
        evt.timestamp_ns = bpf_ktime_get_ns();
        evt.saddr = ip->saddr;
        evt.daddr = ip->daddr;
        evt.sport = tcp->source;
        evt.dport = tcp->dest;
        evt.direction = 1;

        parse_http_headers(payload, data_end, &evt, conn);

        if (evt.request_id[0] != '\0' || evt.method[0] != '\0' || evt.status_code > 0) {
            bpf_perf_event_output(skb, &events, BPF_F_CURRENT_CPU, &evt, sizeof(evt));
        }
    }

    return TC_ACT_OK;
}

SEC("tc")
int trace_ingress(struct __sk_buff *skb) {
    void *data = (void *)(long)skb->data;
    void *data_end = (void *)(long)skb->data_end;

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) return TC_ACT_OK;
    if (eth->h_proto != __constant_htons(ETH_P_IP)) return TC_ACT_OK;

    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end) return TC_ACT_OK;
    if (ip->protocol != IPPROTO_TCP) return TC_ACT_OK;

    struct tcphdr *tcp = (void *)ip + (ip->ihl * 4);
    if ((void *)(tcp + 1) > data_end) return TC_ACT_OK;

    void *payload = (void *)tcp + (tcp->doff * 4);
    int payload_len = data_end - payload;
    if (payload_len <= 0) return TC_ACT_OK;

    struct conn_key key = {};
    key.saddr = ip->daddr;
    key.daddr = ip->saddr;
    key.sport = tcp->dest;
    key.dport = tcp->source;
    key.seq = tcp->ack_seq;

    struct conn_info *conn = bpf_map_lookup_elem(&conn_map, &key);
    struct conn_info new_conn = {};

    if (!conn) {
        new_conn.request_start_ns = bpf_ktime_get_ns();
        bpf_map_update_elem(&conn_map, &key, &new_conn, BPF_ANY);
        conn = bpf_map_lookup_elem(&conn_map, &key);
        if (!conn) return TC_ACT_OK;
    }

    if (payload_len > 3 &&
        (((char *)payload)[0] == 'G' || ((char *)payload)[0] == 'P' ||
         ((char *)payload)[0] == 'D' || ((char *)payload)[0] == 'H' ||
         ((char *)payload)[0] == 'O' || ((char *)payload)[0] == 'T' ||
         ((char *)payload)[0] == 'h' || ((char *)payload)[0] == 'H'))) {

        struct http_event evt = {};
        evt.timestamp_ns = bpf_ktime_get_ns();
        evt.saddr = ip->saddr;
        evt.daddr = ip->daddr;
        evt.sport = tcp->source;
        evt.dport = tcp->dest;
        evt.direction = 0;

        parse_http_headers(payload, data_end, &evt, conn);

        if (evt.request_id[0] != '\0' || evt.method[0] != '\0' || evt.status_code > 0) {
            bpf_perf_event_output(skb, &events, BPF_F_CURRENT_CPU, &evt, sizeof(evt));
        }
    }

    return TC_ACT_OK;
}

SEC("kprobe/tcp_sendmsg")
int BPF_KPROBE(tcp_sendmsg_entry, struct sock *sk, struct msghdr *msg, size_t size) {
    return 0;
}
