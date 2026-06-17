import os
import sys
import json
import time
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from collections import deque

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("lstm-predictor")

try:
    import torch
    import torch.nn as nn
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False
    logger.warning("PyTorch not available, falling back to statistical prediction")

from elasticsearch import Elasticsearch
from notifier import MultiNotifier


FEATURE_DIM = 6
SEQ_LEN = 12
PRED_WINDOW_MIN = 5


class LSTMPredictor(nn.Module if HAS_TORCH else object):
    def __init__(self, input_size: int = FEATURE_DIM, hidden_size: int = 64,
                 num_layers: int = 2, output_size: int = 1):
        if HAS_TORCH:
            super().__init__()
            self.hidden_size = hidden_size
            self.num_layers = num_layers
            self.lstm = nn.LSTM(input_size, hidden_size, num_layers,
                                batch_first=True, dropout=0.2)
            self.fc = nn.Sequential(
                nn.Linear(hidden_size, 32),
                nn.ReLU(),
                nn.Dropout(0.2),
                nn.Linear(32, output_size),
                nn.Sigmoid()
            )
            self._init_weights()
        self.rng = np.random.RandomState(42)

    if HAS_TORCH:
        def _init_weights(self):
            for name, param in self.lstm.named_parameters():
                if 'weight' in name:
                    nn.init.xavier_uniform_(param)
                elif 'bias' in name:
                    nn.init.zeros_(param)

        def forward(self, x):
            h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size)
            c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size)
            out, _ = self.lstm(x, (h0, c0))
            out = self.fc(out[:, -1, :])
            return out.squeeze()


class ServicePredictionState:
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.history: deque = deque(maxlen=60)
        self.features: deque = deque(maxlen=SEQ_LEN)
        self.last_prediction = 0.0
        self.last_alert_time = 0
        self.total_requests = 0
        self.total_timeouts = 0
        self.timeout_window = deque(maxlen=60)


class FaultPredictorService:
    def __init__(self):
        self.es_url = os.environ.get("ELASTICSEARCH_URL", "http://elasticsearch:9200")
        self.es_index = os.environ.get("ES_INDEX", "trace-spans")
        self.predict_interval = int(os.environ.get("PREDICT_INTERVAL", 30))
        self.timeout_threshold_ms = int(os.environ.get("TIMEOUT_THRESHOLD_MS", 1000))
        self.alert_threshold = float(os.environ.get("ALERT_THRESHOLD", 0.7))

        self.services: Dict[str, ServicePredictionState] = {}
        self.notifier = MultiNotifier()

        dt_webhook = os.environ.get("DINGTALK_WEBHOOK", "")
        dt_secret = os.environ.get("DINGTALK_SECRET", "")
        if dt_webhook:
            self.notifier.add_dingtalk(dt_webhook, dt_secret)

        fs_webhook = os.environ.get("FEISHU_WEBHOOK", "")
        fs_secret = os.environ.get("FEISHU_SECRET", "")
        if fs_webhook:
            self.notifier.add_feishu(fs_webhook, fs_secret)

        self.es: Optional[Elasticsearch] = None
        self.models: Dict[str, LSTMPredictor] = {}

        self._init_es()
        self._init_models()

    def _init_es(self):
        for attempt in range(30):
            try:
                self.es = Elasticsearch(self.es_url)
                if self.es.ping():
                    logger.info(f"Connected to Elasticsearch at {self.es_url}")
                    return
            except Exception as e:
                logger.warning(f"ES connection attempt {attempt + 1} failed: {e}")
            time.sleep(2)
        logger.warning("Could not connect to Elasticsearch, will retry later")

    def _init_models(self):
        for svc in ["gateway", "function-a", "function-b", "function-c"]:
            self.models[svc] = LSTMPredictor()
            self.services[svc] = ServicePredictionState(svc)
            if HAS_TORCH:
                self.models[svc].eval()
        logger.info(f"Initialized {len(self.models)} prediction models")

    def _ensure_es(self) -> bool:
        if self.es is None or not self.es.ping():
            try:
                self.es = Elasticsearch(self.es_url)
                return self.es.ping()
            except Exception:
                return False
        return True

    def collect_service_metrics(self, service: str, minutes: int = 60) -> Optional[Dict]:
        if not self._ensure_es():
            return None
        try:
            now = datetime.utcnow()
            start_time = now - timedelta(minutes=minutes)

            query = {
                "size": 0,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"service_name": service}},
                            {"range": {"timestamp": {"gte": start_time.isoformat()}}}
                        ]
                    }
                },
                "aggs": {
                    "per_minute": {
                        "date_histogram": {
                            "field": "timestamp",
                            "fixed_interval": "1m",
                            "min_doc_count": 0
                        },
                        "aggs": {
                            "avg_dur": {"avg": {"field": "duration_ms"}},
                            "p95_dur": {"percentiles": {"field": "duration_ms", "percents": [95]}},
                            "max_dur": {"max": {"field": "duration_ms"}},
                            "err_count": {
                                "filter": {"range": {"status_code": {"gte": 500}}}
                            },
                            "timeout_count": {
                                "filter": {"range": {"duration_ms": {"gte": self.timeout_threshold_ms}}}
                            }
                        }
                    },
                    "total_avg": {"avg": {"field": "duration_ms"}},
                    "total_err": {
                        "filter": {"range": {"status_code": {"gte": 500}}}
                    },
                    "total_timeout": {
                        "filter": {"range": {"duration_ms": {"gte": self.timeout_threshold_ms}}}
                    }
                }
            }

            resp = self.es.search(index=self.es_index, body=query)
            aggs = resp.get("aggregations", {})
            buckets = aggs.get("per_minute", {}).get("buckets", [])

            total_count = resp.get("hits", {}).get("total", {}).get("value", 0)
            if total_count == 0:
                return None

            sequences = []
            for b in buckets:
                doc_count = b.get("doc_count", 0)
                avg_d = b.get("avg_dur", {}).get("value", 0) or 0
                p95_values = b.get("p95_dur", {}).get("values", {})
                p95_d = list(p95_values.values())[0] if p95_values else 0
                max_d = b.get("max_dur", {}).get("value", 0) or 0
                err_c = b.get("err_count", {}).get("doc_count", 0)
                timeout_c = b.get("timeout_count", {}).get("doc_count", 0)

                qps = doc_count / 60.0
                err_rate = err_c / doc_count if doc_count > 0 else 0
                timeout_rate = timeout_c / doc_count if doc_count > 0 else 0
                norm_avg = min(avg_d / self.timeout_threshold_ms, 2.0)
                norm_p95 = min(p95_d / self.timeout_threshold_ms, 2.0)

                sequences.append([qps, err_rate, timeout_rate, norm_avg, norm_p95, doc_count])

            total_avg = aggs.get("total_avg", {}).get("value", 0) or 0
            total_err_count = aggs.get("total_err", {}).get("doc_count", 0)
            total_timeout_count = aggs.get("total_timeout", {}).get("doc_count", 0)
            err_rate = total_err_count / total_count if total_count > 0 else 0
            timeout_rate = total_timeout_count / total_count if total_count > 0 else 0

            return {
                "service": service,
                "total_requests": total_count,
                "avg_latency_ms": total_avg,
                "error_rate": err_rate,
                "timeout_rate": timeout_rate,
                "sequences": sequences[-SEQ_LEN:] if len(sequences) >= SEQ_LEN else sequences,
                "window_minutes": minutes
            }

        except Exception as e:
            logger.error(f"Failed to collect metrics for {service}: {e}")
            return None

    def predict_timeout(self, service: str, metrics: Dict) -> Tuple[float, Dict]:
        state = self.services.get(service) or ServicePredictionState(service)
        seqs = metrics.get("sequences", [])

        features = None
        if len(seqs) >= SEQ_LEN:
            features = np.array(seqs[-SEQ_LEN:], dtype=np.float32)
        elif len(seqs) > 0:
            padded = np.zeros((SEQ_LEN, FEATURE_DIM), dtype=np.float32)
            padded[-len(seqs):] = seqs
            features = padded

        probability = 0.0
        if features is not None and HAS_TORCH and service in self.models:
            try:
                model = self.models[service]
                tensor = torch.FloatTensor(features).unsqueeze(0)
                with torch.no_grad():
                    output = model(tensor)
                    probability = float(output.item() if output.dim() == 0 else output[0].item())
            except Exception as e:
                logger.error(f"LSTM inference failed for {service}: {e}, falling back")
                probability = self._statistical_predict(metrics)
        else:
            probability = self._statistical_predict(metrics)

        avg_latency = metrics.get("avg_latency_ms", 0)
        err_rate = metrics.get("error_rate", 0)
        timeout_rate = metrics.get("timeout_rate", 0)
        total_req = metrics.get("total_requests", 0)

        if timeout_rate > 0.1:
            probability = max(probability, 0.9)
        elif err_rate > 0.05:
            probability = max(probability, 0.7)
        elif avg_latency > self.timeout_threshold_ms * 0.8:
            probability = max(probability, 0.6)

        probability = min(max(probability, 0.0), 1.0)
        state.last_prediction = probability

        explanation = []
        if timeout_rate > 0.05:
            explanation.append(f"历史超时率 {timeout_rate:.1%} 偏高")
        if err_rate > 0.03:
            explanation.append(f"错误率 {err_rate:.1%} 超过阈值")
        if avg_latency > self.timeout_threshold_ms * 0.7:
            explanation.append(f"平均延迟 {avg_latency:.0f}ms 接近超时阈值")
        if total_req < 10:
            explanation.append("样本量不足，预测仅供参考")

        details = "; ".join(explanation) if explanation else "基于历史时序特征预测"

        return probability, {
            "service": service,
            "predicted_timeout_probability": probability,
            "predicted_timeout": probability >= self.alert_threshold,
            "confidence": probability,
            "avg_latency_ms": avg_latency,
            "error_rate": err_rate,
            "timeout_rate": timeout_rate,
            "total_requests": total_req,
            "threshold_ms": self.timeout_threshold_ms,
            "details": details,
            "model": "LSTM" if HAS_TORCH and features is not None else "Statistical",
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

    def _statistical_predict(self, metrics: Dict) -> float:
        avg_latency = metrics.get("avg_latency_ms", 0)
        err_rate = metrics.get("error_rate", 0)
        timeout_rate = metrics.get("timeout_rate", 0)

        p_latency = min(avg_latency / self.timeout_threshold_ms, 1.0)
        p_error = min(err_rate / 0.1, 1.0)
        p_timeout = min(timeout_rate / 0.05, 1.0)

        return 0.4 * p_latency + 0.3 * p_error + 0.3 * p_timeout

    def run_prediction_cycle(self):
        logger.info(f"Starting prediction cycle, interval={self.predict_interval}s")
        while True:
            try:
                results = []
                for service in list(self.services.keys()):
                    metrics = self.collect_service_metrics(service)
                    if metrics is None:
                        continue
                    prob, details = self.predict_timeout(service, metrics)
                    results.append(details)

                    if details["predicted_timeout"]:
                        logger.warning(
                            f"⚠️  PREDICTED TIMEOUT for {service}: "
                            f"prob={prob:.2%} avg_latency={metrics.get('avg_latency_ms', 0):.0f}ms "
                            f"err_rate={metrics.get('error_rate', 0):.1%}"
                        )
                        sent = self.notifier.send_alert(
                            service=service,
                            predicted_timeout=True,
                            confidence=prob,
                            avg_latency=metrics.get("avg_latency_ms", 0),
                            error_rate=metrics.get("error_rate", 0),
                            details=details.get("details", "")
                        )
                        if sent > 0:
                            logger.info(f"Alert sent for {service} via {sent} channel(s)")
                    else:
                        logger.info(
                            f"✓ {service}: prob={prob:.2%} "
                            f"avg={metrics.get('avg_latency_ms', 0):.0f}ms"
                        )

                self._last_results = results
                self._last_run_time = datetime.utcnow()

            except Exception as e:
                logger.error(f"Prediction cycle error: {e}", exc_info=True)

            time.sleep(self.predict_interval)

    def get_latest_results(self) -> Dict:
        return {
            "timestamp": getattr(self, "_last_run_time", datetime.utcnow()).isoformat() + "Z",
            "predict_interval_seconds": self.predict_interval,
            "timeout_threshold_ms": self.timeout_threshold_ms,
            "alert_threshold": self.alert_threshold,
            "services": getattr(self, "_last_results", [])
        }


def main():
    logger.info("=" * 60)
    logger.info("LSTM Fault Prediction Service starting...")
    logger.info(f"PyTorch available: {HAS_TORCH}")
    service = FaultPredictorService()
    service.run_prediction_cycle()


if __name__ == "__main__":
    main()
