import os
import sys
import time
import logging
import threading
from flask import Flask, jsonify, request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from predictor import FaultPredictorService, HAS_TORCH

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("predictor-api")

app = Flask(__name__)
predictor: FaultPredictorService = None
predictor_thread: threading.Thread = None


@app.route("/health")
def health():
    status = "healthy"
    es_ok = False
    try:
        if predictor and predictor.es:
            es_ok = predictor.es.ping()
    except Exception:
        pass
    return jsonify({
        "service": "ai-fault-predictor",
        "status": status,
        "pytorch_available": HAS_TORCH,
        "elasticsearch_connected": es_ok,
        "prediction_thread_running": predictor_thread.is_alive() if predictor_thread else False,
        "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    })


@app.route("/api/predictions")
def get_predictions():
    if predictor is None:
        return jsonify({"error": "predictor not initialized"}), 503
    return jsonify(predictor.get_latest_results())


@app.route("/api/predictions/<service>")
def get_prediction_for_service(service: str):
    if predictor is None:
        return jsonify({"error": "predictor not initialized"}), 503

    metrics = predictor.collect_service_metrics(service)
    if metrics is None:
        return jsonify({
            "service": service,
            "error": "no data available",
            "predicted_timeout_probability": 0.0,
            "predicted_timeout": False
        })

    prob, details = predictor.predict_timeout(service, metrics)
    return jsonify(details)


@app.route("/api/metrics/<service>")
def get_metrics_for_service(service: str):
    if predictor is None:
        return jsonify({"error": "predictor not initialized"}), 503
    minutes = int(request.args.get("minutes", 60))
    metrics = predictor.collect_service_metrics(service, minutes)
    if metrics is None:
        return jsonify({"error": "no data", "service": service}), 404
    return jsonify(metrics)


@app.route("/api/alert", methods=["POST"])
def trigger_alert():
    if predictor is None:
        return jsonify({"error": "predictor not initialized"}), 503
    data = request.get_json(silent=True) or {}
    service = data.get("service", "unknown")
    confidence = float(data.get("confidence", 0.8))
    avg_latency = float(data.get("avg_latency", 0))
    error_rate = float(data.get("error_rate", 0))
    details = data.get("details", "Manual test alert")

    sent = predictor.notifier.send_alert(
        service=service,
        predicted_timeout=True,
        confidence=confidence,
        avg_latency=avg_latency,
        error_rate=error_rate,
        details=details,
        cooldown_seconds=0
    )
    return jsonify({
        "status": "sent" if sent > 0 else "failed",
        "channels_notified": sent,
        "service": service
    })


@app.route("/api/config", methods=["GET", "POST"])
def config():
    if predictor is None:
        return jsonify({"error": "predictor not initialized"}), 503
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        if "timeout_threshold_ms" in data:
            predictor.timeout_threshold_ms = int(data["timeout_threshold_ms"])
        if "alert_threshold" in data:
            predictor.alert_threshold = float(data["alert_threshold"])
        if "predict_interval" in data:
            predictor.predict_interval = int(data["predict_interval"])
        return jsonify({"status": "updated"})
    return jsonify({
        "timeout_threshold_ms": predictor.timeout_threshold_ms,
        "alert_threshold": predictor.alert_threshold,
        "predict_interval_seconds": predictor.predict_interval
    })


def start_predictor():
    global predictor, predictor_thread
    predictor = FaultPredictorService()
    predictor_thread = threading.Thread(target=predictor.run_prediction_cycle, daemon=True)
    predictor_thread.start()
    logger.info("Prediction background thread started")


if __name__ == "__main__":
    port = int(os.environ.get("PREDICTOR_PORT", "8087"))
    start_predictor()
    logger.info(f"AI Fault Predictor API starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
