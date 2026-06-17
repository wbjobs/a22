import os
import time
import json
import random
import logging
import threading
from datetime import datetime, timedelta
from flask import request, jsonify

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

logger = logging.getLogger("chaos")


class ChaosEngine:
    def __init__(self, service_name=None, manager_url=None):
        self.service_name = service_name or os.environ.get("CHAOS_SERVICE_NAME") or os.environ.get("FUNCTION_NAME") or "unknown"
        self.manager_url = manager_url or os.environ.get("CHAOS_MANAGER_URL", "http://chaos-manager:8088")
        self.rules = {}
        self.hit_count = {}
        self._lock = threading.Lock()

        if HAS_REQUESTS and self.manager_url:
            self._start_sync()

        self._start_cleanup()
        logger.info(f"[Chaos] Engine initialized for service={self.service_name}")

    def _start_sync(self):
        def sync_loop():
            while True:
                try:
                    url = f"{self.manager_url}/api/rules?service={self.service_name}"
                    resp = requests.get(url, timeout=3)
                    if resp.status_code == 200:
                        data = resp.json()
                        if isinstance(data, list):
                            now = time.time()
                            with self._lock:
                                for rule in data:
                                    if rule.get("enabled") and rule.get("expires_at"):
                                        try:
                                            exp = datetime.fromisoformat(rule["expires_at"].replace("Z", "")).timestamp()
                                            if exp > now:
                                                self.rules[rule["id"]] = rule
                                            else:
                                                self.rules.pop(rule["id"], None)
                                        except Exception:
                                            pass
                except Exception:
                    pass
                time.sleep(10)

        t = threading.Thread(target=sync_loop, daemon=True)
        t.start()

    def _start_cleanup(self):
        def cleanup_loop():
            while True:
                with self._lock:
                    now = time.time()
                    to_remove = []
                    for rid, rule in self.rules.items():
                        if rule.get("expires_at"):
                            try:
                                exp = datetime.fromisoformat(rule["expires_at"].replace("Z", "")).timestamp()
                                if exp <= now:
                                    to_remove.append(rid)
                            except Exception:
                                pass
                    for rid in to_remove:
                        self.rules.pop(rid, None)
                        logger.info(f"[Chaos] Rule expired and removed: id={rid}")
                time.sleep(10)

        t = threading.Thread(target=cleanup_loop, daemon=True)
        t.start()

    def add_rule(self, rule):
        rule["id"] = rule.get("id") or f"rule-{int(time.time()*1000)}"
        rule["created_at"] = datetime.utcnow().isoformat() + "Z"
        if rule.get("duration_sec") and not rule.get("expires_at"):
            rule["expires_at"] = (datetime.utcnow() + timedelta(seconds=int(rule["duration_sec"]))).isoformat() + "Z"
        rule["enabled"] = True
        with self._lock:
            self.rules[rule["id"]] = rule
        logger.info(f"[Chaos] Rule added: id={rule['id']} type={rule.get('type')} prob={rule.get('probability', 1.0)}")
        return rule

    def remove_rule(self, rule_id):
        with self._lock:
            self.rules.pop(rule_id, None)
        logger.info(f"[Chaos] Rule removed: id={rule_id}")

    def clear_rules(self):
        with self._lock:
            self.rules.clear()
        logger.info("[Chaos] All rules cleared")

    def list_rules(self):
        with self._lock:
            return list(self.rules.values())

    def _find_matching_rule(self):
        now = time.time()
        with self._lock:
            for rule in self.rules.values():
                if not rule.get("enabled"):
                    continue
                if rule.get("expires_at"):
                    try:
                        exp = datetime.fromisoformat(rule["expires_at"].replace("Z", "")).timestamp()
                        if exp <= now:
                            continue
                    except Exception:
                        pass
                if rule.get("paths"):
                    if request.path not in rule["paths"]:
                        continue
                if rule.get("headers"):
                    matched = True
                    for k, v in rule["headers"].items():
                        if request.headers.get(k) != v:
                            matched = False
                            break
                    if not matched:
                        continue
                prob = float(rule.get("probability", 1.0))
                if random.random() < prob:
                    return rule
        return None

    def middleware(self):
        def chaos_middleware():
            rule = self._find_matching_rule()
            if not rule:
                return None

            rid = rule["id"]
            with self._lock:
                self.hit_count[rid] = self.hit_count.get(rid, 0) + 1
            logger.info(f"[Chaos] Injecting {rule.get('type')} for {request.method} {request.path} (rule={rid})")

            rtype = rule.get("type")
            if rtype == "latency":
                ms = int(rule.get("latency_ms", 500))
                time.sleep(ms / 1000.0)
                return None

            if rtype == "error":
                status = int(rule.get("status_code", 500))
                msg = rule.get("message") or "Chaos Engineering: Injected Error"
                return jsonify({
                    "error": msg,
                    "chaos_rule": rid,
                    "chaos_type": rtype
                }), status

            if rtype == "abort":
                status = int(rule.get("status_code", 503))
                return ("", status)

            if rtype == "exception":
                msg = rule.get("message") or "Chaos Engineering: Simulated Exception"
                raise Exception(msg)

            return None

        return chaos_middleware

    def register_admin_routes(self, app):
        @app.route("/admin/chaos/rules", methods=["GET"])
        def list_rules_route():
            return jsonify({"service": self.service_name, "rules": self.list_rules()})

        @app.route("/admin/chaos/rules", methods=["POST"])
        def add_rule_route():
            data = request.get_json(silent=True) or {}
            data["service_name"] = self.service_name
            data.setdefault("probability", 1.0)
            data["enabled"] = True
            rule = self.add_rule(data)
            return jsonify(rule), 201

        @app.route("/admin/chaos/rules/<rule_id>", methods=["DELETE"])
        def remove_rule_route(rule_id):
            self.remove_rule(rule_id)
            return jsonify({"status": "removed", "id": rule_id})

        @app.route("/admin/chaos/clear", methods=["POST"])
        def clear_rules_route():
            self.clear_rules()
            return jsonify({"status": "cleared"})

        @app.route("/admin/chaos/inject/latency", methods=["POST"])
        def inject_latency():
            data = request.get_json(silent=True) or {}
            rule = self.add_rule({
                "id": f"latency-{int(time.time()*1000)}",
                "service_name": self.service_name,
                "type": "latency",
                "probability": data.get("prob", 1.0),
                "latency_ms": data.get("ms", 500),
                "duration_sec": data.get("duration_sec", 60),
                "enabled": True
            })
            return jsonify(rule)

        @app.route("/admin/chaos/inject/error", methods=["POST"])
        def inject_error():
            data = request.get_json(silent=True) or {}
            rule = self.add_rule({
                "id": f"error-{int(time.time()*1000)}",
                "service_name": self.service_name,
                "type": "error",
                "probability": data.get("prob", 1.0),
                "status_code": data.get("status_code", 500),
                "message": data.get("message", ""),
                "duration_sec": data.get("duration_sec", 60),
                "enabled": True
            })
            return jsonify(rule)


_default_engine = None


def get_engine():
    global _default_engine
    if _default_engine is None:
        _default_engine = ChaosEngine()
    return _default_engine


def middleware():
    return get_engine().middleware()


def add_rule(rule):
    return get_engine().add_rule(rule)


def remove_rule(rule_id):
    get_engine().remove_rule(rule_id)


def clear_rules():
    get_engine().clear_rules()


def list_rules():
    return get_engine().list_rules()


def register_admin_routes(app):
    get_engine().register_admin_routes(app)
