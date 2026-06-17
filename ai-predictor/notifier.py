import json
import time
import logging
import requests
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class DingTalkNotifier:
    def __init__(self, webhook_url: str, secret: Optional[str] = None):
        self.webhook_url = webhook_url
        self.secret = secret

    def _sign(self) -> tuple:
        if not self.secret:
            return None, None
        timestamp = str(round(time.time() * 1000))
        import hmac
        import hashlib
        import base64
        import urllib.parse
        string_to_sign = f"{timestamp}\n{self.secret}"
        hmac_code = hmac.new(
            self.secret.encode("utf-8"),
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256
        ).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        return timestamp, sign

    def send_text(self, content: str, at_mobiles: Optional[List[str]] = None) -> bool:
        payload = {
            "msgtype": "text",
            "text": {"content": content}
        }
        if at_mobiles:
            payload["at"] = {"atMobiles": at_mobiles, "isAtAll": False}
        return self._send(payload)

    def send_markdown(self, title: str, text: str, at_mobiles: Optional[List[str]] = None) -> bool:
        payload = {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": text}
        }
        if at_mobiles:
            payload["at"] = {"atMobiles": at_mobiles, "isAtAll": False}
        return self._send(payload)

    def send_alert(self, service: str, predicted_timeout: bool, confidence: float,
                   avg_latency: float, error_rate: float, details: str = "") -> bool:
        level = "🔴 严重" if confidence > 0.8 else ("🟠 警告" if confidence > 0.6 else "🟡 注意")
        md = f"""### {level} 故障预测告警

**服务名称**: {service}
**预测结果**: {'极可能超时 ⚠️' if predicted_timeout else '运行正常'}
**置信度**: {confidence:.2%}
**平均延迟**: {avg_latency:.1f} ms
**错误率**: {error_rate:.2%}

**详情**:
{details}

---
*由 LSTM 故障预测模型自动生成 | {time.strftime('%Y-%m-%d %H:%M:%S')}*
"""
        return self.send_markdown(f"[预测告警] {service}", md)

    def _send(self, payload: Dict[str, Any]) -> bool:
        try:
            url = self.webhook_url
            timestamp, sign = self._sign()
            if timestamp and sign:
                url = f"{url}&timestamp={timestamp}&sign={sign}"
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code == 200:
                result = resp.json()
                if result.get("errcode") == 0:
                    logger.info(f"DingTalk notification sent successfully")
                    return True
                else:
                    logger.warning(f"DingTalk returned error: {result}")
            else:
                logger.warning(f"DingTalk HTTP {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.error(f"Failed to send DingTalk notification: {e}")
        return False


class FeishuNotifier:
    def __init__(self, webhook_url: str, secret: Optional[str] = None):
        self.webhook_url = webhook_url
        self.secret = secret

    def _sign(self) -> tuple:
        if not self.secret:
            return None, None
        timestamp = str(int(time.time()))
        import hmac
        import hashlib
        import base64
        string_to_sign = f"{timestamp}\n{self.secret}"
        hmac_code = hmac.new(
            string_to_sign.encode("utf-8"),
            digestmod=hashlib.sha256
        ).digest()
        sign = base64.b64encode(hmac_code).decode("utf-8")
        return timestamp, sign

    def send_text(self, content: str) -> bool:
        payload = {"msg_type": "text", "content": {"text": content}}
        return self._send(payload)

    def send_interactive(self, title: str, content_lines: List[str]) -> bool:
        elements = []
        for line in content_lines:
            elements.append({
                "tag": "div",
                "text": {"tag": "lark_md", "content": line}
            })
        payload = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"tag": "plain_text", "content": title},
                    "template": "red"
                },
                "elements": elements
            }
        }
        return self._send(payload)

    def send_alert(self, service: str, predicted_timeout: bool, confidence: float,
                   avg_latency: float, error_rate: float, details: str = "") -> bool:
        level = "🔴 严重" if confidence > 0.8 else ("🟠 警告" if confidence > 0.6 else "🟡 注意")
        lines = [
            f"**{level} 故障预测告警**",
            f"**服务名称**: {service}",
            f"**预测结果**: {'极可能超时 ⚠️' if predicted_timeout else '运行正常'}",
            f"**置信度**: {confidence:.2%}",
            f"**平均延迟**: {avg_latency:.1f} ms",
            f"**错误率**: {error_rate:.2%}",
            f"**详情**: {details}",
            f"---",
            f"*由 LSTM 故障预测模型自动生成 | {time.strftime('%Y-%m-%d %H:%M:%S')}*"
        ]
        return self.send_interactive(f"[预测告警] {service}", lines)

    def _send(self, payload: Dict[str, Any]) -> bool:
        try:
            if self.secret:
                timestamp, sign = self._sign()
                if timestamp and sign:
                    payload["timestamp"] = timestamp
                    payload["sign"] = sign
            resp = requests.post(self.webhook_url, json=payload, timeout=5)
            if resp.status_code == 200:
                result = resp.json()
                if result.get("code") == 0 or result.get("StatusCode") == 0:
                    logger.info("Feishu notification sent successfully")
                    return True
                else:
                    logger.warning(f"Feishu returned error: {result}")
            else:
                logger.warning(f"Feishu HTTP {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.error(f"Failed to send Feishu notification: {e}")
        return False


class MultiNotifier:
    def __init__(self):
        self.notifiers = []
        self.cooldowns = {}

    def add_dingtalk(self, webhook: str, secret: Optional[str] = None):
        if webhook:
            self.notifiers.append(DingTalkNotifier(webhook, secret))
            logger.info(f"DingTalk notifier added")

    def add_feishu(self, webhook: str, secret: Optional[str] = None):
        if webhook:
            self.notifiers.append(FeishuNotifier(webhook, secret))
            logger.info(f"Feishu notifier added")

    def send_alert(self, service: str, predicted_timeout: bool, confidence: float,
                   avg_latency: float, error_rate: float, details: str = "",
                   cooldown_seconds: int = 300) -> int:
        if predicted_timeout and confidence < 0.6:
            return 0

        key = f"alert_{service}"
        now = time.time()
        if key in self.cooldowns:
            if now - self.cooldowns[key] < cooldown_seconds:
                logger.info(f"Alert for {service} in cooldown, skipping")
                return 0
        self.cooldowns[key] = now

        success = 0
        for n in self.notifiers:
            try:
                if n.send_alert(service, predicted_timeout, confidence,
                                avg_latency, error_rate, details):
                    success += 1
            except Exception as e:
                logger.error(f"Notifier error: {e}")
        return success
