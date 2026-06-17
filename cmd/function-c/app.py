import os
import time
import json
import uuid
import random
import logging
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('function-c')

FUNCTION_A_URL = os.environ.get('FUNCTION_A_URL', 'http://function-a:8082')
FUNCTION_B_URL = os.environ.get('FUNCTION_B_URL', 'http://function-b:8083')
FUNCTION_C_URL = os.environ.get('FUNCTION_C_URL', 'http://function-c:8084')
PORT = int(os.environ.get('FUNCTION_C_PORT', 8084))


@app.before_request
def log_request_start():
    request.start_time = time.time()
    request_id = request.headers.get('X-Request-ID', 'unknown')
    logger.info(f"[FunctionC:Python] request_id={request_id} method={request.method} path={request.path} started")


@app.after_request
def log_request_end(response):
    if hasattr(request, 'start_time'):
        duration = int((time.time() - request.start_time) * 1000)
        request_id = request.headers.get('X-Request-ID', 'unknown')
        logger.info(f"[FunctionC:Python] request_id={request_id} path={request.path} status={response.status_code} duration={duration}ms")
    return response


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'service': 'function-c',
        'language': 'python',
        'status': 'healthy',
        'time': datetime.now(timezone.utc).isoformat()
    })


@app.route('/notify', methods=['POST'])
def handle_notify():
    request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
    trace_id = request.headers.get('X-Trace-ID', str(uuid.uuid4()))
    parent_span_id = request.headers.get('X-Parent-Span-ID', '')
    span_id = str(uuid.uuid4())
    start_time = time.time()

    try:
        data = request.get_json(force=True, silent=True)
        if data is None:
            return jsonify({'error': 'Invalid JSON payload'}), 400

        order_id = data.get('order_id', '')
        payment_id = data.get('payment_id', '')
        user_id = data.get('user_id', '')
        amount = data.get('amount', 0)
        status = data.get('status', '')
        notification_type = data.get('notification_type', 'generic')

        logger.info(f"[FunctionC] request_id={request_id} sending notification: "
                    f"type={notification_type} order={order_id} payment={payment_id} "
                    f"user={user_id} amount={amount} status={status}")

        if not user_id:
            return jsonify({'error': 'Missing required field: user_id'}), 400

        time.sleep(random.uniform(0.06, 0.18))

        notification_id = f"NOT-{uuid.uuid4().hex[:8]}"
        channels = ['email', 'sms', 'push']
        sent_channels = random.sample(channels, random.randint(1, 3))

        for ch in sent_channels:
            logger.info(f"[FunctionC] request_id={request_id} notification sent via {ch} to user {user_id}")

        duration = int((time.time() - start_time) * 1000)
        logger.info(f"[FunctionC] request_id={request_id} notification completed: "
                    f"notification_id={notification_id} channels={len(sent_channels)} duration={duration}ms")

        return jsonify({
            'notification_id': notification_id,
            'order_id': order_id,
            'payment_id': payment_id,
            'user_id': user_id,
            'status': 'sent',
            'channels_sent': sent_channels,
            'sent_at': datetime.now(timezone.utc).isoformat()
        }), 200

    except Exception as e:
        duration = int((time.time() - start_time) * 1000)
        logger.error(f"[FunctionC] request_id={request_id} error: {str(e)}, duration={duration}ms")
        return jsonify({'error': f'Notification processing failed: {str(e)}'}), 500


@app.route('/notify/batch', methods=['POST'])
def handle_batch_notify():
    request_id = request.headers.get('X-Request-ID', str(uuid.uuid4()))
    start_time = time.time()

    try:
        data = request.get_json(force=True, silent=True)
        notifications = data.get('notifications', []) if data else []

        logger.info(f"[FunctionC] request_id={request_id} batch notification started: count={len(notifications)}")

        success_count = 0
        results = []

        for i, notif in enumerate(notifications):
            time.sleep(random.uniform(0.02, 0.06))
            notif_id = f"NOT-{uuid.uuid4().hex[:8]}"
            results.append({
                'index': i,
                'notification_id': notif_id,
                'status': 'sent' if random.random() > 0.05 else 'failed',
                'user_id': notif.get('user_id', '')
            })
            if results[-1]['status'] == 'sent':
                success_count += 1

        duration = int((time.time() - start_time) * 1000)
        logger.info(f"[FunctionC] request_id={request_id} batch completed: "
                    f"success={success_count}/{len(notifications)} duration={duration}ms")

        return jsonify({
            'request_id': request_id,
            'total': len(notifications),
            'success': success_count,
            'failed': len(notifications) - success_count,
            'results': results
        }), 200

    except Exception as e:
        duration = int((time.time() - start_time) * 1000)
        logger.error(f"[FunctionC] request_id={request_id} batch error: {str(e)}, duration={duration}ms")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    logger.info(f"FunctionC (Python - Notification Service) starting on port {PORT}")
    app.run(host='0.0.0.0', port=PORT, threaded=True)
